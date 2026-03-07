using System.Data;
using Microsoft.Data.SqlClient;
using SyncForge.Abstractions.Connectors;
using SyncForge.Abstractions.Logging;
using SyncForge.Abstractions.Models;

namespace SyncForge.Plugin.MsSql;

public sealed class MsSqlTargetConnector : ITargetConnector
{
    private readonly ISyncForgeLogger _logger;

    public MsSqlTargetConnector(ISyncForgeLogger? logger = null)
    {
        _logger = logger ?? NoOpSyncForgeLogger.Instance;
    }

    public async Task<WriteResult> WriteAsync(IAsyncEnumerable<DataRecord> records, JobContext context)
    {
        var options = MsSqlTargetOptions.FromContext(context);
        var strategy = context.StrategyMode;
        var keyFields = context.StrategyKeyFields;

        long processed = 0;
        long succeeded = 0;
        long failed = 0;
        long inserted = 0;
        long updated = 0;

        var batch = new List<DataRecord>(options.BatchSize);

        _logger.Info(
            "[MSSQL] Target write started. Strategy={Strategy}, BatchSize={BatchSize}, UpsertImplementation={UpsertImplementation}.",
            strategy,
            options.BatchSize,
            options.UpsertImplementation);

        if (context.DryRun)
        {
            await foreach (var _ in records)
            {
                processed++;
                succeeded++;
            }

            return new WriteResult
            {
                ProcessedRecords = processed,
                SucceededRecords = succeeded,
                FailedRecords = failed,
                Message = "Dry-run: no database write executed.",
                Stats = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
                {
                    ["processed"] = processed,
                    ["succeeded"] = succeeded,
                    ["failed"] = failed
                }
            };
        }

        await using var connection = new SqlConnection(options.ConnectionString);
        await connection.OpenAsync();

        if (string.Equals(strategy, "Replace", StringComparison.OrdinalIgnoreCase))
        {
            _logger.Info("[MSSQL] Replace strategy uses a single transaction for clear + insert batches.");
            await using var replaceTx = (SqlTransaction)await connection.BeginTransactionAsync();
            try
            {
                _logger.Info("[MSSQL] Replace transaction begin.");
                await ClearTargetAsync(connection, options, replaceTx);

                await foreach (var record in records)
                {
                    batch.Add(record);
                    if (batch.Count >= options.BatchSize)
                    {
                        var result = await ProcessBatchAsync(connection, batch, strategy, keyFields, options, replaceTx);
                        processed += result.Processed;
                        succeeded += result.Succeeded;
                        failed += result.Failed;
                        inserted += result.Inserted;
                        updated += result.Updated;
                        batch.Clear();
                    }
                }

                if (batch.Count > 0)
                {
                    var result = await ProcessBatchAsync(connection, batch, strategy, keyFields, options, replaceTx);
                    processed += result.Processed;
                    succeeded += result.Succeeded;
                    failed += result.Failed;
                    inserted += result.Inserted;
                    updated += result.Updated;
                    batch.Clear();
                }

                await replaceTx.CommitAsync();
                _logger.Info("[MSSQL] Replace transaction commit.");
            }
            catch
            {
                await replaceTx.RollbackAsync();
                _logger.Error("[MSSQL] Replace transaction rollback.");
                throw;
            }
        }
        else
        {
            await foreach (var record in records)
            {
                batch.Add(record);
                if (batch.Count >= options.BatchSize)
                {
                    var result = await ProcessBatchAsync(connection, batch, strategy, keyFields, options);
                    processed += result.Processed;
                    succeeded += result.Succeeded;
                    failed += result.Failed;
                    inserted += result.Inserted;
                    updated += result.Updated;
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                var result = await ProcessBatchAsync(connection, batch, strategy, keyFields, options);
                processed += result.Processed;
                succeeded += result.Succeeded;
                failed += result.Failed;
                inserted += result.Inserted;
                updated += result.Updated;
                batch.Clear();
            }
        }

        return new WriteResult
        {
            ProcessedRecords = processed,
            SucceededRecords = succeeded,
            FailedRecords = failed,
            Message = $"Inserted={inserted}, Updated={updated}, Failed={failed}",
            Stats = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
            {
                ["processed"] = processed,
                ["succeeded"] = succeeded,
                ["failed"] = failed,
                ["inserted"] = inserted,
                ["updated"] = updated
            }
        };
    }

    private static async Task ClearTargetAsync(SqlConnection connection, MsSqlTargetOptions options, SqlTransaction? externalTx = null)
    {
        var ownsTx = externalTx is null;
        var tx = externalTx ?? (SqlTransaction)await connection.BeginTransactionAsync();
        try
        {
            var table = MsSqlIdentifier.QuoteCompound(options.TableName);
            var sql = options.ReplaceMode == ReplaceMode.SoftDelete
                ? $"DELETE FROM {table};"
                : $"TRUNCATE TABLE {table};";

            await using var cmd = new SqlCommand(sql, connection, tx)
            {
                CommandTimeout = options.CommandTimeoutSeconds
            };
            await cmd.ExecuteNonQueryAsync();

            if (ownsTx)
            {
                await tx.CommitAsync();
            }
        }
        catch
        {
            if (ownsTx)
            {
                await tx.RollbackAsync();
            }

            throw;
        }
    }

    private static async Task<BatchResult> ProcessBatchAsync(
        SqlConnection connection,
        IReadOnlyList<DataRecord> batch,
        string strategy,
        IReadOnlyList<string> keyFields,
        MsSqlTargetOptions options,
        SqlTransaction? externalTx = null)
    {
        if (string.Equals(strategy, "UpsertByKey", StringComparison.OrdinalIgnoreCase))
        {
            if (options.UpsertImplementation == UpsertImplementationMode.UpdateThenInsert)
            {
                return await UpsertUpdateThenInsertAsync(connection, batch, keyFields, options, externalTx);
            }

            if (options.ConstraintHandling == ConstraintHandlingMode.SkipRow)
            {
                return await UpsertRowByRowSkipAsync(connection, batch, keyFields, options, externalTx);
            }

            return await UpsertBatchMergeAsync(connection, batch, keyFields, options, externalTx);
        }

        return await InsertBatchAsync(connection, batch, options, externalTx);
    }

    private static async Task<BatchResult> InsertBatchAsync(
        SqlConnection connection,
        IReadOnlyList<DataRecord> batch,
        MsSqlTargetOptions options,
        SqlTransaction? externalTx = null)
    {
        var columns = GetColumns(batch);
        var table = MsSqlIdentifier.QuoteCompound(options.TableName);
        var quotedColumns = columns.Select(MsSqlIdentifier.Quote).ToList();
        var parameterNames = columns.Select((_, i) => $"@p{i}").ToList();
        var sql = $"INSERT INTO {table} ({string.Join(",", quotedColumns)}) VALUES ({string.Join(",", parameterNames)});";

        long processed = 0;
        long succeeded = 0;
        long failed = 0;

        var ownsTx = externalTx is null;
        var tx = externalTx ?? (SqlTransaction)await connection.BeginTransactionAsync();
        try
        {
            await using var cmd = new SqlCommand(sql, connection, tx)
            {
                CommandTimeout = options.CommandTimeoutSeconds
            };

            foreach (var record in batch)
            {
                processed++;
                cmd.Parameters.Clear();
                for (var i = 0; i < columns.Count; i++)
                {
                    record.Fields.TryGetValue(columns[i], out var value);
                    cmd.Parameters.AddWithValue(parameterNames[i], value ?? DBNull.Value);
                }

                try
                {
                    await cmd.ExecuteNonQueryAsync();
                    succeeded++;
                }
                catch (SqlException ex) when (IsConstraintViolation(ex) && options.ConstraintHandling == ConstraintHandlingMode.SkipRow)
                {
                    failed++;
                }
            }

            if (ownsTx)
            {
                await tx.CommitAsync();
            }
        }
        catch
        {
            if (ownsTx)
            {
                await tx.RollbackAsync();
            }

            throw;
        }

        return new BatchResult
        {
            Processed = processed,
            Succeeded = succeeded,
            Failed = failed,
            Inserted = succeeded,
            Updated = 0
        };
    }

    private static async Task<BatchResult> UpsertBatchMergeAsync(
        SqlConnection connection,
        IReadOnlyList<DataRecord> batch,
        IReadOnlyList<string> keyFields,
        MsSqlTargetOptions options,
        SqlTransaction? externalTx = null)
    {
        var dedupedBatch = DeduplicateByKey(batch, keyFields);
        var columns = GetColumns(dedupedBatch);
        ValidateKeyFields(columns, keyFields);

        var dataTable = BuildDataTable(dedupedBatch, columns);
        var table = MsSqlIdentifier.QuoteCompound(options.TableName);

        long inserted = 0;
        long updated = 0;

        var ownsTx = externalTx is null;
        var tx = externalTx ?? (SqlTransaction)await connection.BeginTransactionAsync();
        try
        {
            var tempTableName = "#SyncForgeTemp";
            var quotedColumnList = string.Join(",", columns.Select(MsSqlIdentifier.Quote));
            var createTempSql = $"SELECT TOP 0 {quotedColumnList} INTO {tempTableName} FROM {table};";

            await using (var createCmd = new SqlCommand(createTempSql, connection, tx)
            {
                CommandTimeout = options.CommandTimeoutSeconds
            })
            {
                await createCmd.ExecuteNonQueryAsync();
            }

            using (var bulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, tx))
            {
                bulk.DestinationTableName = tempTableName;
                bulk.BatchSize = options.BatchSize;
                bulk.BulkCopyTimeout = options.CommandTimeoutSeconds;

                foreach (var column in columns)
                {
                    bulk.ColumnMappings.Add(column, column);
                }

                await bulk.WriteToServerAsync(dataTable);
            }

            var mergeSql = BuildMergeSql(table, tempTableName, columns, keyFields);
            await using (var mergeCmd = new SqlCommand(mergeSql, connection, tx)
            {
                CommandTimeout = options.CommandTimeoutSeconds
            })
            await using (var reader = await mergeCmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var action = reader.GetString(0);
                    if (string.Equals(action, "INSERT", StringComparison.OrdinalIgnoreCase))
                    {
                        inserted++;
                    }
                    else if (string.Equals(action, "UPDATE", StringComparison.OrdinalIgnoreCase))
                    {
                        updated++;
                    }
                }
            }

            if (ownsTx)
            {
                await tx.CommitAsync();
            }
        }
        catch
        {
            if (ownsTx)
            {
                await tx.RollbackAsync();
            }

            throw;
        }

        return new BatchResult
        {
            Processed = batch.Count,
            Succeeded = inserted + updated,
            Failed = 0,
            Inserted = inserted,
            Updated = updated
        };
    }

    private static async Task<BatchResult> UpsertRowByRowSkipAsync(
        SqlConnection connection,
        IReadOnlyList<DataRecord> batch,
        IReadOnlyList<string> keyFields,
        MsSqlTargetOptions options,
        SqlTransaction? externalTx = null)
    {
        var dedupedBatch = DeduplicateByKey(batch, keyFields);
        var columns = GetColumns(dedupedBatch);
        ValidateKeyFields(columns, keyFields);

        var table = MsSqlIdentifier.QuoteCompound(options.TableName);
        var mergeSql = BuildSingleRowMergeSql(table, columns, keyFields);

        long processed = 0;
        long succeeded = 0;
        long failed = 0;
        long inserted = 0;
        long updated = 0;

        var ownsTx = externalTx is null;
        var tx = externalTx ?? (SqlTransaction)await connection.BeginTransactionAsync();
        try
        {
            foreach (var record in dedupedBatch)
            {
                processed++;

                await using var cmd = new SqlCommand(mergeSql, connection, tx)
                {
                    CommandTimeout = options.CommandTimeoutSeconds
                };

                foreach (var column in columns)
                {
                    record.Fields.TryGetValue(column, out var value);
                    cmd.Parameters.AddWithValue($"@{column}", value ?? DBNull.Value);
                }

                try
                {
                    var action = (string?)await cmd.ExecuteScalarAsync();
                    if (string.Equals(action, "INSERT", StringComparison.OrdinalIgnoreCase))
                    {
                        inserted++;
                    }
                    else if (string.Equals(action, "UPDATE", StringComparison.OrdinalIgnoreCase))
                    {
                        updated++;
                    }

                    succeeded++;
                }
                catch (SqlException ex) when (IsConstraintViolation(ex))
                {
                    failed++;
                }
            }

            if (ownsTx)
            {
                await tx.CommitAsync();
            }
        }
        catch
        {
            if (ownsTx)
            {
                await tx.RollbackAsync();
            }

            throw;
        }

        return new BatchResult
        {
            Processed = processed,
            Succeeded = succeeded,
            Failed = failed,
            Inserted = inserted,
            Updated = updated
        };
    }

    private static async Task<BatchResult> UpsertUpdateThenInsertAsync(
        SqlConnection connection,
        IReadOnlyList<DataRecord> batch,
        IReadOnlyList<string> keyFields,
        MsSqlTargetOptions options,
        SqlTransaction? externalTx = null)
    {
        var dedupedBatch = DeduplicateByKey(batch, keyFields);
        var columns = GetColumns(dedupedBatch);
        ValidateKeyFields(columns, keyFields);

        var table = MsSqlIdentifier.QuoteCompound(options.TableName);
        var nonKeyColumns = columns.Where(column => !keyFields.Contains(column, StringComparer.OrdinalIgnoreCase)).ToList();

        var updateSql = BuildSingleRowUpdateSql(table, keyFields, nonKeyColumns);
        var existsSql = BuildSingleRowExistsSql(table, keyFields);
        var insertSql = BuildSingleRowInsertSql(table, columns);

        long processed = 0;
        long succeeded = 0;
        long failed = 0;
        long inserted = 0;
        long updated = 0;

        var ownsTx = externalTx is null;
        var tx = externalTx ?? (SqlTransaction)await connection.BeginTransactionAsync();
        try
        {
            foreach (var record in dedupedBatch)
            {
                processed++;

                try
                {
                    var exists = false;
                    if (nonKeyColumns.Count > 0)
                    {
                        await using var updateCmd = new SqlCommand(updateSql, connection, tx)
                        {
                            CommandTimeout = options.CommandTimeoutSeconds
                        };
                        AddParameters(updateCmd, record, nonKeyColumns, "u_");
                        AddParameters(updateCmd, record, keyFields, "k_");
                        var rows = await updateCmd.ExecuteNonQueryAsync();
                        exists = rows > 0;
                    }
                    else
                    {
                        await using var existsCmd = new SqlCommand(existsSql, connection, tx)
                        {
                            CommandTimeout = options.CommandTimeoutSeconds
                        };
                        AddParameters(existsCmd, record, keyFields, "k_");
                        var result = await existsCmd.ExecuteScalarAsync();
                        exists = result is int intValue && intValue == 1;
                    }

                    if (exists)
                    {
                        updated++;
                        succeeded++;
                        continue;
                    }

                    await using var insertCmd = new SqlCommand(insertSql, connection, tx)
                    {
                        CommandTimeout = options.CommandTimeoutSeconds
                    };
                    AddParameters(insertCmd, record, columns, "i_");
                    await insertCmd.ExecuteNonQueryAsync();
                    inserted++;
                    succeeded++;
                }
                catch (SqlException ex) when (IsConstraintViolation(ex) && options.ConstraintHandling == ConstraintHandlingMode.SkipRow)
                {
                    failed++;
                }
            }

            if (ownsTx)
            {
                await tx.CommitAsync();
            }
        }
        catch
        {
            if (ownsTx)
            {
                await tx.RollbackAsync();
            }

            throw;
        }

        return new BatchResult
        {
            Processed = processed,
            Succeeded = succeeded,
            Failed = failed,
            Inserted = inserted,
            Updated = updated
        };
    }

    private static void AddParameters(SqlCommand command, DataRecord record, IReadOnlyList<string> columns, string prefix)
    {
        foreach (var column in columns)
        {
            record.Fields.TryGetValue(column, out var value);
            command.Parameters.AddWithValue($"@{prefix}{column}", value ?? DBNull.Value);
        }
    }

    private static string BuildSingleRowUpdateSql(string table, IReadOnlyList<string> keyFields, IReadOnlyList<string> nonKeyColumns)
    {
        var setClause = string.Join(",", nonKeyColumns.Select(column => $"{MsSqlIdentifier.Quote(column)}=@u_{column}"));
        var whereClause = string.Join(" AND ", keyFields.Select(key => $"{MsSqlIdentifier.Quote(key)}=@k_{key}"));
        return $"UPDATE {table} SET {setClause} WHERE {whereClause};";
    }

    private static string BuildSingleRowExistsSql(string table, IReadOnlyList<string> keyFields)
    {
        var whereClause = string.Join(" AND ", keyFields.Select(key => $"{MsSqlIdentifier.Quote(key)}=@k_{key}"));
        return $"SELECT CASE WHEN EXISTS (SELECT 1 FROM {table} WHERE {whereClause}) THEN 1 ELSE 0 END;";
    }

    private static string BuildSingleRowInsertSql(string table, IReadOnlyList<string> columns)
    {
        var insertColumns = string.Join(",", columns.Select(MsSqlIdentifier.Quote));
        var insertValues = string.Join(",", columns.Select(column => $"@i_{column}"));
        return $"INSERT INTO {table} ({insertColumns}) VALUES ({insertValues});";
    }

    private static string BuildMergeSql(string table, string sourceTable, IReadOnlyList<string> columns, IReadOnlyList<string> keyFields)
    {
        var onClause = string.Join(
            " AND ",
            keyFields.Select(key => $"target.{MsSqlIdentifier.Quote(key)} = source.{MsSqlIdentifier.Quote(key)}"));

        var nonKeyColumns = columns.Where(column => !keyFields.Contains(column, StringComparer.OrdinalIgnoreCase)).ToList();
        var updateClause = nonKeyColumns.Count > 0
            ? "WHEN MATCHED THEN UPDATE SET " + string.Join(",", nonKeyColumns.Select(column => $"target.{MsSqlIdentifier.Quote(column)} = source.{MsSqlIdentifier.Quote(column)}"))
            : string.Empty;

        var insertColumns = string.Join(",", columns.Select(MsSqlIdentifier.Quote));
        var insertValues = string.Join(",", columns.Select(column => $"source.{MsSqlIdentifier.Quote(column)}"));

        return $@"
MERGE {table} AS target
USING {sourceTable} AS source
ON {onClause}
{updateClause}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insertColumns})
    VALUES ({insertValues})
OUTPUT $action;";
    }

    private static string BuildSingleRowMergeSql(string table, IReadOnlyList<string> columns, IReadOnlyList<string> keyFields)
    {
        var sourceValues = string.Join(",", columns.Select(column => $"@{column} AS {MsSqlIdentifier.Quote(column)}"));
        var onClause = string.Join(
            " AND ",
            keyFields.Select(key => $"target.{MsSqlIdentifier.Quote(key)} = source.{MsSqlIdentifier.Quote(key)}"));

        var nonKeyColumns = columns.Where(column => !keyFields.Contains(column, StringComparer.OrdinalIgnoreCase)).ToList();
        var updateClause = nonKeyColumns.Count > 0
            ? "WHEN MATCHED THEN UPDATE SET " + string.Join(",", nonKeyColumns.Select(column => $"target.{MsSqlIdentifier.Quote(column)} = source.{MsSqlIdentifier.Quote(column)}"))
            : string.Empty;

        var insertColumns = string.Join(",", columns.Select(MsSqlIdentifier.Quote));
        var insertValues = string.Join(",", columns.Select(column => $"source.{MsSqlIdentifier.Quote(column)}"));

        return $@"
MERGE {table} AS target
USING (SELECT {sourceValues}) AS source
ON {onClause}
{updateClause}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insertColumns})
    VALUES ({insertValues})
OUTPUT $action;";
    }

    private static void ValidateKeyFields(IReadOnlyList<string> columns, IReadOnlyList<string> keyFields)
    {
        if (keyFields.Count == 0)
        {
            throw new InvalidOperationException("UpsertByKey strategy requires at least one key field.");
        }

        foreach (var keyField in keyFields)
        {
            if (!columns.Contains(keyField, StringComparer.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Upsert key field '{keyField}' is missing in batch columns.");
            }
        }
    }

    private static List<string> GetColumns(IReadOnlyList<DataRecord> batch)
    {
        var columns = new List<string>();
        var known = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var record in batch)
        {
            foreach (var key in record.Fields.Keys)
            {
                if (known.Add(key))
                {
                    columns.Add(key);
                }
            }
        }

        if (columns.Count == 0)
        {
            throw new InvalidOperationException("No mapped columns available to write to SQL target.");
        }

        return columns;
    }

    private static DataTable BuildDataTable(IReadOnlyList<DataRecord> batch, IReadOnlyList<string> columns)
    {
        var table = new DataTable();
        foreach (var column in columns)
        {
            table.Columns.Add(column, typeof(object));
        }

        foreach (var record in batch)
        {
            var row = table.NewRow();
            foreach (var column in columns)
            {
                record.Fields.TryGetValue(column, out var value);
                row[column] = value ?? DBNull.Value;
            }

            table.Rows.Add(row);
        }

        return table;
    }

    private static bool IsConstraintViolation(SqlException ex)
    {
        return ex.Number is 2627 or 2601 or 515 or 547;
    }

    private static List<DataRecord> DeduplicateByKey(IReadOnlyList<DataRecord> records, IReadOnlyList<string> keyFields)
    {
        if (keyFields.Count == 0)
        {
            return records.ToList();
        }

        var deduped = new Dictionary<string, DataRecord>(StringComparer.OrdinalIgnoreCase);
        foreach (var record in records)
        {
            var key = BuildCompositeKey(record, keyFields);
            deduped[key] = record;
        }

        return deduped.Values.ToList();
    }

    private static string BuildCompositeKey(DataRecord record, IReadOnlyList<string> keyFields)
    {
        var parts = new List<string>(keyFields.Count);
        foreach (var keyField in keyFields)
        {
            record.Fields.TryGetValue(keyField, out var value);
            parts.Add(value?.ToString() ?? string.Empty);
        }

        return string.Join("|", parts);
    }

    private sealed class BatchResult
    {
        public long Processed { get; init; }

        public long Succeeded { get; init; }

        public long Failed { get; init; }

        public long Inserted { get; init; }

        public long Updated { get; init; }
    }
}
