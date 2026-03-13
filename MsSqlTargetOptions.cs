using SyncForge.Abstractions.Configuration;
using SyncForge.Abstractions.Models;

namespace SyncForge.Plugin.MsSql;

internal enum ConstraintHandlingMode
{
    FailFast,
    SkipRow
}

internal enum ReplaceMode
{
    Truncate,
    SoftDelete
}

internal enum UpsertImplementationMode
{
    Merge,
    UpdateThenInsert
}

internal sealed class MsSqlTargetOptions
{
    public required string ConnectionString { get; init; }

    public required string TableName { get; init; }

    public int BatchSize { get; init; } = 500;

    public int CommandTimeoutSeconds { get; init; } = 30;

    public ConstraintHandlingMode ConstraintHandling { get; init; } = ConstraintHandlingMode.FailFast;

    public ReplaceMode ReplaceMode { get; init; } = ReplaceMode.Truncate;

    public UpsertImplementationMode UpsertImplementation { get; init; } = UpsertImplementationMode.Merge;

    public static MsSqlTargetOptions FromContext(JobContext context)
    {
        var settings = context.Parameters
            .Where(pair => pair.Key.StartsWith("target.settings.", StringComparison.OrdinalIgnoreCase))
            .ToDictionary(
                pair => pair.Key.Substring("target.settings.".Length),
                pair => pair.Value,
                StringComparer.OrdinalIgnoreCase);

        if (!settings.TryGetValue("connectionString", out var connectionString) || string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException("Target setting 'connectionString' is required for target.type 'mssql'.");
        }

        if (!settings.TryGetValue("table", out var tableName) || string.IsNullOrWhiteSpace(tableName))
        {
            throw new InvalidOperationException("Target setting 'table' is required for target.type 'mssql'.");
        }

        var batchSize = ReadInt(settings, "batchSize", 500);
        var timeoutSeconds = ReadInt(settings, "commandTimeoutSeconds", 30);

        var constraintHandling = settings.TryGetValue("constraintStrategy", out var strategy)
            && string.Equals(strategy, "SkipRow", StringComparison.OrdinalIgnoreCase)
                ? ConstraintHandlingMode.SkipRow
                : ConstraintHandlingMode.FailFast;

        var replaceMode = settings.TryGetValue("replaceMode", out var replaceModeRaw)
            && string.Equals(replaceModeRaw, "SoftDelete", StringComparison.OrdinalIgnoreCase)
                ? ReplaceMode.SoftDelete
                : ReplaceMode.Truncate;

        var upsertImplementation = settings.TryGetValue("upsertImplementation", out var upsertRaw)
            && string.Equals(upsertRaw, "UpdateThenInsert", StringComparison.OrdinalIgnoreCase)
                ? UpsertImplementationMode.UpdateThenInsert
                : UpsertImplementationMode.Merge;

        return new MsSqlTargetOptions
        {
            ConnectionString = connectionString,
            TableName = tableName,
            BatchSize = Math.Max(1, batchSize),
            CommandTimeoutSeconds = Math.Max(5, timeoutSeconds),
            ConstraintHandling = constraintHandling,
            ReplaceMode = replaceMode,
            UpsertImplementation = upsertImplementation
        };
    }

    private static int ReadInt(IReadOnlyDictionary<string, string> settings, string key, int fallback)
    {
        if (!settings.TryGetValue(key, out var raw) || !int.TryParse(raw, out var value))
        {
            return fallback;
        }

        return value;
    }
}
