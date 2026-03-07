# SyncForge.Plugin.MsSql

MSSQL Target Connector fuer SyncForge.

Dieses Plugin schreibt gemappte Datensaetze in Microsoft SQL Server und unterstuetzt die Strategien `InsertOnly`, `Replace` und `UpsertByKey`.

## Scope

- Connector-Typ: `target.type = "mssql"`
- Runtime: `.NET 8`
- Hauptklasse: `MsSqlTargetConnector`
- Paket: `Microsoft.Data.SqlClient`

## Einbindung als Submodul

Das Plugin liegt als eigenes Repository ausserhalb des Haupt-Repos:

- Plugin-Pfad: `../SyncForge.Plugin.MsSql`
- Haupt-Repo-Pfad: `./SyncForge`

`SyncForge.Cli` und `SyncForge.sln` referenzieren das Projekt ueber relative Pfade.

## Build

Aus dem Haupt-Repo (`SyncForge`) heraus:

```powershell
dotnet build .\src\SyncForge.sln -c Release
```

Direkt im Plugin:

```powershell
dotnet build .\SyncForge.Plugin.MsSql.csproj -c Release
```

## Job-Konfiguration

Minimalbeispiel fuer Target in `job.json`:

```json
{
  "target": {
    "type": "mssql",
    "plugin": "SyncForge.Plugin.MsSql",
    "settings": {
      "connectionString": "Server=localhost,1433;Database=SyncForge;User Id=sa;Password=Your_password123;TrustServerCertificate=true",
      "table": "dbo.customers"
    }
  }
}
```

## Target Settings

Pflichtfelder:

- `connectionString`: SQL Server Connection String
- `table`: Zieltabelle, z. B. `dbo.customers`

Optionale Felder:

- `batchSize` (Default: `500`)
- `commandTimeoutSeconds` (Default: `30`, Minimum: `5`)
- `constraintStrategy` (Default: `FailFast`)
  - `FailFast`: erster Constraint-Fehler bricht Batch/Operation ab
  - `SkipRow`: betroffene Zeile wird als Fehler gezaehlt und uebersprungen
- `replaceMode` (Default: `Truncate`)
  - `Truncate`: `TRUNCATE TABLE`
  - `SoftDelete`: `DELETE FROM`
- `upsertImplementation` (Default: `Merge`)
  - `Merge`
  - `UpdateThenInsert`

## Strategie-Verhalten

### InsertOnly

- Fuehrt Inserts in Batches aus.
- Bei `constraintStrategy = SkipRow` werden Constraint-Verletzungen pro Zeile uebersprungen.

### Replace

- Oeffnet eine Transaktion fuer "Clear + Insert".
- Leert die Zieltabelle zuerst (`Truncate` oder `Delete` je nach `replaceMode`).
- Schreibt danach alle Batches in derselben Transaktion.

### UpsertByKey

- Benoetigt `strategy.keyFields` im Job.
- Key-Felder muessen in den gemappten Spalten vorhanden sein.
- Input wird pro Batch anhand der Key-Felder dedupliziert (letzter Datensatz gewinnt).
- Implementierung je nach `upsertImplementation`:
  - `Merge`: temp table + SQL MERGE
  - `UpdateThenInsert`: erst UPDATE, dann INSERT falls nicht vorhanden

## Dry-Run

Wenn `context.DryRun = true`:

- Es wird keine DB-Schreiboperation ausgefuehrt.
- Rueckgabe: `WriteResult` mit `processed/succeeded/failed`, Message: `Dry-run: no database write executed.`

## WriteResult / Stats

Der Connector liefert u. a. diese Kennzahlen:

- `processed`
- `succeeded`
- `failed`
- `inserted`
- `updated`

Message-Format im Erfolgsfall:

- `Inserted={inserted}, Updated={updated}, Failed={failed}`

## SQL-Sicherheit (Identifier)

Tabellen- und Spaltennamen werden validiert und gequoted:

- Zulassige Zeichen: Buchstaben/Ziffern/`_`
- Erstes Zeichen: Buchstabe oder `_`
- Compound-Namen wie `dbo.customers` werden segmentweise zu `[dbo].[customers]`

Unsichere Identifier fuehren zu einer `InvalidOperationException`.

## Constraint-Fehlercodes

Als Constraint-Verletzung werden behandelt:

- `2627` (unique/PK)
- `2601` (unique index)
- `515` (NOT NULL)
- `547` (FK/check)

## Troubleshooting

### "Target setting 'connectionString' is required"

`target.settings.connectionString` fehlt oder ist leer.

### "Target setting 'table' is required"

`target.settings.table` fehlt oder ist leer.

### "UpsertByKey strategy requires at least one key field"

In `strategy.keyFields` ist kein Feld gesetzt.

### "Upsert key field 'X' is missing in batch columns"

Mindestens ein Key-Feld kommt nicht in den gemappten Source-Spalten vor.

### SQL-Fehler bei Replace + Truncate

`TRUNCATE TABLE` kann bei FK-Abhaengigkeiten scheitern. In diesem Fall `replaceMode = SoftDelete` verwenden.

## Verwandte Dateien

- `MsSqlTargetConnector.cs`
- `MsSqlTargetOptions.cs`
- `MsSqlIdentifier.cs`
