namespace SyncForge.Plugin.MsSql;

internal static class MsSqlIdentifier
{
    public static string QuoteCompound(string identifier)
    {
        var segments = identifier.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (segments.Length == 0)
        {
            throw new InvalidOperationException("SQL identifier cannot be empty.");
        }

        return string.Join('.', segments.Select(Quote));
    }

    public static string Quote(string identifier)
    {
        if (!IsValid(identifier))
        {
            throw new InvalidOperationException($"Unsafe SQL identifier: '{identifier}'.");
        }

        return $"[{identifier}]";
    }

    private static bool IsValid(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            return false;
        }

        if (!(char.IsLetter(identifier[0]) || identifier[0] == '_'))
        {
            return false;
        }

        for (var i = 1; i < identifier.Length; i++)
        {
            var c = identifier[i];
            if (!(char.IsLetterOrDigit(c) || c == '_'))
            {
                return false;
            }
        }

        return true;
    }
}
