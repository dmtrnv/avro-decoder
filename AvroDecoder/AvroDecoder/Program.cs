using System.Collections;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Avro;
using Avro.File;
using Avro.Generic;
using Avro.IO;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.Error.WriteLine("Usage: avro <schema-file> <data-file> [--pretty|--compact]");
            return 1;
        }

        var schemaPath = args[0];
        var dataPath = args[1];
        var pretty = args.Contains("--pretty", StringComparer.OrdinalIgnoreCase) || !args.Contains("--compact", StringComparer.OrdinalIgnoreCase);

        try
        {
            var schemaText = await File.ReadAllTextAsync(schemaPath);
            var schema = Schema.Parse(schemaText);

            var payload = await ReadDataAsync(dataPath);
            var records = Decode(schema, payload);

            var serializerOptions = new JsonSerializerOptions
            {
                WriteIndented = pretty,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };

            object? output = records.Count switch
            {
                0 => Array.Empty<object?>(),
                1 => records[0],
                _ => records
            };

            Console.WriteLine(JsonSerializer.Serialize(output, serializerOptions));
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: {ex.Message}");
#if DEBUG
            Console.Error.WriteLine(ex);
#endif
            return 2;
        }
    }

    private static async Task<byte[]> ReadDataAsync(string dataPath)
    {
        var rawBytes = await File.ReadAllBytesAsync(dataPath);

        if (rawBytes.Length >= 4 && rawBytes[0] == 'O' && rawBytes[1] == 'b' && rawBytes[2] == 'j' && rawBytes[3] == 1)
        {
            return rawBytes; // Avro Object Container File
        }

        if (!LooksLikeHex(rawBytes))
        {
            return rawBytes;
        }

        var hex = Encoding.UTF8.GetString(rawBytes);
        hex = new string(hex.Where(c => !char.IsWhiteSpace(c)).ToArray());

        if (hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hex = hex[2..];
        }

        if (hex.Length % 2 != 0)
        {
            throw new InvalidOperationException("Hex-encoded data must contain an even number of characters.");
        }

        var buffer = new byte[hex.Length / 2];
        for (var i = 0; i < buffer.Length; i++)
        {
            buffer[i] = byte.Parse(hex.AsSpan(i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }

        return buffer;
    }

    private static bool LooksLikeHex(byte[] bytes)
    {
        if (bytes.Length == 0)
        {
            return false;
        }

        return bytes.All(b => b == (byte)'x' || b == (byte)'X' || b == (byte)'0' || b == (byte)'1' || b == (byte)'2' || b == (byte)'3' ||
                               b == (byte)'4' || b == (byte)'5' || b == (byte)'6' || b == (byte)'7' || b == (byte)'8' || b == (byte)'9' ||
                               b == (byte)'a' || b == (byte)'A' || b == (byte)'b' || b == (byte)'B' || b == (byte)'c' || b == (byte)'C' ||
                               b == (byte)'d' || b == (byte)'D' || b == (byte)'e' || b == (byte)'E' || b == (byte)'f' || b == (byte)'F' ||
                               b is (byte)' ' or (byte)'\n' or (byte)'\r' or (byte)'\t');
    }

    private static List<object?> Decode(Schema schema, byte[] data)
    {
        if (data.Length >= 4 && data[0] == 'O' && data[1] == 'b' && data[2] == 'j' && data[3] == 1)
        {
            using var containerStream = new MemoryStream(data, writable: false);
            using var dataFileReader = DataFileReader<GenericRecord>.OpenReader(containerStream);
            var containerResults = new List<object?>();
            while (dataFileReader.HasNext())
            {
                containerResults.Add(ConvertToPlainObject(dataFileReader.Next()));
            }

            return containerResults;
        }

        using var stream = new MemoryStream(data, writable: false);
        var decoder = new BinaryDecoder(stream);
        var reader = new GenericDatumReader<GenericRecord>(schema, schema);
        var results = new List<object?>();

        while (stream.Position < stream.Length)
        {
            try
            {
#pragma warning disable CS8625
                var record = reader.Read(null, decoder);
#pragma warning restore CS8625
                results.Add(ConvertToPlainObject(record));
            }
            catch (EndOfStreamException)
            {
                break;
            }
            catch (AvroException ex) when (ex.InnerException is EndOfStreamException)
            {
                break;
            }
        }

        return results;
    }

    private static object? ConvertToPlainObject(object? value)
    {
        switch (value)
        {
            case null:
                return null;
            case GenericRecord record:
            {
                var result = new Dictionary<string, object?>();
                foreach (var field in record.Schema.Fields)
                {
                    result[field.Name] = ConvertToPlainObject(record[field.Name]);
                }

                return result;
            }
            case IDictionary dictionary:
            {
                var result = new Dictionary<string, object?>();
                foreach (DictionaryEntry entry in dictionary)
                {
                    result[entry.Key?.ToString() ?? string.Empty] = ConvertToPlainObject(entry.Value);
                }

                return result;
            }
            case byte[] bytes:
                return Convert.ToBase64String(bytes);
            case ArraySegment<byte> segment:
                return Convert.ToBase64String(segment.ToArray());
            case string:
                return value;
            case IEnumerable enumerable:
            {
                var list = new List<object?>();
                foreach (var item in enumerable)
                {
                    list.Add(ConvertToPlainObject(item));
                }

                return list;
            }
            case bool or int or long or float or double or string or decimal:
                return value;
            default:
                return value?.ToString();
        }
    }
}
