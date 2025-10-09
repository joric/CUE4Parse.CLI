using CUE4Parse.Compression;
using CUE4Parse.Encryption.Aes;
using CUE4Parse.FileProvider;
using CUE4Parse.FileProvider.Vfs;
using CUE4Parse.MappingsProvider;
using CUE4Parse.UE4.AssetRegistry;
using CUE4Parse.UE4.Assets;
using CUE4Parse.UE4.Assets.Exports;
using CUE4Parse.UE4.Assets.Exports.Animation;
using CUE4Parse.UE4.Assets.Exports.Material;
using CUE4Parse.UE4.Assets.Exports.SkeletalMesh;
using CUE4Parse.UE4.Assets.Exports.Sound;
using CUE4Parse.UE4.Assets.Exports.StaticMesh;
using CUE4Parse.UE4.Assets.Exports.Texture;
using CUE4Parse.UE4.Assets.Exports.Wwise;
using CUE4Parse.UE4.Localization;
using CUE4Parse.UE4.Objects.Core.Misc;
using CUE4Parse.UE4.Objects.UObject;
using CUE4Parse.UE4.Oodle.Objects;
using CUE4Parse.UE4.Readers;
using CUE4Parse.UE4.Shaders;
using CUE4Parse.UE4.Versions;
using CUE4Parse.UE4.Wwise;
using CUE4Parse.Utils;
using CUE4Parse_Conversion;
using CUE4Parse_Conversion.Animations;
using CUE4Parse_Conversion.Meshes;
using CUE4Parse_Conversion.Sounds;
using CUE4Parse_Conversion.Textures;
using CUE4Parse_Conversion.Textures.BC;
using CUE4Parse_Conversion.UEFormat.Enums;
using Newtonsoft.Json;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using SkiaSharp;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Enumeration;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.CommandLine.Help;
using System.CommandLine.Builder;
using System.CommandLine.Invocation;
using System.Threading;

[Flags]
public enum ExportType
{
    None       = 0,
    Texture    = 1 << 0,
    Sound      = 1 << 1,
    Mesh       = 1 << 2,
    Animation  = 1 << 3,
    Other      = 1 << 4,
}

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        var sources     = new Option<string[]>(new[] { "-i", "--input" },       "Input game directory");
        var destination = new Option<string>  (new[] { "-o", "--output" },      "Output directory");
        var inputs      = new Option<string[]>(new[] { "-p", "--package" },     "Package path or wildcard pattern (repeatable)");
        var files       = new Option<string[]>(new[] { "-c", "--config" },      "Package list (repeatable)");
        var game        = new Option<string>  (new[] { "-g", "--game" },        ()=>"GAME_UE5_LATEST", "Game version");
        var keys        = new Option<string[]>(new[] { "-k", "--key" },         "AES key in hex format (repeatable)");
        var mappings    = new Option<string>  (new[] { "-m", "--mappings" },    "Mappings file");
        var format      = new Option<string>  (new[] { "-f", "--format" },      ()=> "auto", "Output format: raw, json, csv");
        var list        = new Option<bool>    (new[] { "-l", "--list" },        "List matching packages (supports csv)");
        var overwrite   = new Option<bool>    (new[] { "-y", "--yes" },         "Overwrite existing files");
        var verbose     = new Option<bool>    (new[] { "-v", "--verbose" },     "Enable verbose output");

        static string GetExamples()
        {
            var sb = new StringBuilder();
            sb.AppendLine("Examples:");
            sb.AppendLine("  Export all package names to a text file:");
            sb.AppendLine("    cue4parse -i MyGame -l > packages.txt");
            sb.AppendLine();
            sb.AppendLine("  Export a single package to stdout in json format:");
            sb.AppendLine("    cue4parse -i MyGame -p Assets/MyAsset.uasset -f json");
            sb.AppendLine();
            sb.AppendLine("  Export multiple packages matching wildcard patterns to a directory:");
            sb.AppendLine("    cue4parse -i MyGame -p */Textures* -p */Icons* -o Exports");
            sb.AppendLine();
            sb.AppendLine("  Export packages from list, overwrite existing files:");
            sb.AppendLine("    cue4parse -i MyGame -c packages.txt -o Exports -y");
            sb.AppendLine();
            return sb.ToString();
        }

        var cliVersion = Assembly.GetExecutingAssembly().GetName().Version;
        var libVersion = typeof(DefaultFileProvider).Assembly.GetName().Version;

        var root = new RootCommand($"CUE4Parse.CLI v{cliVersion} (built with CUE4Parse {libVersion})")
        {
            sources, destination, inputs, files, game, keys, mappings, format, list, overwrite, verbose
        };

        root.SetHandler(async (context) =>
        {
            await ExecuteAsync(
                context.ParseResult.GetValueForOption(sources) ?? Array.Empty<string>(),
                context.ParseResult.GetValueForOption(destination),
                context.ParseResult.GetValueForOption(inputs) ?? Array.Empty<string>(),
                context.ParseResult.GetValueForOption(files) ?? Array.Empty<string>(),
                context.ParseResult.GetValueForOption(game) ?? "GAME_UE5_LATEST",
                context.ParseResult.GetValueForOption(keys) ?? Array.Empty<string>(),
                context.ParseResult.GetValueForOption(mappings),
                context.ParseResult.GetValueForOption(format) ?? "auto",
                context.ParseResult.GetValueForOption(list),
                context.ParseResult.GetValueForOption(overwrite),
                context.ParseResult.GetValueForOption(verbose)
            );
        });

        if (args.Length == 0)
        {
            root.Invoke("-h");
            Console.WriteLine(GetExamples());
        }

        return await root.InvokeAsync(args);
    }

    internal static class PathSearch
    {
        public static bool IsMatch(string path, string pattern)
        {
            return FileSystemName.MatchesSimpleExpression(pattern, path);
        }

        public static IEnumerable<string> Filter(IEnumerable<string> paths, string pattern)
        {
            foreach (string path in paths)
            {
                if (IsMatch(path, pattern)) yield return path;
            }
        }
    }

    public static string _exportDirectory { get; set; } = "Export";
    public static bool _overwrite { get; set; } = false;

    private static async Task ExecuteAsync(string[] sources, string? destination, string[] inputs, string[] files,
        string game, string[] keys, string? mappings, string format, bool list, bool overwrite, bool verbose)
    {
        Program._overwrite = overwrite;

        if (verbose)
        {
            Log.Logger = new LoggerConfiguration().MinimumLevel.Verbose().WriteTo.Console().CreateLogger();
        }
        else
        {
            Log.Logger = new LoggerConfiguration().MinimumLevel.Fatal().WriteTo.Console().CreateLogger();
        }

        // Parse game version
        if (!Enum.TryParse<EGame>(game, out var gameVersion))
        {
            throw new ArgumentException($"Invalid game version: {game}");
        }

        // Create Version
        var version = new VersionContainer(gameVersion, ETexturePlatform.DesktopMobile);

        var directory = sources.Length>0 ? sources[0] : null;

        if (string.IsNullOrEmpty(directory)) return;

        Console.Error.WriteLine($"Loading {NormPath(directory)}...");

        // Create provider
        var provider = directory.EndsWith(".apk")
            ? new ApkFileProvider(directory, new VersionContainer(gameVersion))
            : new DefaultFileProvider(directory, SearchOption.AllDirectories, new VersionContainer(gameVersion));

        // Set mappings if specified
        if (!string.IsNullOrEmpty(mappings) && File.Exists(mappings))
        {
            provider.MappingsContainer = new FileUsmapTypeMappingsProvider(mappings);
        }

        // Init oodle
        var oodlePath = Path.Combine(Path.GetTempPath(), OodleHelper.OODLE_DLL_NAME);
        OodleHelper.DownloadOodleDll(oodlePath);
        OodleHelper.Initialize(oodlePath);

        // Initialize provider
        provider.Initialize();

        // Add AES keys
        foreach (var keyEntry in keys)
        {
            provider.SubmitKey(new FGuid(), new FAesKey(keyEntry));
        }

        // Mandatory key
        provider.SubmitKey(Guid.Empty, new FAesKey(new byte[32]));

        provider.PostMount();

        provider.ChangeCulture(provider.GetLanguageCode(ELanguage.English));

        Console.Error.WriteLine($"Total assets: {provider.Files.Count}");
        Console.Error.WriteLine($"Output format: {format}");

        // init detex library for BCD encoding
        var detexPath = Path.Combine(Path.GetTempPath(), DetexHelper.DLL_NAME);

        if (!File.Exists(detexPath))
            await DetexHelper.LoadDllAsync(detexPath);

        DetexHelper.Initialize(detexPath);

        // scan all inputs, add package path (wildcards allowed)
        var packagePaths = new List<string>();

        foreach (var packageListFile in files)
        {
            if (!string.IsNullOrEmpty(packageListFile) && File.Exists(packageListFile))
            {
                Console.Error.WriteLine($"Loading file list: {NormPath(packageListFile)}");

                foreach (var line in File.ReadLines(packageListFile))
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    var trimmed = line.Trim();
                    if (trimmed.StartsWith('#') || trimmed.StartsWith('[')) continue;
                    packagePaths.Add(trimmed);
                }
            }
        }

        foreach (var input in inputs) {
            if (!string.IsNullOrEmpty(input))
            {
                packagePaths.Add(input);
            }
        }

        // if no paths, add common wildcard
        if (packagePaths.Count == 0)
        {
            packagePaths.Add("*");
        }

        // final filtering by wildcards an paths
        var packages = new List<CUE4Parse.FileProvider.Objects.GameFile>();

        foreach (var path in packagePaths)
        {
            List<CUE4Parse.FileProvider.Objects.GameFile> matched;

            if (path.IndexOfAny(new[] { '*', '?' }) >= 0)
            {
                // Path contains wildcard, do filtering
                matched = PathSearch.Filter(provider.Files.Keys, path).Select(key => provider.Files[key]).ToList();
                Console.Error.WriteLine($"Added wildcard: {path} ({matched.Count} matches)");
            }
            else
            {   // No wildcard, just get the single entry if it exists
                matched = provider.Files.TryGetValue(path, out var obj)
                ? new List<CUE4Parse.FileProvider.Objects.GameFile> { obj }
                : new List<CUE4Parse.FileProvider.Objects.GameFile>();
            }

            packages.AddRange(matched);
        }

        if (packages.Count==0)
        {
            Console.Error.WriteLine("No matches, exiting.");
            return;
        }

        ExportType type = ExportType.Texture | ExportType.Sound | ExportType.Mesh | ExportType.Animation | ExportType.Other;

        var options = new ExporterOptions
        {
            LodFormat = ELodFormat.FirstLod,
            MeshFormat = EMeshFormat.UEFormat,
            AnimFormat = EAnimFormat.UEFormat,
            MaterialFormat = EMaterialFormat.AllLayersNoRef,
            TextureFormat = ETextureFormat.Png,
            CompressionFormat = EFileCompressionFormat.None,
            Platform = version.Platform,
            SocketFormat = ESocketFormat.Bone,
            ExportMorphTargets = true,
            ExportMaterials = false
        };

        // Check for output directory
        if (string.IsNullOrEmpty(destination)) {
            Console.Error.WriteLine("Output directory is not specified.");
            return;
        } else {
            Program._exportDirectory = NormPath(destination);
        }

        var counter = 0;
        var exportCount = 0;
        var watch = new Stopwatch();
        watch.Start();

        var cts = new CancellationTokenSource();

        //foreach (var package in packages)
        Parallel.ForEach(packages, new ParallelOptions { CancellationToken = cts.Token }, package =>
        {
            // list assets
            if (list)
            {
                if (!string.IsNullOrEmpty(format) && format=="csv")
                {
                    Console.WriteLine($"{package.Path},{provider.Files[package.Path].Size}");
                }
                else
                {
                    Console.WriteLine(package.Path);
                }
                return;
            }

            if (!verbose)
                Console.Error.Write($"Exporting package {counter+1} of {packages.Count}...         \r");

            var folder = package.Path.SubstringBeforeLast('/');
            string ext = Path.GetExtension(package.Name);

            // Select target format
            var targetFormat = format;
            if (targetFormat == "auto")
            {
                switch(ext)
                {
                    case ".umap": targetFormat = "json"; break;
                }
            }

            if (!provider.TryLoadPackage(package, out var pkg))
            {
                Log.Information($"Could not load asset (maybe raw data) {package.Name}");

                // check if we can't load uasset or map then something's fishy going on
                if (ext == ".uasset" || ext == ".umap") // do NOT add ubulk here!
                {
                    Console.Error.WriteLine($"Could not load standard asset, check game version, mappings or keys.");
                    cts.Cancel();
                    return;
                }

                // not a package, save raw data if allowed
                if (targetFormat == "auto" || targetFormat == "raw")
                {
                    targetFormat = "raw";
                }
                else
                {
                    Log.Information($"Incompatible format: {targetFormat} (format: {format}, ext: {ext}) for {package.Name}");
                    counter++;
                    return;
                }
            }

            if (targetFormat == "raw") {
                SaveRaw(folder, package, provider, ref exportCount);
                counter++;
                return;
            }

            if (targetFormat == "json") {
                SaveJson(folder, package.Name, pkg, ref exportCount);
                counter++;
                return;
            }

            // types below are only auto
            if (targetFormat != "auto") {
                counter++;
                return;
            }

            bool parsed = false;

            // optimized way of checking for exports type without loading most of them
            for (var i = 0; i < pkg.ExportMapLength; i++)
            {
                var pointer = new FPackageIndex(pkg, i + 1).ResolvedObject;
                if (pointer?.Object is null) continue;

                var dummy = ((AbstractUePackage) pkg).ConstructObject(pointer.Class?.Object?.Value as UStruct, pkg);

                //Console.WriteLine($"{dummy?.GetType().Name} - {package.Name}");

                switch (dummy)
                {
                    case UTexture when type.HasFlag(ExportType.Texture) && pointer.Object.Value is UTexture texture:
                    {
                        try
                        {
                            Log.Information("{ExportType} found in {PackageName}", dummy.ExportType, package.Name);
                            SaveTexture(folder, texture, options.Platform, options, ref exportCount);
                        }
                        catch (Exception e)
                        {
                            Log.Warning(e, "failed to decode {TextureName}", texture.Name);
                        }
                        parsed = true;

                        break;
                    }
                    case USoundWave when type.HasFlag(ExportType.Sound):
                    case UAkMediaAssetData when type.HasFlag(ExportType.Sound):
                    {
                        Log.Information("{ExportType} found in {PackageName}", dummy.ExportType, package.Name);

                        pointer.Object.Value.Decode(true, out var mediaFormat, out var bytes);
                        if (bytes is not null)
                        {
                            var fileName = $"{pointer.Object.Value.Name}.{mediaFormat.ToLower()}";
                            WriteToFile(folder, fileName, bytes, fileName, ref exportCount);
                        }
                        parsed = true;

                        break;
                    }
                    case UAnimSequenceBase when type.HasFlag(ExportType.Animation):
                    case USkeletalMesh when type.HasFlag(ExportType.Mesh):
                    case UStaticMesh when type.HasFlag(ExportType.Mesh):
                    case USkeleton when type.HasFlag(ExportType.Mesh):
                    {
                        Log.Information("{ExportType} found in {PackageName}", dummy.ExportType, package.Name);

                        var exporter = new CUE4Parse_Conversion.Exporter(pointer.Object.Value, options);
                        if (exporter.TryWriteToDir(new DirectoryInfo(_exportDirectory), out _, out var filePath))
                        {
                            WriteToLog(folder, Path.GetFileName(filePath), ref exportCount);
                        }
                        break;
                        parsed = true;
                    }
                }
            }

            if (!parsed)
            {
                SaveJson(folder, package.Name, pkg, ref exportCount);
            }

            counter++;

        }); // parallel foreach, must end with "});"
        //} // simple foreach

        watch.Stop();

        if (!verbose)
            Console.Error.WriteLine($"Processed {packages.Count} packages in {watch.Elapsed}");

        Log.Information("Processed {Packages.Count} packages in {watch.Elapsed}", packages.Count, watch.Elapsed);

        return;
    }

    public static string NormPath(string path)
    {
        if (string.IsNullOrEmpty(path)) return ".";
        path = path.Replace('/', Path.DirectorySeparatorChar);
        path = path.Replace("\\\\", "\\");
        return path;
    }

    private static void SaveRaw(string folder, CUE4Parse.FileProvider.Objects.GameFile package, AbstractVfsFileProvider provider, ref int exportCount)
    {
        var name = package.Name;
        var outPath = Path.Combine(_exportDirectory, folder, name);

        if (!CheckFile(outPath)) return;
        Directory.CreateDirectory(Path.Combine(_exportDirectory, folder));

        byte[] bytes = provider.Files[package.Path].Read();
        WriteToFile(folder, name, bytes, $"{name}", ref exportCount);
    }

    private static void SaveJson(string folder, string name, IPackage pkg, ref int exportCount)
    {
        string fileName = Path.ChangeExtension(name, ".json");
        var outPath = Path.Combine(_exportDirectory, folder, fileName);

        if (!CheckFile(outPath)) return;
        Directory.CreateDirectory(Path.Combine(_exportDirectory, folder));

        IEnumerable<UObject> exports = pkg.GetExports();
        JsonSerializer serializer = new();
        serializer.Formatting = Formatting.Indented;
        using StreamWriter stream = new(outPath, false, Encoding.UTF8);
        using JsonWriter writer = new JsonTextWriter(stream);
        serializer.Serialize(writer, exports);

        WriteToLog(folder, $"{name}", ref exportCount);
    }

    private static void SaveTexture(string folder, UTexture texture, ETexturePlatform platform, ExporterOptions options, ref int exportCount)
    {
        var outPath = Path.Combine(_exportDirectory, folder, texture.Name);

        foreach (var ext in new[] { ".png", ".hdr" })
        {
            var path = Path.ChangeExtension(outPath, ext);
            if (!CheckFile(path, true))
            {
                CheckFile(path);
                return;
            }
        }

        var bitmaps = new[] { texture.Decode(platform) };
        switch (texture)
        {
            case UTexture2DArray textureArray:
                bitmaps = textureArray.DecodeTextureArray(platform);
                break;
            case UTextureCube:
                bitmaps[0] = bitmaps[0]?.ToPanorama();
                break;
        }

        foreach (var bitmap in bitmaps ?? Array.Empty<CTexture>())
        {
            if (bitmap is null) continue;
            bool SaveHdrTexturesAsHdr = true;
            var bytes = bitmap.Encode(options.TextureFormat, SaveHdrTexturesAsHdr, out var extension);
            var fileName = $"{texture.Name}.{extension}";
            WriteToFile(folder, fileName, bytes, $"{fileName} ({bitmap.Width}x{bitmap.Height})", ref exportCount);
        }
    }

    private static void WriteToFile(string folder, string fileName, byte[] bytes, string logMessage, ref int exportCount)
    {
        var outPath = Path.Combine(_exportDirectory, folder, fileName);
        if (!CheckFile(outPath)) return;
        Directory.CreateDirectory(Path.Combine(_exportDirectory, folder));
        File.WriteAllBytesAsync(outPath, bytes);
        WriteToLog(folder, logMessage, ref exportCount);
    }

    private static bool CheckFile(string outPath, bool silent=false)
    {
        bool skipFile = !_overwrite && File.Exists(outPath);

        if (!silent)
        {
            if (skipFile)
                Log.Warning("Already exists {Path}", NormPath(outPath));
            else
                Log.Information("Writing {Path}", NormPath(outPath));
        }

        return !skipFile;
    }

    private static void WriteToLog(string folder, string logMessage, ref int exportCount)
    {
        //Console.Error.WriteLine($"Exported {logMessage} out of {folder}");
        //Log.Information($"Exported {logMessage} out of {folder}");
        exportCount++;
    }
}

