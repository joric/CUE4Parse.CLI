using System;
using System.Reflection;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CUE4Parse.Encryption.Aes;
using CUE4Parse.FileProvider;
using CUE4Parse.FileProvider.Vfs;
using CUE4Parse.MappingsProvider;
using CUE4Parse.UE4.AssetRegistry;
using CUE4Parse.UE4.Assets.Exports;
using CUE4Parse.UE4.Assets.Exports.Texture;
using CUE4Parse.UE4.Localization;
using CUE4Parse.UE4.Oodle.Objects;
using CUE4Parse.UE4.Readers;
using CUE4Parse.UE4.Shaders;
using CUE4Parse.UE4.Versions;
using CUE4Parse.UE4.Wwise;
using CUE4Parse_Conversion.Textures;
using CUE4Parse_Conversion.Textures.BC;
using CUE4Parse_Conversion;
using CUE4Parse_Conversion.Animations;
using CUE4Parse_Conversion.Meshes;
using CUE4Parse_Conversion.Sounds;
using CUE4Parse_Conversion.UEFormat.Enums;
using CUE4Parse.Compression;
using CUE4Parse.UE4.Assets;
using CUE4Parse.UE4.Assets.Exports.Animation;
using CUE4Parse.UE4.Assets.Exports.Material;
using CUE4Parse.UE4.Assets.Exports.SkeletalMesh;
using CUE4Parse.UE4.Assets.Exports.Sound;
using CUE4Parse.UE4.Assets.Exports.StaticMesh;
using CUE4Parse.UE4.Assets.Exports.Wwise;
using CUE4Parse.UE4.Objects.Core.Misc;
using CUE4Parse.UE4.Objects.UObject;
using CUE4Parse.Utils;
using Newtonsoft.Json;
using Serilog;
using System.CommandLine;
using System.IO.Enumeration;
using System.Collections.Generic;
using System.Text;

#nullable enable

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

public static class Program
{
    public static string _exportDirectory { get; set; } = "Export";
    public static bool _overwrite { get; set; } = false;

    public static int Main(string[] args)
    {
        // Configure logging to be minimal for CLI use
        //Log.Logger = new LoggerConfiguration().MinimumLevel.Error().WriteTo.Console().CreateLogger();
        //Log.Logger = new LoggerConfiguration().MinimumLevel.Information().WriteTo.Console().CreateLogger();

        Log.Logger = new LoggerConfiguration().MinimumLevel.Fatal().WriteTo.Console().CreateLogger();

        // Define CLI options
        var directoryOption = new Option<string>(
            new[]{"--directory","-d"},
            "Path to the game files directory"
        )
        { IsRequired = true };

        var versionOption = new Option<string>(
            new[]{"--game-version","-g"},
            () => "GAME_UE5_LATEST",
            "Game version (e.g., GAME_UE5_3)"
        )
        { IsRequired = true };

        var mappingsOption = new Option<string?>(
            new[]{"--mappings", "-m"},
            "Path to mappings file (.usmap)"
        );

        var aesKeyOption = new Option<string[]>(
            new[]{"--aes-key", "-k"},
            "AES key in hex format (can be specified multiple times)"
        )
        { AllowMultipleArgumentsPerToken = true };

        var listPackagesOption = new Option<bool>(
            new[]{"--list-packages", "-l"},
            "List all available packages"
        );

        var packageOption = new Option<string?>(
            new[]{"--package", "--object", "-p"},
            "Package (.asset) path to export (can use wildcards)"
        );

        var packageInfoOption = new Option<bool>(
            "--package-info",
            "Get package information"
        );

        var exportOption = new Option<bool>(
            new[]{"--export", "-e"},
            "Export object(s)"
        );

        var outputOption = new Option<string?>(
            new[]{"--output", "-o"},
            () => "Exports",
            "Output file path for exports"
        );

        var outputFormatOption = new Option<string>(
            new[]{"--output-format", "-t"},
            () => "auto",
            "Output format (json, png, fbx, etc.)"
        );

        var packageListOption = new Option<string>(
            new[]{"--input", "-i"},
            "File with package paths to export (can use wildcards)"
        );

        var overwriteOption = new Option<bool>(
            new[]{"--force-overwrite", "-f"},
            "Force overwrite existing files"
        );

        // Create root command
        var rootCommand = new RootCommand("CUE4Parse CLI tool")
        {
            directoryOption,
            versionOption,
            mappingsOption,
            aesKeyOption,
            listPackagesOption,
            packageOption,
            packageInfoOption,
            exportOption,
            outputOption,
            outputFormatOption,
            packageListOption,
            overwriteOption,
        };

        rootCommand.SetHandler(async (context) =>
        {
            try
            {
                var directory = context.ParseResult.GetValueForOption(directoryOption)!;
                var versionString = context.ParseResult.GetValueForOption(versionOption)!;
                var mappings = context.ParseResult.GetValueForOption(mappingsOption);
                var aesKeys = context.ParseResult.GetValueForOption(aesKeyOption) ?? Array.Empty<string>();
                var listPackages = context.ParseResult.GetValueForOption(listPackagesOption);
                var packagePath = context.ParseResult.GetValueForOption(packageOption);
                var packageInfo = context.ParseResult.GetValueForOption(packageInfoOption);
                var export = context.ParseResult.GetValueForOption(exportOption);
                var output = context.ParseResult.GetValueForOption(outputOption);
                var outputFormat = context.ParseResult.GetValueForOption(outputFormatOption)!;
                var packageListFile = context.ParseResult.GetValueForOption(packageListOption);
                var overwrite = context.ParseResult.GetValueForOption(overwriteOption);

                await ProcessCommand(directory, versionString, mappings, aesKeys, listPackages,
                    packagePath, packageInfo, export, output, outputFormat, packageListFile, overwrite);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Error: {ex.Message}");
                context.ExitCode = 1;
            }
        });

        return rootCommand.Invoke(args);
    }

    private static async Task ProcessCommand(string directory, string versionString, string? mappings,
        string[] aesKeys, bool listPackages, string? packagePath, bool packageInfo,
        bool export, string? output, string outputFormat, string? packageListFile, bool overwrite)
    {
        var libVersion = typeof(DefaultFileProvider).Assembly.GetName().Version;
        var cliVersion = Assembly.GetExecutingAssembly().GetName().Version;

        Console.Error.WriteLine($"Using CUE4Parse {libVersion}, CUE4Parse.CLI {cliVersion}");

        // Init oodle
        OodleHelper.DownloadOodleDll();
        OodleHelper.Initialize(OodleHelper.OODLE_DLL_NAME);

        // Parse game version
        if (!Enum.TryParse<EGame>(versionString, out var gameVersion))
        {
            throw new ArgumentException($"Invalid game version: {versionString}");
        }

        // Create Version
        var version = new VersionContainer(gameVersion, ETexturePlatform.DesktopMobile);

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

        // Create provider
        var provider = directory.EndsWith(".apk")
            ? new ApkFileProvider(directory, new VersionContainer(gameVersion))
            : new DefaultFileProvider(directory, SearchOption.TopDirectoryOnly, new VersionContainer(gameVersion));

        // Set mappings if provided
        if (!string.IsNullOrEmpty(mappings) && File.Exists(mappings))
        {
            provider.MappingsContainer = new FileUsmapTypeMappingsProvider(mappings);
        }

        // Initialize provider
        provider.Initialize();

        // Add AES keys
        foreach (var keyEntry in aesKeys)
        {
            provider.SubmitKey(new FGuid(), new FAesKey(keyEntry));
        }

        // mandatory
        provider.SubmitKey(Guid.Empty, new FAesKey(new byte[32]));

        provider.PostMount();

        provider.ChangeCulture(provider.GetLanguageCode(ELanguage.English));

        Console.Error.WriteLine($"Total packages in the game: {provider.Files.Count}");

        var detexPath = Path.Combine(AppContext.BaseDirectory, DetexHelper.DLL_NAME);
        if (!File.Exists(detexPath))
            await DetexHelper.LoadDllAsync(detexPath);
        DetexHelper.Initialize(detexPath);

        if (!string.IsNullOrEmpty(output)) Program._exportDirectory = output;
        Program._overwrite = overwrite;

        // Process commands
        if (listPackages)
        {
            var packages = provider.Files.Keys
                .Where(x => x.EndsWith(".uasset") || x.EndsWith(".umap"))
                .OrderBy(x => x)
                .ToList();

            foreach (var pkg in packages)
            {
                Console.WriteLine(pkg);
            }
        }
        else if (packageInfo && !string.IsNullOrEmpty(packagePath))
        {
            var pkg = provider.LoadPackage(packagePath);
            var exports = pkg.GetExports();

            var packageInfoObj = new
            {
                name = packagePath,
                exports = exports.Select(export => new
                {
                    name = export.Name,
                    class_name = export.Class?.Name ?? "Unknown",
                    outer = export.Outer?.ToString(),
                }).ToList()
            };

            Console.WriteLine(JsonConvert.SerializeObject(packageInfoObj, Formatting.Indented));
        }
        else if (export && (!string.IsNullOrEmpty(packagePath) || !string.IsNullOrEmpty(packageListFile)))
        {
            ExportType type = ExportType.Texture | ExportType.Sound | ExportType.Mesh | ExportType.Animation | ExportType.Other;

            var packagePaths = new List<string>();

            if (!string.IsNullOrEmpty(packageListFile) && File.Exists(packageListFile))
            {
                Console.Error.WriteLine($"Loading asset list: {packageListFile}");
                foreach (var line in File.ReadLines(packageListFile))
                {
                    //Console.Error.WriteLine($"Loading line: {line}");
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    var trimmed = line.Trim();
                    if (trimmed.StartsWith('#') || trimmed.StartsWith('[')) continue;
                    packagePaths.Add(trimmed);
                }
            }

            if (!string.IsNullOrEmpty(packagePath))
            {
                packagePaths.Add(packagePath);
            }

            var exportCount = 0;
            var watch = new Stopwatch();
            watch.Start();

            var counter = 0;
            var total = packagePaths.Count;

            foreach (var path in packagePaths)
            {
                List<CUE4Parse.FileProvider.Objects.GameFile> packages;
                if (path.IndexOfAny(new[] { '*', '?' }) >= 0)
                {
                    // Path contains wildcard, do filtering
                    var filteredPaths = PathSearch.Filter(provider.Files.Keys, path);
                    packages = filteredPaths.Select(key => provider.Files[key]).ToList();
                }
                else
                {
                    // No wildcard, just get the single entry if it exists
                    packages = provider.Files.TryGetValue(path, out var obj) ? new List<CUE4Parse.FileProvider.Objects.GameFile> { obj } : new List<CUE4Parse.FileProvider.Objects.GameFile>();
                }

                //var filteredPaths = PathSearch.Filter(provider.Files.Keys, path);
                //var packages = filteredPaths.Select(key => provider.Files[key]).ToList();

                //Console.Error.WriteLine($"PackagePath: {path}, matches {packages.Count} package(s).");

                total += packages.Count-1;

                Parallel.ForEach(packages, package =>
                {
                    counter += 1;

                    var folder = package.Path.SubstringBeforeLast('/');
                    string ext = Path.GetExtension(package.Name);

                    if (type.HasFlag(ExportType.Other))
                    {
                        switch(ext)
                        {
                            case ".umap":
                            {
                                var outPath = Path.Combine(_exportDirectory, folder, Path.ChangeExtension(package.Name, ".json"));
                                outPath = outPath.Replace('/', Path.DirectorySeparatorChar);

                                if (!overwrite && File.Exists(outPath))
                                {
                                    Console.Error.WriteLine($"UMAP file already exists: {outPath}");
                                    return;
                                }

                                Console.Error.WriteLine($"Writing ({counter}/{total}): {package}");

                                Directory.CreateDirectory(Path.Combine(_exportDirectory, folder));

                                IEnumerable<UObject> exports = provider.LoadPackage(package).GetExports();

                                JsonSerializer serializer = new();
                                serializer.Formatting = Formatting.Indented;

                                using StreamWriter stream = new(outPath, false, Encoding.UTF8);
                                using JsonWriter writer = new JsonTextWriter(stream);
                                serializer.Serialize(writer, exports);

                                return;
                            }
                        }
                    }

                    if (!provider.TryLoadPackage(package, out var pkg)) {
                        //Console.Error.WriteLine($"Failed to load {package}");
                        try
                        {
                            // possibly raw? save as is
                            string fileName = package.Name;
                            string outPath = Path.Combine(_exportDirectory, folder, package.Name);
                            byte[] bytes = provider.Files[package.Path].Read();
                            WriteToFile(folder, fileName, bytes, $"{fileName}", ref exportCount);

                        } catch (Exception ex)
                        {
                            Console.Error.WriteLine($"Exception writing raw package {package}, error: {ex.Message}");
                        }

                        return;
                    }

                    // optimized way of checking for exports type without loading most of them
                    for (var i = 0; i < pkg.ExportMapLength; i++)
                    {
                        var pointer = new FPackageIndex(pkg, i + 1).ResolvedObject;
                        if (pointer?.Object is null) continue;

                        var dummy = ((AbstractUePackage) pkg).ConstructObject(pointer.Class?.Object?.Value as UStruct, pkg);
                        switch (dummy)
                        {
                            case UTexture when type.HasFlag(ExportType.Texture) && pointer.Object.Value is UTexture texture:
                            {
                                try
                                {
                                    Log.Information("{ExportType} found in {PackageName}", dummy.ExportType, package.Name);

                                    var outPath = Path.Combine(_exportDirectory, folder, package.Name);
                                    outPath = outPath.Replace('/', Path.DirectorySeparatorChar);

                                    var p0 = outPath.Replace(".uasset","");
                                    var p1 = Path.ChangeExtension(outPath, ".png");
                                    var p2 = Path.ChangeExtension(outPath, ".hdr");

                                    if (!overwrite && (File.Exists(p1) || File.Exists(p2)))
                                    {
                                        Console.Error.WriteLine($"Image already exists: {p0}");
                                        return;
                                    }

                                    SaveTexture(folder, texture, version.Platform, options, ref exportCount);
                                }
                                catch (Exception e)
                                {
                                    Log.Warning(e, "failed to decode {TextureName}", texture.Name);
                                    return;
                                }
                                break;
                            }
                            case USoundWave when type.HasFlag(ExportType.Sound):
                            case UAkMediaAssetData when type.HasFlag(ExportType.Sound):
                            {
                                Log.Information("{ExportType} found in {PackageName}", dummy.ExportType, package.Name);

                                pointer.Object.Value.Decode(true, out var format, out var bytes);
                                if (bytes is not null)
                                {
                                    var fileName = $"{pointer.Object.Value.Name}.{format.ToLower()}";
                                    WriteToFile(folder, fileName, bytes, fileName, ref exportCount);
                                }

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
                            }
                        }
                    }
                });
            }

            watch.Stop();

            Log.Information("exported {ExportCount} files ({Types}) in {Time}",
                exportCount,
                type.ToStringBitfield(),
                watch.Elapsed);

            Console.Error.Write($"Exported {exportCount} files in {watch.Elapsed}");
        }
        else
        {
            throw new ArgumentException("Invalid command combination. Use --help for usage information.");
        }
    }

    private static void SaveTexture(string folder, UTexture texture, ETexturePlatform platform, ExporterOptions options, ref int exportCount)
    {
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

        foreach (var bitmap in bitmaps.Where(b => b != null))
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
        outPath = outPath.Replace('/', Path.DirectorySeparatorChar);

        if (!_overwrite && File.Exists(outPath))
        {
            Console.Error.WriteLine($"File already exists: {outPath}");
            return;
        }

        Console.Error.WriteLine($"Writing: {outPath}");

        Directory.CreateDirectory(Path.Combine(_exportDirectory, folder));
        File.WriteAllBytesAsync(outPath, bytes);

        WriteToLog(folder, logMessage, ref exportCount);
    }

    private static void WriteToLog(string folder, string logMessage, ref int exportCount)
    {
        Log.Information("exported {LogMessage} out of {Folder}", logMessage, folder);
        exportCount++;
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
}
