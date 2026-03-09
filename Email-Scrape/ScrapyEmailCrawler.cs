using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Email_Scrape;

internal sealed class ScrapyEmailCrawler : IEmailCrawler
{
    private readonly string _pythonExecutable;
    private readonly string _workerScriptPath;
    private readonly TimeSpan _timeout;

    public ScrapyEmailCrawler(string pythonExecutable, TimeSpan timeout)
    {
        _pythonExecutable = string.IsNullOrWhiteSpace(pythonExecutable) ? "python" : pythonExecutable;
        _workerScriptPath = ResolveWorkerScriptPath();
        _timeout = timeout;
    }

    public static bool IsScrapyAvailable(string pythonExecutable, out string detail)
    {
        detail = string.Empty;

        try
        {
            var processStartInfo = new ProcessStartInfo
            {
                FileName = string.IsNullOrWhiteSpace(pythonExecutable) ? "python" : pythonExecutable,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                RedirectStandardInput = false,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            processStartInfo.ArgumentList.Add("-c");
            processStartInfo.ArgumentList.Add("import scrapy,sys;sys.stdout.write(scrapy.__version__)");

            using var process = Process.Start(processStartInfo);
            if (process is null)
            {
                detail = "Failed to start Python process.";
                return false;
            }

            var stdout = process.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            var exited = process.WaitForExit((int)TimeSpan.FromSeconds(10).TotalMilliseconds);
            if (!exited)
            {
                TryKillProcess(process);
                detail = "Timed out while validating Scrapy installation.";
                return false;
            }

            if (process.ExitCode != 0)
            {
                detail = string.IsNullOrWhiteSpace(stderr)
                    ? $"Python exited with code {process.ExitCode}."
                    : stderr.Trim();
                return false;
            }

            detail = string.IsNullOrWhiteSpace(stdout) ? "available" : stdout.Trim();
            return true;
        }
        catch (Exception ex)
        {
            detail = ex.Message;
            return false;
        }
    }

    public async Task<CrawlSummary> CrawlAsync(
        IReadOnlyList<Uri> seedUris,
        CrawlOptions options,
        Action<CrawlProgress>? onProgress,
        Action<EmailMatch>? onEmailFound,
        Action<string>? onLog,
        CancellationToken cancellationToken)
    {
        if (seedUris.Count == 0)
        {
            return new CrawlSummary(0, 0, 0, 0, TimeSpan.Zero);
        }

        var processStartInfo = new ProcessStartInfo
        {
            FileName = _pythonExecutable,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = AppContext.BaseDirectory
        };

        processStartInfo.ArgumentList.Add("-u");
        processStartInfo.ArgumentList.Add(_workerScriptPath);

        using var process = new Process
        {
            StartInfo = processStartInfo,
            EnableRaisingEvents = false
        };

        var started = process.Start();
        if (!started)
        {
            throw new InvalidOperationException("Unable to start the Scrapy worker process.");
        }

        using var cancellationRegistration = cancellationToken.Register(() => TryKillProcess(process));

        var stopwatch = Stopwatch.StartNew();

        var stdoutTask = ConsumeStdOutAsync(process.StandardOutput, onProgress, onEmailFound, onLog);
        var stderrTask = ConsumeStdErrAsync(process.StandardError, onLog);

        var configPayload = new Dictionary<string, object?>
        {
            ["seed_urls"] = seedUris.Select(uri => uri.AbsoluteUri).ToArray(),
            ["max_pages"] = options.MaxPages,
            ["max_concurrency"] = options.MaxConcurrency,
            ["timeout_seconds"] = Math.Max(1, (int)Math.Round(_timeout.TotalSeconds)),
            ["same_host_only"] = options.SameHostOnly,
            ["max_links_per_page"] = options.MaxLinksPerPage,
            ["max_depth"] = Math.Max(1, options.MaxDepth),
            ["download_delay_ms"] = Math.Max(0, options.DownloadDelayMs),
            ["enable_theharvester"] = options.EnableTheHarvester,
            ["theharvester_sources"] = options.TheHarvesterSources?.ToArray() ?? Array.Empty<string>(),
            ["theharvester_source_timeout_seconds"] = Math.Max(5, options.TheHarvesterSourceTimeoutSeconds),
            ["theharvester_seed_cap"] = Math.Max(1, options.TheHarvesterSeedCap)
        };

        var configJson = JsonSerializer.Serialize(configPayload);
        await process.StandardInput.WriteAsync(configJson).ConfigureAwait(false);
        await process.StandardInput.FlushAsync().ConfigureAwait(false);
        process.StandardInput.Close();

        await process.WaitForExitAsync().ConfigureAwait(false);
        var summary = await stdoutTask.ConfigureAwait(false);
        var stderrTail = await stderrTask.ConfigureAwait(false);

        cancellationToken.ThrowIfCancellationRequested();

        if (process.ExitCode != 0)
        {
            var detail = string.IsNullOrWhiteSpace(stderrTail)
                ? $"Scrapy worker exited with code {process.ExitCode}."
                : $"Scrapy worker exited with code {process.ExitCode}: {stderrTail}";
            throw new InvalidOperationException(detail);
        }

        stopwatch.Stop();
        if (summary.Duration <= TimeSpan.Zero)
        {
            summary = summary with { Duration = stopwatch.Elapsed };
        }

        return summary;
    }

    public void Dispose()
    {
        // The worker is process-scoped per crawl call; no persistent resources to release here.
    }

    private static async Task<CrawlSummary> ConsumeStdOutAsync(
        StreamReader reader,
        Action<CrawlProgress>? onProgress,
        Action<EmailMatch>? onEmailFound,
        Action<string>? onLog)
    {
        var processedPages = 0;
        var failedPages = 0;
        var uniqueEmailCount = 0;
        var totalEmailHits = 0;
        var duration = TimeSpan.Zero;
        var sawSummary = false;

        while (true)
        {
            var line = await reader.ReadLineAsync().ConfigureAwait(false);
            if (line is null)
            {
                break;
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            JsonDocument document;
            try
            {
                document = JsonDocument.Parse(line);
            }
            catch (JsonException)
            {
                onLog?.Invoke($"[Scrapy] {line}");
                continue;
            }

            using (document)
            {
                var root = document.RootElement;
                if (!root.TryGetProperty("type", out var typeElement))
                {
                    continue;
                }

                var eventType = typeElement.GetString();
                if (string.Equals(eventType, "email", StringComparison.OrdinalIgnoreCase))
                {
                    var email = GetString(root, "email");
                    var sourceUrl = GetString(root, "source_url");
                    if (!string.IsNullOrWhiteSpace(email) && !string.IsNullOrWhiteSpace(sourceUrl))
                    {
                        onEmailFound?.Invoke(new EmailMatch(email, sourceUrl));
                    }
                }
                else if (string.Equals(eventType, "progress", StringComparison.OrdinalIgnoreCase))
                {
                    processedPages = GetInt(root, "processed_pages", processedPages);
                    failedPages = GetInt(root, "failed_pages", failedPages);
                    uniqueEmailCount = GetInt(root, "unique_email_count", uniqueEmailCount);
                    totalEmailHits = GetInt(root, "total_email_hits", totalEmailHits);
                    var queueLength = GetInt(root, "queue_length", 0);
                    var currentUrl = GetString(root, "current_url");

                    onProgress?.Invoke(new CrawlProgress(
                        ProcessedPages: processedPages,
                        FailedPages: failedPages,
                        QueueLength: queueLength,
                        UniqueEmailCount: uniqueEmailCount,
                        TotalEmailHits: totalEmailHits,
                        CurrentUrl: currentUrl));
                }
                else if (string.Equals(eventType, "log", StringComparison.OrdinalIgnoreCase))
                {
                    var message = GetString(root, "message");
                    if (!string.IsNullOrWhiteSpace(message))
                    {
                        onLog?.Invoke($"[Scrapy] {message}");
                    }
                }
                else if (string.Equals(eventType, "summary", StringComparison.OrdinalIgnoreCase))
                {
                    processedPages = GetInt(root, "processed_pages", processedPages);
                    failedPages = GetInt(root, "failed_pages", failedPages);
                    uniqueEmailCount = GetInt(root, "unique_email_count", uniqueEmailCount);
                    totalEmailHits = GetInt(root, "total_email_hits", totalEmailHits);
                    var durationSeconds = GetDouble(root, "duration_seconds", 0d);
                    duration = durationSeconds <= 0 ? TimeSpan.Zero : TimeSpan.FromSeconds(durationSeconds);
                    sawSummary = true;
                }
            }
        }

        if (!sawSummary)
        {
            duration = TimeSpan.Zero;
        }

        return new CrawlSummary(
            ProcessedPages: processedPages,
            FailedPages: failedPages,
            UniqueEmailCount: uniqueEmailCount,
            TotalEmailHits: totalEmailHits,
            Duration: duration);
    }

    private static async Task<string> ConsumeStdErrAsync(StreamReader reader, Action<string>? onLog)
    {
        var tailBuffer = new Queue<string>();

        while (true)
        {
            var line = await reader.ReadLineAsync().ConfigureAwait(false);
            if (line is null)
            {
                break;
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            onLog?.Invoke($"[Scrapy:stderr] {line}");

            tailBuffer.Enqueue(line.Trim());
            while (tailBuffer.Count > 20)
            {
                tailBuffer.Dequeue();
            }
        }

        return string.Join(" | ", tailBuffer);
    }

    private static int GetInt(JsonElement root, string propertyName, int fallback)
    {
        if (!root.TryGetProperty(propertyName, out var value))
        {
            return fallback;
        }

        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var parsedNumber))
        {
            return parsedNumber;
        }

        if (value.ValueKind == JsonValueKind.String &&
            int.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedText))
        {
            return parsedText;
        }

        return fallback;
    }

    private static double GetDouble(JsonElement root, string propertyName, double fallback)
    {
        if (!root.TryGetProperty(propertyName, out var value))
        {
            return fallback;
        }

        if (value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var parsedNumber))
        {
            return parsedNumber;
        }

        if (value.ValueKind == JsonValueKind.String &&
            double.TryParse(value.GetString(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsedText))
        {
            return parsedText;
        }

        return fallback;
    }

    private static string GetString(JsonElement root, string propertyName)
    {
        if (!root.TryGetProperty(propertyName, out var value))
        {
            return string.Empty;
        }

        if (value.ValueKind == JsonValueKind.String)
        {
            return value.GetString() ?? string.Empty;
        }

        return value.ToString();
    }

    private static string ResolveWorkerScriptPath()
    {
        var candidate = Path.Combine(AppContext.BaseDirectory, "Python", "scrapy_email_worker.py");
        if (File.Exists(candidate))
        {
            return candidate;
        }

        throw new FileNotFoundException(
            "Scrapy worker script was not found in output directory. Expected path: " + candidate,
            candidate);
    }

    private static void TryKillProcess(Process process)
    {
        try
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }
        catch
        {
            // Ignore cancellation race conditions when the process exits naturally.
        }
    }
}
