using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Mail;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Email_Scrape;

internal sealed class EmailCrawler : IEmailCrawler
{
    private static readonly Regex EmailRegex = new(
        @"(?<![A-Z0-9._%+-])[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,63}(?![A-Z0-9._%+-])",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex HrefRegex = new(
        @"href\s*=\s*(?:\""(?<url>[^\"">]+)\""|'(?<url>[^'>]+)'|(?<url>[^\s>]+))",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly HashSet<string> NonHtmlExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp", ".ico",
        ".pdf", ".zip", ".rar", ".7z", ".tar", ".gz", ".mp3", ".wav", ".mp4",
        ".avi", ".mov", ".wmv", ".mkv", ".css", ".js", ".json", ".xml", ".csv",
        ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".exe", ".msi", ".dmg",
        ".woff", ".woff2", ".ttf", ".eot", ".otf"
    };

    private readonly HttpClient _httpClient;

    public EmailCrawler(TimeSpan timeout)
    {
        var handler = new HttpClientHandler
        {
            AllowAutoRedirect = true,
            AutomaticDecompression = DecompressionMethods.Brotli | DecompressionMethods.Deflate | DecompressionMethods.GZip
        };

        _httpClient = new HttpClient(handler)
        {
            Timeout = timeout
        };

        _httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("EmailScrape/1.0");
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

        var frontier = new ConcurrentQueue<Uri>();
        var seenUrlKeys = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);
        var uniqueEmails = new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase);

        var allowedHosts = new HashSet<string>(seedUris.Select(u => u.Host), StringComparer.OrdinalIgnoreCase);

        foreach (var seed in seedUris)
        {
            var normalizedSeed = NormalizeUri(seed);
            if (seenUrlKeys.TryAdd(normalizedSeed, 0))
            {
                frontier.Enqueue(seed);
            }
        }

        var sw = Stopwatch.StartNew();
        var budget = options.MaxPages;
        var processedPages = 0;
        var failedPages = 0;
        var totalEmailHits = 0;
        var activeWorkers = 0;

        async Task WorkerAsync()
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (!frontier.TryDequeue(out var currentUrl))
                {
                    if (Volatile.Read(ref activeWorkers) == 0 && frontier.IsEmpty)
                    {
                        return;
                    }

                    await Task.Delay(75, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (Interlocked.Decrement(ref budget) < 0)
                {
                    return;
                }

                Interlocked.Increment(ref activeWorkers);
                var countedPage = false;

                try
                {
                    var fetchResult = await TryFetchHtmlAsync(currentUrl, cancellationToken).ConfigureAwait(false);
                    Interlocked.Increment(ref processedPages);
                    countedPage = true;

                    if (!fetchResult.Success)
                    {
                        Interlocked.Increment(ref failedPages);

                        if (!string.IsNullOrWhiteSpace(fetchResult.FailureReason))
                        {
                            onLog?.Invoke($"Skipped {currentUrl.AbsoluteUri}: {fetchResult.FailureReason}");
                        }

                        onProgress?.Invoke(new CrawlProgress(
                            ProcessedPages: Volatile.Read(ref processedPages),
                            FailedPages: Volatile.Read(ref failedPages),
                            QueueLength: frontier.Count,
                            UniqueEmailCount: uniqueEmails.Count,
                            TotalEmailHits: Volatile.Read(ref totalEmailHits),
                            CurrentUrl: currentUrl.AbsoluteUri));

                        continue;
                    }

                    var pageEmails = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var email in ExtractEmails(fetchResult.Html))
                    {
                        if (!pageEmails.Add(email))
                        {
                            continue;
                        }

                        Interlocked.Increment(ref totalEmailHits);
                        uniqueEmails.TryAdd(email, 0);

                        onEmailFound?.Invoke(new EmailMatch(email, currentUrl.AbsoluteUri));
                    }

                    var queuedFromPage = 0;
                    foreach (var discoveredLink in ExtractLinks(fetchResult.Html, currentUrl))
                    {
                        if (!ShouldQueue(discoveredLink, allowedHosts, options.SameHostOnly))
                        {
                            continue;
                        }

                        if (queuedFromPage >= options.MaxLinksPerPage)
                        {
                            break;
                        }

                        var normalizedLink = NormalizeUri(discoveredLink);
                        if (!seenUrlKeys.TryAdd(normalizedLink, 0))
                        {
                            continue;
                        }

                        frontier.Enqueue(discoveredLink);
                        queuedFromPage++;
                    }

                    onProgress?.Invoke(new CrawlProgress(
                        ProcessedPages: Volatile.Read(ref processedPages),
                        FailedPages: Volatile.Read(ref failedPages),
                        QueueLength: frontier.Count,
                        UniqueEmailCount: uniqueEmails.Count,
                        TotalEmailHits: Volatile.Read(ref totalEmailHits),
                        CurrentUrl: currentUrl.AbsoluteUri));
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref failedPages);
                    if (!countedPage)
                    {
                        Interlocked.Increment(ref processedPages);
                    }
                    onLog?.Invoke($"Error on {currentUrl.AbsoluteUri}: {ex.Message}");

                    onProgress?.Invoke(new CrawlProgress(
                        ProcessedPages: Volatile.Read(ref processedPages),
                        FailedPages: Volatile.Read(ref failedPages),
                        QueueLength: frontier.Count,
                        UniqueEmailCount: uniqueEmails.Count,
                        TotalEmailHits: Volatile.Read(ref totalEmailHits),
                        CurrentUrl: currentUrl.AbsoluteUri));
                }
                finally
                {
                    Interlocked.Decrement(ref activeWorkers);
                }
            }
        }

        var workerCount = Math.Max(1, options.MaxConcurrency);
        var workers = new Task[workerCount];
        for (var i = 0; i < workerCount; i++)
        {
            workers[i] = WorkerAsync();
        }

        await Task.WhenAll(workers).ConfigureAwait(false);

        sw.Stop();

        return new CrawlSummary(
            ProcessedPages: Volatile.Read(ref processedPages),
            FailedPages: Volatile.Read(ref failedPages),
            UniqueEmailCount: uniqueEmails.Count,
            TotalEmailHits: Volatile.Read(ref totalEmailHits),
            Duration: sw.Elapsed);
    }

    private async Task<FetchResult> TryFetchHtmlAsync(Uri url, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            return new FetchResult(false, string.Empty, $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}");
        }

        var mediaType = response.Content.Headers.ContentType?.MediaType;
        if (!string.IsNullOrWhiteSpace(mediaType) && mediaType.IndexOf("html", StringComparison.OrdinalIgnoreCase) < 0)
        {
            return new FetchResult(false, string.Empty, $"Content type '{mediaType}' is not HTML");
        }

        var html = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        return new FetchResult(true, html, null);
    }

    private static IEnumerable<string> ExtractEmails(string html)
    {
        var decoded = WebUtility.HtmlDecode(html);
        var matches = EmailRegex.Matches(decoded);
        foreach (Match match in matches)
        {
            var candidate = match.Value.Trim().TrimEnd('.', ';', ',', ':', ')', ']', '>');
            if (!IsValidEmail(candidate))
            {
                continue;
            }

            yield return candidate.ToLowerInvariant();
        }
    }

    private static IEnumerable<Uri> ExtractLinks(string html, Uri baseUri)
    {
        var matches = HrefRegex.Matches(html);
        foreach (Match match in matches)
        {
            var rawUrl = WebUtility.HtmlDecode(match.Groups["url"].Value).Trim();
            if (string.IsNullOrWhiteSpace(rawUrl))
            {
                continue;
            }

            if (rawUrl.StartsWith("#", StringComparison.Ordinal) ||
                rawUrl.StartsWith("mailto:", StringComparison.OrdinalIgnoreCase) ||
                rawUrl.StartsWith("tel:", StringComparison.OrdinalIgnoreCase) ||
                rawUrl.StartsWith("javascript:", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!Uri.TryCreate(baseUri, rawUrl, out var resolved))
            {
                continue;
            }

            if (!string.Equals(resolved.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(resolved.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!string.IsNullOrEmpty(resolved.Fragment))
            {
                var builder = new UriBuilder(resolved)
                {
                    Fragment = string.Empty
                };

                resolved = builder.Uri;
            }

            yield return resolved;
        }
    }

    private static bool ShouldQueue(Uri uri, HashSet<string> allowedHosts, bool sameHostOnly)
    {
        if (sameHostOnly && !allowedHosts.Contains(uri.Host))
        {
            return false;
        }

        var extension = Path.GetExtension(uri.AbsolutePath);
        if (!string.IsNullOrWhiteSpace(extension) && NonHtmlExtensions.Contains(extension))
        {
            return false;
        }

        return true;
    }

    private static bool IsValidEmail(string email)
    {
        if (email.Length > 254 || email.Contains("..", StringComparison.Ordinal))
        {
            return false;
        }

        try
        {
            var parsed = new MailAddress(email);
            return string.Equals(parsed.Address, email, StringComparison.OrdinalIgnoreCase);
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static string NormalizeUri(Uri uri)
    {
        var builder = new UriBuilder(uri)
        {
            Fragment = string.Empty
        };

        if (builder.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) && builder.Port == 80)
        {
            builder.Port = -1;
        }
        else if (builder.Scheme.Equals(Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase) && builder.Port == 443)
        {
            builder.Port = -1;
        }

        var normalized = builder.Uri.AbsoluteUri;
        if (normalized.EndsWith("/", StringComparison.Ordinal) && builder.Path.Length > 1)
        {
            normalized = normalized.TrimEnd('/');
        }

        return normalized;
    }

    public void Dispose()
    {
        _httpClient.Dispose();
    }

    private readonly record struct FetchResult(bool Success, string Html, string? FailureReason);
}

internal sealed class CrawlOptions
{
    public int MaxPages { get; init; } = 250;

    public int MaxConcurrency { get; init; } = 6;

    public bool SameHostOnly { get; init; } = true;

    public int MaxLinksPerPage { get; init; } = 200;

    public int MaxDepth { get; init; } = 4;

    public int DownloadDelayMs { get; init; } = 0;

    public bool EnableTheHarvester { get; init; }

    public IReadOnlyList<string> TheHarvesterSources { get; init; } = Array.Empty<string>();

    public int TheHarvesterSourceTimeoutSeconds { get; init; } = 30;

    public int TheHarvesterSeedCap { get; init; } = 250;
}

internal readonly record struct CrawlProgress(
    int ProcessedPages,
    int FailedPages,
    int QueueLength,
    int UniqueEmailCount,
    int TotalEmailHits,
    string CurrentUrl);

internal readonly record struct EmailMatch(string Email, string SourceUrl);

internal readonly record struct CrawlSummary(
    int ProcessedPages,
    int FailedPages,
    int UniqueEmailCount,
    int TotalEmailHits,
    TimeSpan Duration);
