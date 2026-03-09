using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Email_Scrape;

internal interface IEmailCrawler : IDisposable
{
    Task<CrawlSummary> CrawlAsync(
        IReadOnlyList<Uri> seedUris,
        CrawlOptions options,
        Action<CrawlProgress>? onProgress,
        Action<EmailMatch>? onEmailFound,
        Action<string>? onLog,
        CancellationToken cancellationToken);
}
