using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace Email_Scrape;

public partial class Form1 : Form
{
    private readonly TextBox _seedInput = new();
    private readonly NumericUpDown _maxPagesInput = new();
    private readonly NumericUpDown _concurrencyInput = new();
    private readonly NumericUpDown _timeoutInput = new();
    private readonly NumericUpDown _maxDepthInput = new();
    private readonly NumericUpDown _downloadDelayInput = new();
    private readonly CheckBox _sameHostOnlyCheckBox = new();
    private readonly CheckBox _useScrapyEngineCheckBox = new();
    private readonly CheckBox _useTheHarvesterCheckBox = new();
    private readonly TextBox _theHarvesterSourcesInput = new();

    private readonly Button _startButton = new();
    private readonly Button _cancelButton = new();
    private readonly Button _clearButton = new();
    private readonly Button _exportButton = new();

    private readonly DataGridView _resultsGrid = new();
    private readonly TextBox _logOutput = new();

    private readonly StatusStrip _statusStrip = new();
    private readonly ToolStripStatusLabel _statusLabel = new();
    private readonly ToolStripProgressBar _progressBar = new();

    private readonly BindingList<EmailResultRow> _emailRows = new();
    private readonly Dictionary<string, EmailResultRow> _emailLookup = new(StringComparer.OrdinalIgnoreCase);
    private readonly string _pythonExecutable = "python";
    private bool _scrapyEngineAvailable;

    private CancellationTokenSource? _runCts;
    private bool _isRunning;

    public Form1()
    {
        InitializeComponent();
        BuildInterface();
        WireEvents();
        InitializeScrapyEngineSelection();
        UpdateControlStates();
    }

    private void BuildInterface()
    {
        SuspendLayout();

        Text = "Email Scrape";
        StartPosition = FormStartPosition.CenterScreen;
        MinimumSize = new Size(1000, 700);

        var mainLayout = new TableLayoutPanel
        {
            Dock = DockStyle.Fill,
            ColumnCount = 1,
            RowCount = 3,
            Padding = new Padding(10)
        };

        mainLayout.RowStyles.Add(new RowStyle(SizeType.Absolute, 190));
        mainLayout.RowStyles.Add(new RowStyle(SizeType.AutoSize));
        mainLayout.RowStyles.Add(new RowStyle(SizeType.Percent, 100f));

        var seedGroup = new GroupBox
        {
            Text = "Seed URLs (one per line)",
            Dock = DockStyle.Fill
        };

        _seedInput.Multiline = true;
        _seedInput.AcceptsReturn = true;
        _seedInput.AcceptsTab = true;
        _seedInput.ScrollBars = ScrollBars.Vertical;
        _seedInput.WordWrap = false;
        _seedInput.Dock = DockStyle.Fill;
        _seedInput.Font = new Font("Consolas", 9.5f, FontStyle.Regular, GraphicsUnit.Point);

        seedGroup.Controls.Add(_seedInput);

        _maxPagesInput.Minimum = 1;
        _maxPagesInput.Maximum = 50_000;
        _maxPagesInput.Value = 250;
        _maxPagesInput.Width = 90;

        _concurrencyInput.Minimum = 1;
        _concurrencyInput.Maximum = 64;
        _concurrencyInput.Value = 6;
        _concurrencyInput.Width = 70;

        _timeoutInput.Minimum = 5;
        _timeoutInput.Maximum = 300;
        _timeoutInput.Value = 15;
        _timeoutInput.Width = 70;

        _maxDepthInput.Minimum = 1;
        _maxDepthInput.Maximum = 20;
        _maxDepthInput.Value = 4;
        _maxDepthInput.Width = 70;

        _downloadDelayInput.Minimum = 0;
        _downloadDelayInput.Maximum = 10000;
        _downloadDelayInput.Value = 250;
        _downloadDelayInput.Increment = 50;
        _downloadDelayInput.Width = 80;

        _sameHostOnlyCheckBox.Text = "Stay on seed hosts only";
        _sameHostOnlyCheckBox.AutoSize = true;
        _sameHostOnlyCheckBox.Checked = true;
        _sameHostOnlyCheckBox.Margin = new Padding(0, 8, 16, 0);

        _useScrapyEngineCheckBox.Text = "Use Scrapy engine";
        _useScrapyEngineCheckBox.AutoSize = true;
        _useScrapyEngineCheckBox.Checked = true;
        _useScrapyEngineCheckBox.Margin = new Padding(0, 8, 16, 0);

        _useTheHarvesterCheckBox.Text = "Enable theHarvester enrichment";
        _useTheHarvesterCheckBox.AutoSize = true;
        _useTheHarvesterCheckBox.Checked = true;
        _useTheHarvesterCheckBox.Margin = new Padding(0, 8, 16, 0);

        _theHarvesterSourcesInput.Width = 320;
        _theHarvesterSourcesInput.Text = "crtsh,rapiddns,waybackarchive,commoncrawl,thc,duckduckgo";

        _startButton.Text = "Start Crawl";
        _startButton.AutoSize = true;
        _startButton.Margin = new Padding(0, 4, 8, 4);

        _cancelButton.Text = "Cancel";
        _cancelButton.AutoSize = true;
        _cancelButton.Margin = new Padding(0, 4, 8, 4);

        _clearButton.Text = "Clear Results";
        _clearButton.AutoSize = true;
        _clearButton.Margin = new Padding(0, 4, 8, 4);

        _exportButton.Text = "Export CSV";
        _exportButton.AutoSize = true;
        _exportButton.Margin = new Padding(0, 4, 8, 4);

        var optionsPanel = new FlowLayoutPanel
        {
            Dock = DockStyle.Fill,
            AutoSize = true,
            AutoSizeMode = AutoSizeMode.GrowAndShrink,
            WrapContents = true,
            FlowDirection = FlowDirection.LeftToRight,
            Margin = new Padding(0, 8, 0, 8),
            Padding = new Padding(0)
        };

        optionsPanel.Controls.Add(CreateLabeledControl("Max pages:", _maxPagesInput));
        optionsPanel.Controls.Add(CreateLabeledControl("Threads:", _concurrencyInput));
        optionsPanel.Controls.Add(CreateLabeledControl("Timeout (s):", _timeoutInput));
        optionsPanel.Controls.Add(CreateLabeledControl("Depth:", _maxDepthInput));
        optionsPanel.Controls.Add(CreateLabeledControl("Delay (ms):", _downloadDelayInput));
        optionsPanel.Controls.Add(_useScrapyEngineCheckBox);
        optionsPanel.Controls.Add(_useTheHarvesterCheckBox);
        optionsPanel.Controls.Add(CreateLabeledControl("Harvester sources:", _theHarvesterSourcesInput));
        optionsPanel.Controls.Add(_sameHostOnlyCheckBox);
        optionsPanel.Controls.Add(_startButton);
        optionsPanel.Controls.Add(_cancelButton);
        optionsPanel.Controls.Add(_clearButton);
        optionsPanel.Controls.Add(_exportButton);

        ConfigureResultsGrid();

        var splitContainer = new SplitContainer
        {
            Dock = DockStyle.Fill,
            Orientation = Orientation.Horizontal,
            SplitterDistance = 360
        };

        var resultsGroup = new GroupBox
        {
            Text = "Discovered Emails",
            Dock = DockStyle.Fill
        };
        resultsGroup.Controls.Add(_resultsGrid);

        _logOutput.Multiline = true;
        _logOutput.ReadOnly = true;
        _logOutput.ScrollBars = ScrollBars.Vertical;
        _logOutput.Dock = DockStyle.Fill;
        _logOutput.Font = new Font("Consolas", 9f, FontStyle.Regular, GraphicsUnit.Point);

        var logGroup = new GroupBox
        {
            Text = "Activity Log",
            Dock = DockStyle.Fill
        };
        logGroup.Controls.Add(_logOutput);

        splitContainer.Panel1.Controls.Add(resultsGroup);
        splitContainer.Panel2.Controls.Add(logGroup);

        mainLayout.Controls.Add(seedGroup, 0, 0);
        mainLayout.Controls.Add(optionsPanel, 0, 1);
        mainLayout.Controls.Add(splitContainer, 0, 2);

        _statusLabel.Text = "Ready.";
        _progressBar.Minimum = 0;
        _progressBar.Maximum = 100;
        _progressBar.Value = 0;
        _progressBar.AutoSize = false;
        _progressBar.Width = 220;

        _statusStrip.SizingGrip = false;
        _statusStrip.Items.Add(_statusLabel);
        _statusStrip.Items.Add(new ToolStripStatusLabel { Spring = true });
        _statusStrip.Items.Add(_progressBar);

        Controls.Add(mainLayout);
        Controls.Add(_statusStrip);

        ResumeLayout(performLayout: true);
    }

    private void ConfigureResultsGrid()
    {
        _resultsGrid.Dock = DockStyle.Fill;
        _resultsGrid.ReadOnly = true;
        _resultsGrid.AllowUserToAddRows = false;
        _resultsGrid.AllowUserToDeleteRows = false;
        _resultsGrid.AllowUserToResizeRows = false;
        _resultsGrid.MultiSelect = false;
        _resultsGrid.SelectionMode = DataGridViewSelectionMode.FullRowSelect;
        _resultsGrid.AutoGenerateColumns = false;

        _resultsGrid.Columns.Add(new DataGridViewTextBoxColumn
        {
            DataPropertyName = nameof(EmailResultRow.Email),
            HeaderText = "Email",
            FillWeight = 35f,
            AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill
        });

        _resultsGrid.Columns.Add(new DataGridViewTextBoxColumn
        {
            DataPropertyName = nameof(EmailResultRow.Occurrences),
            HeaderText = "Hits",
            Width = 70,
            AutoSizeMode = DataGridViewAutoSizeColumnMode.None
        });

        _resultsGrid.Columns.Add(new DataGridViewTextBoxColumn
        {
            DataPropertyName = nameof(EmailResultRow.FirstSeenLocal),
            HeaderText = "First Seen",
            Width = 145,
            DefaultCellStyle = new DataGridViewCellStyle { Format = "yyyy-MM-dd HH:mm:ss" },
            AutoSizeMode = DataGridViewAutoSizeColumnMode.None
        });

        _resultsGrid.Columns.Add(new DataGridViewTextBoxColumn
        {
            DataPropertyName = nameof(EmailResultRow.LastSeenLocal),
            HeaderText = "Last Seen",
            Width = 145,
            DefaultCellStyle = new DataGridViewCellStyle { Format = "yyyy-MM-dd HH:mm:ss" },
            AutoSizeMode = DataGridViewAutoSizeColumnMode.None
        });

        _resultsGrid.Columns.Add(new DataGridViewTextBoxColumn
        {
            DataPropertyName = nameof(EmailResultRow.SourceUrl),
            HeaderText = "Source URL",
            FillWeight = 65f,
            AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill
        });

        _resultsGrid.DataSource = _emailRows;
    }

    private static Control CreateLabeledControl(string labelText, Control control)
    {
        var panel = new FlowLayoutPanel
        {
            AutoSize = true,
            WrapContents = false,
            FlowDirection = FlowDirection.LeftToRight,
            Margin = new Padding(0, 4, 16, 4),
            Padding = new Padding(0)
        };

        var label = new Label
        {
            Text = labelText,
            AutoSize = true,
            Margin = new Padding(0, 7, 6, 0)
        };

        control.Margin = new Padding(0);

        panel.Controls.Add(label);
        panel.Controls.Add(control);
        return panel;
    }

    private void WireEvents()
    {
        _startButton.Click += StartButton_Click;
        _cancelButton.Click += CancelButton_Click;
        _clearButton.Click += ClearButton_Click;
        _exportButton.Click += ExportButton_Click;
        _useScrapyEngineCheckBox.CheckedChanged += ScrapyEngineCheckBox_CheckedChanged;
        _useTheHarvesterCheckBox.CheckedChanged += HarvesterCheckBox_CheckedChanged;
        FormClosing += Form1_FormClosing;
    }

    private void ScrapyEngineCheckBox_CheckedChanged(object? sender, EventArgs e)
    {
        if (!_useScrapyEngineCheckBox.Checked)
        {
            _useTheHarvesterCheckBox.Checked = false;
        }

        UpdateControlStates();
    }

    private void HarvesterCheckBox_CheckedChanged(object? sender, EventArgs e)
    {
        UpdateControlStates();
    }

    private void InitializeScrapyEngineSelection()
    {
        _scrapyEngineAvailable = ScrapyEmailCrawler.IsScrapyAvailable(_pythonExecutable, out var detail);
        if (!_scrapyEngineAvailable)
        {
            _useScrapyEngineCheckBox.Checked = false;
            _useScrapyEngineCheckBox.Enabled = false;
            _useTheHarvesterCheckBox.Checked = false;
            _useTheHarvesterCheckBox.Enabled = false;
            _theHarvesterSourcesInput.Enabled = false;
            AddLog($"Scrapy engine unavailable ({detail}). Native crawler will be used.");
            return;
        }

        _useScrapyEngineCheckBox.Enabled = true;
        _useTheHarvesterCheckBox.Enabled = true;
        AddLog($"Scrapy engine ready (version: {detail}).");
    }

    private IEmailCrawler CreateCrawlerEngine(TimeSpan timeout, out string engineName)
    {
        if (_useScrapyEngineCheckBox.Checked)
        {
            engineName = "Scrapy";
            return new ScrapyEmailCrawler(_pythonExecutable, timeout);
        }

        engineName = "Native";
        return new EmailCrawler(timeout);
    }

    private async void StartButton_Click(object? sender, EventArgs e)
    {
        if (_isRunning)
        {
            return;
        }

        List<Uri> seedUris;
        try
        {
            seedUris = ParseSeedUris();
        }
        catch (FormatException ex)
        {
            MessageBox.Show(this, ex.Message, "Invalid URL input", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            return;
        }

        if (seedUris.Count == 0)
        {
            MessageBox.Show(this, "Enter at least one URL to crawl.", "Missing input", MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        _emailRows.Clear();
        _emailLookup.Clear();
        _logOutput.Clear();

        _isRunning = true;
        _runCts = new CancellationTokenSource();
        UpdateControlStates();

        var options = new CrawlOptions
        {
            MaxPages = (int)_maxPagesInput.Value,
            MaxConcurrency = (int)_concurrencyInput.Value,
            SameHostOnly = _sameHostOnlyCheckBox.Checked,
            MaxLinksPerPage = 200,
            MaxDepth = (int)_maxDepthInput.Value,
            DownloadDelayMs = (int)_downloadDelayInput.Value,
            EnableTheHarvester = _useScrapyEngineCheckBox.Checked && _useTheHarvesterCheckBox.Checked,
            TheHarvesterSources = ParseTheHarvesterSources(_theHarvesterSourcesInput.Text),
            TheHarvesterSourceTimeoutSeconds = 35,
            TheHarvesterSeedCap = 350
        };

        _progressBar.Minimum = 0;
        _progressBar.Maximum = Math.Max(1, options.MaxPages);
        _progressBar.Value = 0;
        _statusLabel.Text = "Running...";

        try
        {
            var timeout = TimeSpan.FromSeconds((double)_timeoutInput.Value);
            IEmailCrawler crawler;
            string engineName;

            try
            {
                crawler = CreateCrawlerEngine(timeout, out engineName);
            }
            catch (Exception ex) when (_useScrapyEngineCheckBox.Checked)
            {
                AddLog($"Scrapy engine initialization failed ({ex.Message}). Falling back to native engine.");
                crawler = new EmailCrawler(timeout);
                engineName = "Native (fallback)";
            }

            using (crawler)
            {
                AddLog(
                    $"Started crawl with {seedUris.Count} seed URL(s). Engine: {engineName}. Max pages: {options.MaxPages}, Threads: {options.MaxConcurrency}, Timeout: {_timeoutInput.Value}s, Depth: {options.MaxDepth}, Delay: {options.DownloadDelayMs}ms, theHarvester: {(options.EnableTheHarvester ? "on" : "off")}.");

                var summary = await crawler.CrawlAsync(
                    seedUris,
                    options,
                    progress => RunOnUiThread(() => ApplyProgress(progress, options.MaxPages)),
                    email => RunOnUiThread(() => AddOrUpdateEmail(email)),
                    message => RunOnUiThread(() => AddLog(message)),
                    _runCts.Token);

                RunOnUiThread(() =>
                {
                    AddLog($"Completed in {summary.Duration.TotalSeconds:F1}s. Pages: {summary.ProcessedPages}, Failures: {summary.FailedPages}, Unique emails: {summary.UniqueEmailCount}, Total hits: {summary.TotalEmailHits}.");
                    _statusLabel.Text = $"Completed. {summary.UniqueEmailCount} unique email(s) from {summary.ProcessedPages} page(s).";
                    _progressBar.Value = Math.Min(summary.ProcessedPages, _progressBar.Maximum);
                });
            }
        }
        catch (OperationCanceledException)
        {
            RunOnUiThread(() =>
            {
                AddLog("Operation canceled by user.");
                _statusLabel.Text = "Canceled.";
            });
        }
        catch (Exception ex)
        {
            RunOnUiThread(() =>
            {
                AddLog($"Unhandled error: {ex.Message}");
                _statusLabel.Text = "Failed.";
                MessageBox.Show(this, ex.Message, "Crawl error", MessageBoxButtons.OK, MessageBoxIcon.Error);
            });
        }
        finally
        {
            _runCts?.Dispose();
            _runCts = null;
            _isRunning = false;
            RunOnUiThread(UpdateControlStates);
        }
    }

    private void CancelButton_Click(object? sender, EventArgs e)
    {
        if (_isRunning)
        {
            _runCts?.Cancel();
        }
    }

    private void ClearButton_Click(object? sender, EventArgs e)
    {
        if (_isRunning)
        {
            return;
        }

        _emailRows.Clear();
        _emailLookup.Clear();
        _logOutput.Clear();
        _statusLabel.Text = "Ready.";
        _progressBar.Value = 0;
        UpdateControlStates();
    }

    private void ExportButton_Click(object? sender, EventArgs e)
    {
        if (_emailRows.Count == 0)
        {
            MessageBox.Show(this, "No emails available to export.", "Export", MessageBoxButtons.OK, MessageBoxIcon.Information);
            return;
        }

        using var saveDialog = new SaveFileDialog
        {
            Title = "Export Results",
            Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
            FileName = $"emails-{DateTime.Now:yyyyMMdd-HHmmss}.csv"
        };

        if (saveDialog.ShowDialog(this) != DialogResult.OK)
        {
            return;
        }

        try
        {
            using var writer = new StreamWriter(saveDialog.FileName, false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));
            writer.WriteLine("Email,Hits,FirstSeen,LastSeen,SourceUrl");

            foreach (var row in _emailRows.OrderBy(r => r.Email, StringComparer.OrdinalIgnoreCase))
            {
                writer.WriteLine(string.Join(",",
                    EscapeCsv(row.Email),
                    row.Occurrences,
                    EscapeCsv(row.FirstSeenLocal.ToString("yyyy-MM-dd HH:mm:ss")),
                    EscapeCsv(row.LastSeenLocal.ToString("yyyy-MM-dd HH:mm:ss")),
                    EscapeCsv(row.SourceUrl)));
            }

            AddLog($"Exported {_emailRows.Count} row(s) to '{saveDialog.FileName}'.");
            _statusLabel.Text = "Export complete.";
        }
        catch (Exception ex)
        {
            MessageBox.Show(this, ex.Message, "Export failed", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
    }

    private void Form1_FormClosing(object? sender, FormClosingEventArgs e)
    {
        _runCts?.Cancel();
    }

    private List<Uri> ParseSeedUris()
    {
        var validUris = new List<Uri>();
        var invalidLines = new List<string>();

        foreach (var rawLine in _seedInput.Lines)
        {
            var value = rawLine.Trim();
            if (string.IsNullOrWhiteSpace(value))
            {
                continue;
            }

            if (!value.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
                !value.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                value = "https://" + value;
            }

            if (Uri.TryCreate(value, UriKind.Absolute, out var parsed) &&
                (string.Equals(parsed.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(parsed.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
            {
                validUris.Add(parsed);
            }
            else
            {
                invalidLines.Add(rawLine.Trim());
            }
        }

        if (invalidLines.Count > 0)
        {
            var sample = string.Join(", ", invalidLines.Take(3));
            var suffix = invalidLines.Count > 3 ? " ..." : string.Empty;
            throw new FormatException($"Invalid URL format: {sample}{suffix}");
        }

        return validUris
            .GroupBy(uri => uri.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();
    }

    private static string[] ParseTheHarvesterSources(string rawSources)
    {
        if (string.IsNullOrWhiteSpace(rawSources))
        {
            return Array.Empty<string>();
        }

        return rawSources
            .Split(new[] { ',', ';', '\n', '\r', '\t', ' ' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(value => value.Trim().ToLowerInvariant())
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private void ApplyProgress(CrawlProgress progress, int maxPages)
    {
        _progressBar.Maximum = Math.Max(1, maxPages);
        _progressBar.Value = Math.Min(progress.ProcessedPages, _progressBar.Maximum);

        _statusLabel.Text =
            $"Pages {progress.ProcessedPages}/{maxPages} | Queue {progress.QueueLength} | Unique emails {progress.UniqueEmailCount}";
    }

    private void AddOrUpdateEmail(EmailMatch match)
    {
        if (_emailLookup.TryGetValue(match.Email, out var existingRow))
        {
            existingRow.Occurrences += 1;
            existingRow.LastSeenLocal = DateTime.Now;
            return;
        }

        var now = DateTime.Now;
        var row = new EmailResultRow
        {
            Email = match.Email,
            Occurrences = 1,
            FirstSeenLocal = now,
            LastSeenLocal = now,
            SourceUrl = match.SourceUrl
        };

        _emailLookup.Add(match.Email, row);
        _emailRows.Add(row);
    }

    private void AddLog(string message)
    {
        var line = $"{DateTime.Now:HH:mm:ss}  {message}";
        _logOutput.AppendText(line + Environment.NewLine);

        var lines = _logOutput.Lines;
        if (lines.Length <= 1200)
        {
            return;
        }

        _logOutput.Lines = lines.Skip(lines.Length - 1000).ToArray();
        _logOutput.SelectionStart = _logOutput.TextLength;
        _logOutput.ScrollToCaret();
    }

    private void UpdateControlStates()
    {
        _startButton.Enabled = !_isRunning;
        _cancelButton.Enabled = _isRunning;

        _clearButton.Enabled = !_isRunning;
        _exportButton.Enabled = !_isRunning && _emailRows.Count > 0;

        _seedInput.Enabled = !_isRunning;
        _maxPagesInput.Enabled = !_isRunning;
        _concurrencyInput.Enabled = !_isRunning;
        _timeoutInput.Enabled = !_isRunning;
        _maxDepthInput.Enabled = !_isRunning;
        _downloadDelayInput.Enabled = !_isRunning;
        _sameHostOnlyCheckBox.Enabled = !_isRunning;

        var scrapyToggleEnabled = !_isRunning && _scrapyEngineAvailable;
        _useScrapyEngineCheckBox.Enabled = scrapyToggleEnabled;

        var harvesterToggleEnabled = !_isRunning && _scrapyEngineAvailable && _useScrapyEngineCheckBox.Checked;
        _useTheHarvesterCheckBox.Enabled = harvesterToggleEnabled;
        _theHarvesterSourcesInput.Enabled = harvesterToggleEnabled && _useTheHarvesterCheckBox.Checked;
    }

    private void RunOnUiThread(Action action)
    {
        if (IsDisposed || Disposing)
        {
            return;
        }

        if (!InvokeRequired)
        {
            action();
            return;
        }

        try
        {
            BeginInvoke(action);
        }
        catch (InvalidOperationException)
        {
            // The form is closing while the background worker is still unwinding.
        }
    }

    private static string EscapeCsv(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        if (!value.Contains('"') && !value.Contains(',') && !value.Contains('\n') && !value.Contains('\r'))
        {
            return value;
        }

        return $"\"{value.Replace("\"", "\"\"")}\"";
    }

    private sealed class EmailResultRow : INotifyPropertyChanged
    {
        private int _occurrences;
        private DateTime _lastSeenLocal;
        private string _sourceUrl = string.Empty;

        public string Email { get; init; } = string.Empty;

        public int Occurrences
        {
            get => _occurrences;
            set
            {
                if (_occurrences == value)
                {
                    return;
                }

                _occurrences = value;
                OnPropertyChanged(nameof(Occurrences));
            }
        }

        public DateTime FirstSeenLocal { get; init; }

        public DateTime LastSeenLocal
        {
            get => _lastSeenLocal;
            set
            {
                if (_lastSeenLocal == value)
                {
                    return;
                }

                _lastSeenLocal = value;
                OnPropertyChanged(nameof(LastSeenLocal));
            }
        }

        public string SourceUrl
        {
            get => _sourceUrl;
            set
            {
                if (string.Equals(_sourceUrl, value, StringComparison.Ordinal))
                {
                    return;
                }

                _sourceUrl = value;
                OnPropertyChanged(nameof(SourceUrl));
            }
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        private void OnPropertyChanged(string propertyName)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}
