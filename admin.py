def get_admin_html() -> str:
    return r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MUZO // SYSTEM OPERATIONAL TERMINAL & CLEANUP HUB</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #090c10;
      --panel: #0d1117;
      --panel-header: #161b22;
      --panel-border: #21262d;
      --text: #c9d1d9;
      --text-muted: #8b949e;
      --text-bright: #f0f6fc;
      
      --green: #238636;
      --green-bright: #3fb950;
      --green-glow: rgba(63, 185, 80, 0.15);
      
      --cyan: #38bdf8;
      --cyan-soft: rgba(56, 189, 248, 0.12);
      
      --amber: #d29922;
      --amber-bright: #e3b341;
      --amber-soft: rgba(210, 153, 34, 0.15);
      
      --red: #f85149;
      --red-soft: rgba(248, 81, 73, 0.15);
      
      --font-mono: 'JetBrains Mono', monospace;
      --font-sans: 'Inter', system-ui, -apple-system, sans-serif;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background-color: var(--bg);
      color: var(--text);
      font-family: var(--font-mono);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      padding: 1rem;
      font-size: 13px;
      line-height: 1.5;
    }

    /* Terminal Header Bar */
    .term-header {
      background: var(--panel-header);
      border: 1px solid var(--panel-border);
      border-radius: 6px 6px 0 0;
      padding: 0.6rem 1rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    
    .term-title-group {
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }

    .term-dots { display: flex; gap: 6px; }
    .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
    .dot-red { background: #ff5f56; }
    .dot-yellow { background: #ffbd2e; }
    .dot-green { background: #27c93f; }

    .term-title {
      font-weight: 700;
      color: var(--text-bright);
      letter-spacing: 0.5px;
      font-size: 0.85rem;
    }

    .status-badge {
      background: var(--green-glow);
      color: var(--green-bright);
      border: 1px solid rgba(63, 185, 80, 0.3);
      padding: 0.2rem 0.6rem;
      border-radius: 4px;
      font-size: 0.75rem;
      font-weight: 500;
      display: flex;
      align-items: center;
      gap: 0.4rem;
    }

    .pulse {
      width: 6px;
      height: 6px;
      background: currentColor;
      border-radius: 50%;
      box-shadow: 0 0 8px currentColor;
      animation: blink 1.5s infinite;
    }
    @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }

    /* Layout Main Wrapper */
    .term-body {
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-top: none;
      border-radius: 0 0 6px 6px;
      padding: 1rem;
      flex-grow: 1;
      display: flex;
      flex-direction: column;
      gap: 1rem;
    }

    /* Metric Strip */
    .metrics-row {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 0.8rem;
    }

    .metric-card {
      background: var(--bg);
      border: 1px solid var(--panel-border);
      border-radius: 4px;
      padding: 0.8rem;
      position: relative;
    }

    .metric-label {
      font-size: 0.75rem;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.5px;
      margin-bottom: 0.4rem;
    }

    .metric-val {
      font-size: 1.5rem;
      font-weight: 700;
      color: var(--text-bright);
      font-family: var(--font-mono);
    }
    .metric-val-green { color: var(--green-bright); }
    .metric-val-cyan { color: var(--cyan); }
    .metric-val-amber { color: var(--amber-bright); }
    .metric-val-red { color: var(--red); }

    /* Progress Container */
    .progress-box {
      background: var(--bg);
      border: 1px solid var(--panel-border);
      border-radius: 4px;
      padding: 0.8rem 1rem;
    }

    .progress-header {
      display: flex;
      justify-content: space-between;
      margin-bottom: 0.4rem;
      font-weight: 600;
    }

    .progress-track {
      background: #161b22;
      border: 1px solid var(--panel-border);
      height: 10px;
      border-radius: 2px;
      overflow: hidden;
    }

    .progress-fill {
      height: 100%;
      background: linear-gradient(90deg, var(--green-bright), var(--cyan));
      width: 0%;
      transition: width 0.3s ease;
    }

    /* Actions Strip */
    .action-bar {
      display: flex;
      gap: 0.8rem;
      align-items: center;
      flex-wrap: wrap;
    }

    .btn {
      background: var(--panel-header);
      color: var(--text-bright);
      border: 1px solid var(--panel-border);
      padding: 0.45rem 0.9rem;
      border-radius: 4px;
      font-family: var(--font-mono);
      font-size: 0.78rem;
      font-weight: 500;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      transition: all 0.2s;
    }
    .btn:hover { background: #21262d; border-color: #30363d; }
    .btn-green { background: var(--green); border-color: var(--green-bright); color: #ffffff; }
    .btn-green:hover { background: #2ea043; }
    .btn-cyan { background: rgba(56, 189, 248, 0.15); border-color: var(--cyan); color: var(--cyan); }
    .btn-cyan:hover { background: rgba(56, 189, 248, 0.25); }
    .btn-red { background: var(--red-soft); border-color: var(--red); color: var(--red); }
    .btn-red:hover { background: rgba(248, 81, 73, 0.25); }
    .btn-amber { background: var(--amber-soft); border-color: var(--amber-bright); color: var(--amber-bright); }
    .btn-amber:hover { background: rgba(210, 153, 34, 0.3); }

    /* Command Center Sections */
    .two-col {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1rem;
    }
    @media(max-width: 960px) { .two-col { grid-template-columns: 1fr; } }

    .panel-block {
      background: var(--bg);
      border: 1px solid var(--panel-border);
      border-radius: 4px;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    .panel-title-bar {
      background: var(--panel-header);
      border-bottom: 1px solid var(--panel-border);
      padding: 0.5rem 0.8rem;
      font-weight: 600;
      font-size: 0.78rem;
      color: var(--text-muted);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .term-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.78rem;
    }

    .term-table th {
      background: #161b22;
      color: var(--text-muted);
      font-weight: 500;
      text-align: left;
      padding: 0.45rem 0.8rem;
      border-bottom: 1px solid var(--panel-border);
    }

    .term-table td {
      padding: 0.45rem 0.8rem;
      border-bottom: 1px solid #161b22;
      color: var(--text);
    }

    .tag {
      padding: 0.15rem 0.4rem;
      border-radius: 3px;
      font-size: 0.7rem;
      font-weight: 600;
    }
    .tag-invalid { background: var(--red-soft); color: var(--red); border: 1px solid var(--red); }
    .tag-valid { background: var(--green-glow); color: var(--green-bright); border: 1px solid var(--green-bright); }
  </style>
</head>
<body>

  <!-- Terminal Top Bar -->
  <div class="term-header">
    <div class="term-title-group">
      <div class="term-dots">
        <span class="dot dot-red"></span>
        <span class="dot dot-yellow"></span>
        <span class="dot dot-green"></span>
      </div>
      <span class="term-title" id="terminal-header-title">muzo-lossless-worker@cluster-01:~</span>
    </div>
    <div class="status-badge" id="main-status-badge">
      <span class="pulse"></span>
      <span id="main-status-text">SYSTEM OPERATIONAL</span>
    </div>
  </div>

  <div class="term-body">

    <!-- ================= DEFAULT OPERATIONAL VIEW ================= -->
    <div id="default-dashboard-view">
      
      <!-- Metrics Strip -->
      <div class="metrics-row">
        <div class="metric-card">
          <div class="metric-label">// STREAM PROCESSED</div>
          <div class="metric-val metric-val-green"><span id="done-count">0</span> / <span id="total-count">0</span></div>
        </div>
        <div class="metric-card">
          <div class="metric-label">// LOSSLESS AUDIO CACHED</div>
          <div class="metric-val metric-val-amber" id="lossless-count">0</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">// PARALLEL WORKERS</div>
          <div class="metric-val metric-val-cyan">50 Workers</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">// TRANSFER VELOCITY</div>
          <div style="font-size: 0.85rem; margin-top: 0.2rem;">UL Rate: <strong id="upload-rate" style="color: var(--green-bright);">0.0/m</strong></div>
          <div style="font-size: 0.85rem;">NET DL: <strong id="dl-speed" style="color: var(--cyan);">0 KB/s</strong> | UL: <strong id="ul-speed" style="color: var(--text-bright);">0 KB/s</strong></div>
        </div>
      </div>

      <!-- Main Progress Meter -->
      <div class="progress-box" style="margin-top: 1rem;">
        <div class="progress-header">
          <span>OVERALL QUEUE COMPLETION RATIO</span>
          <span id="progress-percent" style="font-weight: 700; color: var(--green-bright);">0.0%</span>
        </div>
        <div class="progress-track">
          <div class="progress-fill" id="progress-fill"></div>
        </div>
      </div>

      <!-- Controls Strip -->
      <div class="action-bar" style="margin-top: 1rem;">
        <button class="btn btn-green" onclick="triggerSync()">► TRIGGER BULK SYNC</button>
        <button class="btn btn-cyan" onclick="triggerLosslessBackup()">⚡ START LOSSLESS BACKUP</button>
        <button class="btn btn-amber" id="reconstruct-btn" onclick="triggerDbReconstruct()">🔄 RECONSTRUCT DB FROM BUCKETS</button>
        <button class="btn btn-red" id="cleanup-trigger-btn" onclick="startCleanup()">🧹 CLEANUP INVALID LOSSLESS (50 WORKERS)</button>
        <div style="margin-left: auto; color: var(--text-muted); font-size: 0.75rem;">
          Queued: <strong id="val-queued" style="color: var(--amber-bright);">0</strong> | Progressing: <strong id="val-progressing" style="color: var(--cyan);">0</strong> | Failed: <strong id="val-failed" style="color: var(--red);">0</strong>
        </div>
      </div>

      <!-- Stream Monitors -->
      <div class="two-col" style="margin-top: 1rem;">
        <div class="panel-block">
          <div class="panel-title-bar">
            <span>ACTIVE STREAM JOBS</span>
            <span style="color: var(--text-muted);">MONITOR #1</span>
          </div>
          <div style="max-height: 240px; overflow-y: auto;">
            <table class="term-table">
              <thead><tr><th>VIDEO ID</th><th>STATUS</th><th>FORMAT</th></tr></thead>
              <tbody id="jobs-tbody"><tr><td colspan="3" style="text-align: center; color: var(--text-muted);">No active jobs running</td></tr></tbody>
            </table>
          </div>
        </div>

        <div class="panel-block">
          <div class="panel-title-bar">
            <span>QOBUZ LOSSLESS RESOLUTION JOBS</span>
            <span style="color: var(--text-muted);">MONITOR #2</span>
          </div>
          <div style="max-height: 240px; overflow-y: auto;">
            <table class="term-table">
              <thead><tr><th>VIDEO ID</th><th>STATUS</th><th>FORMAT</th></tr></thead>
              <tbody id="lossless-jobs-tbody"><tr><td colspan="3" style="text-align: center; color: var(--text-muted);">No active lossless jobs</td></tr></tbody>
            </table>
          </div>
        </div>
      </div>

    </div>

    <!-- ================= ACTIVE CLEANUP GUI VIEW (AUTO-SWITCHES) ================= -->
    <div id="cleanup-active-view" style="display: none;">
      
      <!-- Cleanup Metrics Strip -->
      <div class="metrics-row">
        <div class="metric-card" style="border-color: var(--cyan);">
          <div class="metric-label">// AUDIT PROGRESS</div>
          <div class="metric-val metric-val-cyan"><span id="c-checked">0</span> / <span id="c-total">0</span></div>
        </div>
        <div class="metric-card" style="border-color: var(--green);">
          <div class="metric-label">// VALID LOSSLESS CONFIRMED</div>
          <div class="metric-val metric-val-green" id="c-valid">0</div>
        </div>
        <div class="metric-card" style="border-color: var(--red);">
          <div class="metric-label">// INVALID REMOVED (SET TO 0)</div>
          <div class="metric-val metric-val-red" id="c-invalid">0</div>
        </div>
        <div class="metric-card" style="border-color: var(--amber);">
          <div class="metric-label">// API ERRORS / TIMEOUTS</div>
          <div class="metric-val metric-val-amber" id="c-errors">0</div>
        </div>
      </div>

      <!-- Cleanup Live Progress -->
      <div class="progress-box" style="margin-top: 1rem; border-color: var(--amber);">
        <div class="progress-header">
          <span style="color: var(--amber-bright);">⚡ FULL LOSSLESS AUDIT & CLEANUP RATIO</span>
          <span id="c-pct" style="font-weight: 700; color: var(--green-bright);">0.0%</span>
        </div>
        <div class="progress-track" style="height: 12px;">
          <div class="progress-fill" id="c-progress-fill" style="background: linear-gradient(90deg, var(--amber), var(--red), var(--green-bright)); width: 0%;"></div>
        </div>
        <div style="font-size: 0.72rem; color: var(--text-muted); margin-top: 0.4rem;">
          * Running full audit across all cached tracks using 50 parallel workers. Normal playback streaming remains online.
        </div>
      </div>

      <!-- Clean Live Data Tables (No raw text logs) -->
      <div class="two-col" style="margin-top: 1rem;">
        <!-- Removed Invalid Tracks Table -->
        <div class="panel-block" style="border-color: rgba(248, 81, 73, 0.4);">
          <div class="panel-title-bar" style="color: var(--red);">
            <span>❌ RECENTLY REMOVED INVALID TRACKS</span>
            <span id="c-invalid-count" style="font-size: 0.75rem;">0 removed</span>
          </div>
          <div style="max-height: 280px; overflow-y: auto;">
            <table class="term-table">
              <thead><tr><th>VIDEO ID</th><th>TRACK TITLE & ARTIST</th><th>STATUS</th></tr></thead>
              <tbody id="c-invalid-tbody">
                <tr><td colspan="3" style="text-align: center; color: var(--text-muted); padding: 1.5rem;">No invalid tracks removed yet</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Verified Valid Tracks Table -->
        <div class="panel-block" style="border-color: rgba(63, 185, 80, 0.4);">
          <div class="panel-title-bar" style="color: var(--green-bright);">
            <span>✅ RECENTLY VERIFIED LOSSLESS TRACKS</span>
            <span id="c-valid-count" style="font-size: 0.75rem;">0 verified</span>
          </div>
          <div style="max-height: 280px; overflow-y: auto;">
            <table class="term-table">
              <thead><tr><th>VIDEO ID</th><th>TRACK TITLE & ARTIST</th><th>STATUS</th></tr></thead>
              <tbody id="c-valid-tbody">
                <tr><td colspan="3" style="text-align: center; color: var(--text-muted); padding: 1.5rem;">No tracks audited yet</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>

    </div>

  </div>

  <script>
    async function triggerSync() {
      try {
        const res = await fetch('/api/stream/sync', { method: 'POST' });
        const data = await res.json();
        alert("[SYS_CMD] " + data.message);
      } catch (e) {
        alert("[SYS_ERR] Failed to execute bulk sync command");
      }
    }

    async function triggerLosslessBackup() {
      try {
        const res = await fetch('/api/lossless/sync', { method: 'POST' });
        const data = await res.json();
        alert("[SYS_CMD] " + data.message);
      } catch (e) {
        alert("[SYS_ERR] Failed to execute lossless backup");
      }
    }

    async function startCleanup() {
      if (!confirm("Start full lossless audit & cleanup across all tracks with 50 parallel workers?")) return;
      try {
        const res = await fetch('/api/lossless/cleanup', { method: 'POST' });
        const data = await res.json();
        alert("[SYS_CMD] " + data.message);
      } catch (e) {
        alert("[SYS_ERR] Failed to start cleanup: " + e.message);
      }
    }

    async function triggerDbReconstruct() {
      if (!confirm("Scan all audio files in BOTH buckets (stream & lossless), reconstruct track indexes, and upload a fresh db_backup.json?")) return;
      const btn = document.getElementById('reconstruct-btn');
      if (btn) {
        btn.disabled = true;
        btn.innerText = "⏳ RECONSTRUCTING FROM BUCKETS...";
      }
      try {
        const res = await fetch('/api/db/reconstruct', { method: 'POST' });
        const data = await res.json();
        const details = data.result || data.details || {};
        alert(`[RECONSTRUCTION COMPLETE]\nStream Tracks: ${details.stream_tracks_indexed || 0}\nLossless Tracks: ${details.lossless_tracks_indexed || 0}\nTotal in Database Cache: ${details.total_songs_in_cache || 0}\nJSON backup refreshed!`);
      } catch (e) {
        alert("[SYS_ERR] Reconstruction failed: " + e.message);
      } finally {
        if (btn) {
          btn.disabled = false;
          btn.innerText = "🔄 RECONSTRUCT DB FROM BUCKETS";
        }
      }
    }

    function renderAdmin(data) {
      if (!data) return;

      const cleanup = data.cleanup || {};
      const isCleanupRunning = cleanup.running === true;

      const defaultView = document.getElementById('default-dashboard-view');
      const cleanupView = document.getElementById('cleanup-active-view');
      const headerTitle = document.getElementById('terminal-header-title');
      const statusBadge = document.getElementById('main-status-badge');
      const statusText = document.getElementById('main-status-text');

      if (isCleanupRunning) {
        // Switch into Cleanup GUI mode
        defaultView.style.display = 'none';
        cleanupView.style.display = 'block';
        headerTitle.textContent = 'muzo-lossless-cleanup@50-workers [AUDIT IN PROGRESS]';
        statusBadge.style.background = 'var(--amber-soft)';
        statusBadge.style.borderColor = 'var(--amber)';
        statusBadge.style.color = 'var(--amber-bright)';
        statusText.textContent = 'CLEANUP IN PROGRESS';

        const total = cleanup.total || 0;
        const checked = cleanup.checked || 0;
        const valid = cleanup.valid || 0;
        const invalid = cleanup.invalid || 0;
        const errors = cleanup.errors || 0;
        const pct = total > 0 ? ((checked / total) * 100).toFixed(1) : 0;

        document.getElementById('c-checked').textContent = checked;
        document.getElementById('c-total').textContent = total;
        document.getElementById('c-valid').textContent = valid;
        document.getElementById('c-invalid').textContent = invalid;
        document.getElementById('c-errors').textContent = errors;
        document.getElementById('c-pct').textContent = pct + '%';
        document.getElementById('c-progress-fill').style.width = pct + '%';

        // Render Invalid Table
        const invalidList = cleanup.recent_invalid || [];
        document.getElementById('c-invalid-count').textContent = `${invalidList.length} shown (${invalid} total)`;
        const invTbody = document.getElementById('c-invalid-tbody');
        if (invalidList.length === 0) {
          invTbody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: var(--text-muted); padding: 1.5rem;">No invalid tracks found yet</td></tr>';
        } else {
          invTbody.innerHTML = invalidList.slice().reverse().slice(0, 30).map(item => `
            <tr>
              <td style="font-weight: 600; color: var(--cyan);">${item.id}</td>
              <td><strong>${item.name || 'Unknown'}</strong><br><span style="color: var(--text-muted); font-size: 0.72rem;">${item.artists || 'Unknown'}</span></td>
              <td><span class="tag tag-invalid">SET TO 0</span></td>
            </tr>
          `).join('');
        }

        // Render Valid Table
        const validList = cleanup.recent_valid || [];
        document.getElementById('c-valid-count').textContent = `${validList.length} shown (${valid} total)`;
        const valTbody = document.getElementById('c-valid-tbody');
        if (validList.length === 0) {
          valTbody.innerHTML = '<tr><td colspan="3" style="text-align: center; color: var(--text-muted); padding: 1.5rem;">Auditing in progress...</td></tr>';
        } else {
          valTbody.innerHTML = validList.slice().reverse().slice(0, 30).map(item => `
            <tr>
              <td style="font-weight: 600; color: var(--cyan);">${item.id}</td>
              <td><strong>${item.name || item.matched || 'Verified'}</strong><br><span style="color: var(--text-muted); font-size: 0.72rem;">${item.artists || 'Verified'}</span></td>
              <td><span class="tag tag-valid">FLAC VALID</span></td>
            </tr>
          `).join('');
        }

      } else {
        // Return to Normal Operations mode
        defaultView.style.display = 'block';
        cleanupView.style.display = 'none';
        headerTitle.textContent = 'muzo-lossless-worker@cluster-01:~';
        statusBadge.style.background = 'var(--green-glow)';
        statusBadge.style.borderColor = 'rgba(63, 185, 80, 0.3)';
        statusBadge.style.color = 'var(--green-bright)';
        statusText.textContent = 'SYSTEM OPERATIONAL';

        const total = data.total || 0;
        const done = data.stream || 0;
        const pct = total > 0 ? ((done / total) * 100).toFixed(1) : 0;

        document.getElementById('progress-percent').textContent = pct + '%';
        document.getElementById('progress-fill').style.width = pct + '%';
        document.getElementById('done-count').textContent = done;
        document.getElementById('total-count').textContent = total;
        document.getElementById('lossless-count').textContent = data.lossless || 0;

        if (data.stats) {
          document.getElementById('val-progressing').textContent = data.stats.progressing || 0;
          document.getElementById('val-failed').textContent = data.stats.failed || 0;
          document.getElementById('val-queued').textContent = data.queue_length || 0;
        }

        if (data.avg_uploads_per_min !== undefined) {
          document.getElementById('upload-rate').textContent = data.avg_uploads_per_min.toFixed(1) + ' / min';
        }

        if (data.network_stats) {
          const dl = data.network_stats.last_download_speed_kbps;
          document.getElementById('dl-speed').textContent = dl > 1024 ? (dl/1024).toFixed(1) + ' MB/s' : dl.toFixed(1) + ' KB/s';
          const ul = data.network_stats.last_upload_speed_kbps;
          document.getElementById('ul-speed').textContent = ul > 1024 ? (ul/1024).toFixed(1) + ' MB/s' : ul.toFixed(1) + ' KB/s';
        }

        if (data.jobs) {
          const entries = Object.entries(data.jobs).sort((a, b) => b[1].timestamp - a[1].timestamp).slice(0, 20);
          const tbody = document.getElementById('jobs-tbody');
          tbody.innerHTML = entries.length === 0 
            ? '<tr><td colspan="3" style="text-align: center; color: var(--text-muted);">No active jobs running</td></tr>'
            : entries.map(([id, job]) => `<tr><td>${id}</td><td>${job.status.toUpperCase()}</td><td>${job.ext || 'm4a'}</td></tr>`).join('');
        }

        if (data.lossless_jobs) {
          const lEntries = Object.entries(data.lossless_jobs).sort((a, b) => b[1].timestamp - a[1].timestamp).slice(0, 20);
          const ltbody = document.getElementById('lossless-jobs-tbody');
          if (ltbody) {
            ltbody.innerHTML = lEntries.length === 0
              ? '<tr><td colspan="3" style="text-align: center; color: var(--text-muted);">No active lossless jobs</td></tr>'
              : lEntries.map(([id, job]) => `<tr><td>${id}</td><td>${job.status.toUpperCase()}</td><td>${job.ext || 'flac'}</td></tr>`).join('');
          }
        }
      }
    }

    async function pollAdmin() {
      try {
        const res = await fetch('/api/stats');
        if (!res.ok) return;
        const data = await res.json();
        renderAdmin(data);
      } catch (e) {}
    }

    function initWebSocket() {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${window.location.host}/ws/admin`;
      let ws = new WebSocket(wsUrl);

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          renderAdmin(data);
        } catch(e) {}
      };

      ws.onclose = () => setTimeout(initWebSocket, 2000);
      ws.onerror = () => ws.close();
    }

    initWebSocket();
    setInterval(pollAdmin, 3000);
    pollAdmin();
  </script>
</body>
</html>
"""

