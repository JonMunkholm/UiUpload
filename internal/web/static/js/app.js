// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

// Modal utilities - generic show/hide for any modal
function showModal(id) {
    const modal = document.getElementById(id);
    if (modal) modal.classList.remove('hidden');
}

function hideModal(id) {
    const modal = document.getElementById(id);
    if (modal) modal.classList.add('hidden');
}

// Storage key generators
const STORAGE_KEYS = {
    THEME: 'theme-mode',
    SIDEBAR: 'sidebar-collapsed',
    columns: (tableKey) => `columns_${tableKey}`,
    sort: (tableKey) => `sort_${tableKey}`,
    views: (tableKey) => `views_${tableKey}`,
    metrics: (tableKey) => `agg_metrics_${tableKey}`
};

// Generic storage helpers with JSON parsing
function getStorage(key, defaultValue = null) {
    const data = localStorage.getItem(key);
    if (!data) return defaultValue;
    try {
        return JSON.parse(data);
    } catch (e) {
        return defaultValue;
    }
}

function setStorage(key, value) {
    localStorage.setItem(key, JSON.stringify(value));
}

// ============================================================================
// THEME MANAGEMENT
// ============================================================================

function getThemeStorageKey() {
    return STORAGE_KEYS.THEME;
}

function getSavedTheme() {
    return localStorage.getItem(getThemeStorageKey()) || 'light';
}

function saveTheme(theme) {
    localStorage.setItem(getThemeStorageKey(), theme);
}

function applyTheme(theme) {
    if (theme === 'dark') {
        document.documentElement.classList.add('dark');
    } else {
        document.documentElement.classList.remove('dark');
    }
}

function toggleTheme() {
    const current = getSavedTheme();
    const newTheme = current === 'light' ? 'dark' : 'light';
    saveTheme(newTheme);
    applyTheme(newTheme);
}

// Initialize theme on page load (backup to inline script in layout)
document.addEventListener('DOMContentLoaded', function() {
    applyTheme(getSavedTheme());
});

// Ensure theme persists after HTMX full-page navigations
document.body.addEventListener('htmx:afterSettle', function() {
    applyTheme(getSavedTheme());
});

// ============================================================================
// SIDEBAR NAVIGATION
// ============================================================================

function getSidebarCollapsed() {
    return localStorage.getItem(STORAGE_KEYS.SIDEBAR) === 'true';
}

function saveSidebarState(collapsed) {
    localStorage.setItem(STORAGE_KEYS.SIDEBAR, collapsed ? 'true' : 'false');
}

function applySidebarState(collapsed) {
    const sidebar = document.getElementById('sidebar');
    if (!sidebar) return;

    if (collapsed) {
        sidebar.classList.add('collapsed');
    } else {
        sidebar.classList.remove('collapsed');
    }
}

function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    if (!sidebar) return;

    const isCollapsed = sidebar.classList.toggle('collapsed');
    saveSidebarState(isCollapsed);
}

// Initialize sidebar on page load
document.addEventListener('DOMContentLoaded', function() {
    applySidebarState(getSidebarCollapsed());
});

// Ensure sidebar state persists after HTMX navigations
document.body.addEventListener('htmx:afterSettle', function() {
    applySidebarState(getSidebarCollapsed());
});

// ============================================================================
// CARD OVERFLOW MENUS
// ============================================================================

function toggleCardMenu(tableKey) {
    const menu = document.getElementById('card-menu-' + tableKey);
    if (!menu) return;

    const wasHidden = menu.classList.contains('hidden');

    // Close all other menus first
    closeCardMenus();

    // Toggle this menu
    if (wasHidden) {
        menu.classList.remove('hidden');
    }
}

function closeCardMenus() {
    document.querySelectorAll('[id^="card-menu-"]').forEach(menu => {
        menu.classList.add('hidden');
    });
}

// Close card menus when clicking outside
document.addEventListener('click', function(e) {
    if (!e.target.closest('[id^="card-menu-"]') && !e.target.closest('button[aria-label="More actions"]')) {
        closeCardMenus();
    }
});

// ============================================================================
// TABLE LOADING INDICATOR
// ============================================================================

// Show loading indicator when HTMX requests target #table-container
document.body.addEventListener('htmx:beforeRequest', function(e) {
    const target = e.detail.target;
    if (target && target.id === 'table-container') {
        target.classList.add('htmx-request');
    }
});

// Hide loading indicator after swap completes
document.body.addEventListener('htmx:afterSwap', function(e) {
    const target = e.detail.target;
    if (target && target.id === 'table-container') {
        target.classList.remove('htmx-request');
    }
});

// Also handle request errors
document.body.addEventListener('htmx:responseError', function(e) {
    const target = e.detail.target;
    if (target && target.id === 'table-container') {
        target.classList.remove('htmx-request');
    }
});

// ============================================================================
// UPLOAD MODAL HANDLING
// ============================================================================

// Maximum file size: 100MB
const MAX_FILE_SIZE = 100 * 1024 * 1024;

// Validate file size before upload
function validateFileSize(input) {
    const file = input.files[0];
    if (!file) return false;

    if (file.size > MAX_FILE_SIZE) {
        const sizeMB = (file.size / (1024 * 1024)).toFixed(1);
        showError(`File is too large (${sizeMB} MB). Maximum size is 100 MB.`);
        input.value = ''; // Clear the file input
        return false;
    }
    return true;
}

// Format file size for display
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// Upload modal handling
function showUploadModal() {
    showModal('upload-modal');
}

function hideUploadModal() {
    hideModal('upload-modal');
    document.getElementById('upload-progress-container').innerHTML = '';
}

// Handle upload response - start SSE connection for progress
function handleUploadResponse(event) {
    const response = event.detail.xhr.response;
    try {
        const data = JSON.parse(response);
        if (data.upload_id) {
            startProgressStream(data.upload_id);
        } else if (data.error) {
            showError(data.error);
        }
    } catch (e) {
        showError('Upload failed');
    }
}

// Track current upload for cancel functionality
let currentUpload = {
    id: null,
    sseClient: null
};

// Start SSE stream for upload progress with robust reconnection
function startProgressStream(uploadId) {
    const container = document.getElementById('upload-progress-container');

    // Clean up any existing SSE connection
    if (currentUpload.sseClient) {
        currentUpload.sseClient.close();
    }

    // Create new SSE client with exponential backoff reconnection
    const client = new SSEClient(`/api/upload/${uploadId}/progress`, {
        maxRetries: 10,
        baseDelay: 1000,
        maxDelay: 30000,

        onProgress: (progress) => {
            container.innerHTML = renderProgress(progress, uploadId);
        },

        onComplete: () => {
            currentUpload.id = null;
            currentUpload.sseClient = null;

            // Fetch final result
            fetch(`/api/upload/${uploadId}/result`)
                .then(r => r.json())
                .then(result => {
                    container.innerHTML = renderComplete(result);
                })
                .catch(() => {
                    showToast('Upload completed');
                    hideUploadModal();
                });
        },

        onError: (error) => {
            if (error.code === 'MAX_RETRIES') {
                container.innerHTML = renderConnectionLost(uploadId);
            } else if (error.message) {
                container.innerHTML = renderUploadError(error.message);
            }
        },

        onConnectionChange: ({ connected, reconnecting, failed }) => {
            updateConnectionStatus(container, { connected, reconnecting, failed });
        }
    });

    client.connect();

    // Store current upload reference for cancellation
    currentUpload.id = uploadId;
    currentUpload.sseClient = client;
}

// Update connection status indicator in the progress UI
function updateConnectionStatus(container, { connected, reconnecting, failed }) {
    let statusEl = container.querySelector('.connection-status');

    // Create status element if it doesn't exist and we have content
    if (!statusEl && container.innerHTML.trim()) {
        statusEl = document.createElement('div');
        statusEl.className = 'connection-status';
        statusEl.innerHTML = '<span class="status-dot"></span><span class="status-text"></span>';
        container.insertBefore(statusEl, container.firstChild);
    }

    if (!statusEl) return;

    const dot = statusEl.querySelector('.status-dot');
    const text = statusEl.querySelector('.status-text');

    if (failed) {
        statusEl.classList.add('disconnected', 'failed');
        statusEl.classList.remove('reconnecting');
        text.textContent = 'Connection lost';
    } else if (reconnecting) {
        statusEl.classList.add('disconnected', 'reconnecting');
        statusEl.classList.remove('failed');
        text.textContent = 'Reconnecting...';
    } else if (connected) {
        statusEl.classList.remove('disconnected', 'reconnecting', 'failed');
        text.textContent = 'Connected';
    } else {
        statusEl.classList.add('disconnected');
        text.textContent = 'Disconnected';
    }
}

// Render connection lost state with retry option
function renderConnectionLost(uploadId) {
    return `
        <div class="space-y-4 text-center py-6">
            <div class="flex justify-center">
                <svg class="w-12 h-12 text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                          d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
                </svg>
            </div>
            <div>
                <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100">Connection Lost</h3>
                <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">
                    Unable to reconnect after multiple attempts.
                </p>
            </div>
            <div class="flex justify-center gap-3">
                <button onclick="startProgressStream('${uploadId}')"
                        class="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors">
                    Try Again
                </button>
                <button onclick="hideUploadModal()"
                        class="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg text-sm font-medium hover:bg-gray-300 transition-colors dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600">
                    Close
                </button>
            </div>
        </div>
    `;
}

// Render upload error state
function renderUploadError(message) {
    return `
        <div class="space-y-4 text-center py-6">
            <div class="flex justify-center">
                <svg class="w-12 h-12 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                          d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/>
                </svg>
            </div>
            <div>
                <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100">Upload Error</h3>
                <p class="mt-1 text-sm text-red-600 dark:text-red-400">${message}</p>
            </div>
            <button onclick="hideUploadModal()"
                    class="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg text-sm font-medium hover:bg-gray-300 transition-colors dark:bg-gray-700 dark:text-gray-300 dark:hover:bg-gray-600">
                Close
            </button>
        </div>
    `;
}

// Cancel current upload
function cancelUpload() {
    if (!currentUpload.id) return;

    const confirmed = confirm('Are you sure you want to cancel this upload? Any partially imported data will be rolled back.');
    if (!confirmed) return;

    // Close SSE connection
    if (currentUpload.sseClient) {
        currentUpload.sseClient.close();
    }

    // Send cancel request to server
    fetch(`/api/upload/${currentUpload.id}/cancel`, {
        method: 'POST'
    })
    .then(r => r.json())
    .then(result => {
        if (result.success) {
            showToast('Upload cancelled');
        } else {
            showToast(result.error || 'Failed to cancel upload', true);
        }
    })
    .catch(() => {
        showToast('Failed to cancel upload', true);
    })
    .finally(() => {
        currentUpload.id = null;
        currentUpload.sseClient = null;
        hideUploadModal();
    });
}

// Clean up SSE connection on page unload to prevent dangling connections
window.addEventListener('beforeunload', () => {
    if (currentUpload.sseClient) {
        currentUpload.sseClient.close();
        currentUpload.sseClient = null;
    }
});

// Render progress HTML with cancel button
function renderProgress(progress, uploadId) {
    const percent = progress.total_rows > 0
        ? Math.round((progress.current_row / progress.total_rows) * 100)
        : 0;

    const phaseLabels = {
        'starting': 'Starting...',
        'reading': 'Reading file...',
        'validating': 'Validating...',
        'inserting': 'Inserting rows...',
        'complete': 'Complete',
        'failed': 'Failed',
        'cancelled': 'Cancelled'
    };

    const barColor = progress.phase === 'complete' ? 'bg-green-500'
        : progress.phase === 'failed' ? 'bg-red-500'
        : progress.phase === 'cancelled' ? 'bg-yellow-500'
        : 'bg-blue-500';

    const isActive = !['complete', 'failed', 'cancelled'].includes(progress.phase);
    const fileSize = progress.file_size ? formatFileSize(progress.file_size) : '';

    let html = `
        <div class="space-y-4">
            <div class="flex items-center justify-between">
                <div class="min-w-0">
                    <span class="text-sm font-medium text-gray-900 dark:text-white block truncate">${progress.file_name || 'Uploading...'}</span>
                    ${fileSize ? `<span class="text-xs text-gray-400">${fileSize}</span>` : ''}
                </div>
                <span class="text-sm text-gray-500 dark:text-gray-400 whitespace-nowrap ml-2">${phaseLabels[progress.phase] || progress.phase}</span>
            </div>

            <div class="relative">
                <div class="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-3">
                    <div class="h-3 rounded-full transition-all duration-300 ${barColor}" style="width: ${percent}%"></div>
                </div>
                <span class="absolute inset-0 flex items-center justify-center text-xs font-medium ${percent > 50 ? 'text-white' : 'text-gray-700 dark:text-gray-300'}">${percent}%</span>
            </div>

            <div class="flex justify-between text-xs text-gray-500 dark:text-gray-400">
                <span>${progress.current_row.toLocaleString()} / ${progress.total_rows.toLocaleString()} rows</span>
                <span class="text-green-600 dark:text-green-400">${progress.inserted.toLocaleString()} inserted</span>
                ${progress.skipped > 0 ? `<span class="text-amber-600 dark:text-amber-400">${progress.skipped.toLocaleString()} skipped</span>` : ''}
            </div>
    `;

    if (progress.error) {
        html += `<div class="text-sm text-red-600 bg-red-50 dark:bg-red-900/30 dark:text-red-400 rounded p-3">${progress.error}</div>`;
    }

    // Cancel button during active upload
    if (isActive) {
        html += `
            <div class="flex justify-end pt-2 border-t border-gray-200 dark:border-gray-700">
                <button
                    type="button"
                    onclick="cancelUpload()"
                    class="px-4 py-2 text-sm font-medium text-red-600 hover:text-red-700 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-900/30 rounded-md transition-colors"
                >
                    Cancel Upload
                </button>
            </div>
        `;
    }

    html += '</div>';
    return html;
}

// Render completion HTML
function renderComplete(result) {
    const icon = result.error
        ? '<svg class="w-6 h-6 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>'
        : '<svg class="w-6 h-6 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>';

    const title = result.error ? 'Upload Failed' : 'Upload Complete';
    const titleColor = result.error ? 'text-red-700' : 'text-green-700';

    let html = `
        <div class="space-y-4">
            <div class="flex items-center gap-2">
                ${icon}
                <span class="font-medium ${titleColor}">${title}</span>
            </div>
            <div class="bg-gray-50 rounded-lg p-4 space-y-2">
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">File</span>
                    <span class="text-gray-900">${result.file_name || ''}</span>
                </div>
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">Duration</span>
                    <span class="text-gray-900">${result.duration || ''}</span>
                </div>
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">Inserted</span>
                    <span class="text-gray-900 font-medium">${result.inserted || 0}</span>
                </div>
    `;

    if (result.skipped > 0) {
        html += `
                <div class="flex justify-between text-sm">
                    <span class="text-gray-500">Skipped</span>
                    <span class="text-amber-600 font-medium">${result.skipped}</span>
                </div>
        `;
    }

    html += '</div>';

    if (result.error) {
        html += `<div class="text-sm text-red-600 bg-red-50 rounded p-3">${result.error}</div>`;
    }

    html += `
            <button onclick="hideUploadModal()" class="w-full py-2 px-4 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 transition-colors">
                Close
            </button>
        </div>
    `;

    return html;
}

// Toast notifications
function showToast(message, isError = false) {
    const toast = document.createElement('div');
    toast.className = `fixed bottom-4 right-4 px-4 py-3 rounded-lg shadow-lg text-white transition-all duration-300 z-50 ${isError ? 'bg-red-600' : 'bg-green-600'}`;
    toast.innerHTML = `
        <div class="flex items-center gap-2">
            ${isError
                ? '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>'
                : '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>'
            }
            <span>${message}</span>
        </div>
    `;
    document.body.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

function showError(message) {
    showToast(message, true);
}

// Add drag and drop styling
document.addEventListener('DOMContentLoaded', function() {
    document.querySelectorAll('.upload-zone').forEach(zone => {
        zone.addEventListener('dragover', function(e) {
            e.preventDefault();
            this.classList.add('border-blue-400', 'bg-blue-50');
        });

        zone.addEventListener('dragleave', function(e) {
            e.preventDefault();
            this.classList.remove('border-blue-400', 'bg-blue-50');
        });

        zone.addEventListener('drop', function(e) {
            e.preventDefault();
            this.classList.remove('border-blue-400', 'bg-blue-50');

            const files = e.dataTransfer.files;
            if (files.length > 0) {
                const input = this.querySelector('input[type="file"]');
                const dataTransfer = new DataTransfer();
                dataTransfer.items.add(files[0]);
                input.files = dataTransfer.files;
                input.dispatchEvent(new Event('change', { bubbles: true }));
            }
        });
    });
});

// ============================================================================
// Column Toggle Feature
// ============================================================================

// Get localStorage key for a table's column visibility
function getColumnStorageKey(tableKey) {
    return STORAGE_KEYS.columns(tableKey);
}

// Get visible columns from localStorage (default: all visible)
function getVisibleColumns(tableKey, allColumns) {
    return getStorage(STORAGE_KEYS.columns(tableKey), allColumns);
}

// Save visible columns to localStorage
function saveVisibleColumns(tableKey, columns) {
    setStorage(STORAGE_KEYS.columns(tableKey), columns);
}

// Get all column names from table headers
function getAllColumns() {
    const headers = document.querySelectorAll('table thead th');
    return Array.from(headers).map(th => {
        // Get text content, excluding the sort icon
        const span = th.querySelector('span');
        return span ? span.textContent.trim() : th.textContent.trim();
    });
}

// Get table key from data attribute
function getTableKey() {
    const container = document.getElementById('table-container');
    return container ? container.dataset.tableKey : null;
}

// Toggle dropdown visibility
function toggleColumnDropdown() {
    const dropdown = document.getElementById('column-dropdown');
    if (dropdown) {
        dropdown.classList.toggle('hidden');
    }
}

// Initialize column toggle UI and apply saved visibility
function initColumnToggle() {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const allColumns = getAllColumns();
    if (allColumns.length === 0) return;

    const visible = getVisibleColumns(tableKey, allColumns);
    const container = document.getElementById('column-checkboxes');
    if (!container) return;

    // Render checkboxes
    container.innerHTML = allColumns.map((col, i) => `
        <label class="flex items-center gap-2 px-2 py-1 hover:bg-gray-50 rounded cursor-pointer">
            <input type="checkbox"
                   data-col-index="${i}"
                   data-col-name="${col}"
                   ${visible.includes(col) ? 'checked' : ''}
                   onchange="handleColumnToggle(this)"
                   class="rounded border-gray-300 text-blue-600 focus:ring-blue-500">
            <span class="text-sm text-gray-700 truncate">${col}</span>
        </label>
    `).join('');

    // Apply initial visibility
    applyColumnVisibility();
}

// Handle checkbox change
function handleColumnToggle(checkbox) {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const allColumns = getAllColumns();
    let visible = getVisibleColumns(tableKey, allColumns);
    const colName = checkbox.dataset.colName;

    if (checkbox.checked) {
        if (!visible.includes(colName)) {
            visible.push(colName);
        }
    } else {
        visible = visible.filter(c => c !== colName);
    }

    saveVisibleColumns(tableKey, visible);
    applyColumnVisibility();
}

// Apply column visibility based on saved state
function applyColumnVisibility() {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const table = document.querySelector('table');
    if (!table) return;

    const allColumns = getAllColumns();
    const visible = getVisibleColumns(tableKey, allColumns);

    allColumns.forEach((col, i) => {
        const isVisible = visible.includes(col);
        const nth = i + 1;

        // Toggle header
        const th = table.querySelector(`thead th:nth-child(${nth})`);
        if (th) th.style.display = isVisible ? '' : 'none';

        // Toggle all cells in that column
        table.querySelectorAll(`tbody td:nth-child(${nth})`).forEach(td => {
            td.style.display = isVisible ? '' : 'none';
        });
    });
}

// Select all columns
function selectAllColumns() {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const allColumns = getAllColumns();
    saveVisibleColumns(tableKey, allColumns);

    // Update checkboxes
    document.querySelectorAll('#column-checkboxes input[type="checkbox"]').forEach(cb => {
        cb.checked = true;
    });

    applyColumnVisibility();
}

// Clear all columns (hide all)
function clearAllColumns() {
    const tableKey = getTableKey();
    if (!tableKey) return;

    saveVisibleColumns(tableKey, []);

    // Update checkboxes
    document.querySelectorAll('#column-checkboxes input[type="checkbox"]').forEach(cb => {
        cb.checked = false;
    });

    applyColumnVisibility();
}

// Close dropdown when clicking outside
document.addEventListener('click', function(e) {
    const dropdown = document.getElementById('column-dropdown');
    const button = e.target.closest('[onclick*="toggleColumnDropdown"]');

    if (dropdown && !dropdown.contains(e.target) && !button) {
        dropdown.classList.add('hidden');
    }
});

// Re-apply visibility after HTMX swaps
document.body.addEventListener('htmx:afterSwap', function(e) {
    if (e.detail.target.id === 'table-container') {
        // Re-init checkboxes and apply visibility
        initColumnToggle();
    }
});

// ============================================================================
// Sort Persistence Feature (Multi-Column Sort)
// ============================================================================

// Get localStorage key for a table's sort preference
function getSortStorageKey(tableKey) {
    return STORAGE_KEYS.sort(tableKey);
}

// Get saved sorts from localStorage (returns array of {column, dir})
function getSavedSorts(tableKey) {
    const parsed = getStorage(STORAGE_KEYS.sort(tableKey), null);
    if (!parsed) return [];
    // Handle legacy single-sort format
    if (parsed.column) {
        return [{ column: parsed.column, dir: parsed.dir }];
    }
    // New array format
    if (Array.isArray(parsed)) {
        return parsed;
    }
    return [];
}

// Save sorts to localStorage (array format)
function saveSorts(tableKey, sorts) {
    setStorage(STORAGE_KEYS.sort(tableKey), sorts);
}

// Get current sorts from URL
function getCurrentSorts() {
    const url = new URL(window.location.href);
    const sortStr = url.searchParams.get('sort') || '';
    const dirStr = url.searchParams.get('dir') || '';

    if (!sortStr) return [];

    const cols = sortStr.split(',');
    const dirs = dirStr.split(',');

    const sorts = [];
    for (let i = 0; i < cols.length && i < 2; i++) {
        const col = cols[i].trim();
        if (col) {
            sorts.push({
                column: col,
                dir: (dirs[i] || 'asc').trim()
            });
        }
    }
    return sorts;
}

// Toggle sort direction
function toggleDir(dir) {
    return dir === 'asc' ? 'desc' : 'asc';
}

// Build sort URL from sorts array
function buildSortUrl(tableKey, sorts, searchQuery, filterParams) {
    if (sorts.length === 0) {
        return `/table/${tableKey}?page=1`;
    }

    const cols = sorts.map(s => s.column).join(',');
    const dirs = sorts.map(s => s.dir).join(',');

    let url = `/table/${tableKey}?page=1&sort=${encodeURIComponent(cols)}&dir=${dirs}`;

    if (searchQuery) {
        url += '&search=' + encodeURIComponent(searchQuery);
    }

    if (filterParams) {
        url += filterParams;
    }

    return url;
}

// Get current search query from URL
function getCurrentSearch() {
    const url = new URL(window.location.href);
    return url.searchParams.get('search') || '';
}

// Get current filter params from URL (preserves filter[...] params)
function getCurrentFilterParams() {
    const url = new URL(window.location.href);
    let filterParams = '';
    url.searchParams.forEach((value, key) => {
        if (key.startsWith('filter[')) {
            filterParams += '&' + encodeURIComponent(key) + '=' + encodeURIComponent(value);
        }
    });
    return filterParams;
}

// Handle sort click with Shift detection
function handleSortClick(event, element) {
    event.preventDefault();

    const col = element.dataset.col;
    const tableKey = element.dataset.table;
    const isShiftClick = event.shiftKey;

    const currentSorts = getCurrentSorts();
    let newSorts;

    if (isShiftClick && currentSorts.length > 0) {
        // Shift+Click: add as secondary sort
        const primary = currentSorts[0];
        if (primary.column === col) {
            // Shift+clicking primary - just toggle direction
            newSorts = [{ column: col, dir: toggleDir(primary.dir) }];
        } else {
            // Check if clicking on existing secondary
            if (currentSorts.length > 1 && currentSorts[1].column === col) {
                // Toggle secondary direction
                newSorts = [primary, { column: col, dir: toggleDir(currentSorts[1].dir) }];
            } else {
                // Add as new secondary (or replace existing secondary)
                newSorts = [primary, { column: col, dir: 'asc' }];
            }
        }
    } else {
        // Regular click - replace sort
        const existing = currentSorts.find(s => s.column === col);
        const dir = existing ? toggleDir(existing.dir) : 'asc';
        newSorts = [{ column: col, dir }];
    }

    // Build URL and navigate
    const searchQuery = getCurrentSearch();
    const filterParams = getCurrentFilterParams();
    const url = buildSortUrl(tableKey, newSorts, searchQuery, filterParams);

    htmx.ajax('GET', url, {
        target: '#table-container',
        swap: 'innerHTML',
        pushUrl: true
    });

    // Save to localStorage
    saveSorts(tableKey, newSorts);
}

// On page load: redirect to saved sort if no URL sort params
function initSortPersistence() {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const url = new URL(window.location.href);
    const hasUrlSort = url.searchParams.has('sort');

    if (!hasUrlSort) {
        const saved = getSavedSorts(tableKey);
        if (saved.length > 0) {
            const cols = saved.map(s => s.column).join(',');
            const dirs = saved.map(s => s.dir).join(',');
            url.searchParams.set('sort', cols);
            url.searchParams.set('dir', dirs);
            url.searchParams.set('page', '1');
            window.location.replace(url.toString());
        }
    }
}

// ============================================================================
// Saved Views Feature
// ============================================================================

// Get localStorage key for a table's saved views
function getViewsStorageKey(tableKey) {
    return STORAGE_KEYS.views(tableKey);
}

// Get all saved views for a table
function getSavedViews(tableKey) {
    return getStorage(STORAGE_KEYS.views(tableKey), []);
}

// Save views array to localStorage
function saveViews(tableKey, views) {
    setStorage(STORAGE_KEYS.views(tableKey), views);
}

// Capture current view state from URL and localStorage
function captureCurrentView() {
    const url = new URL(window.location.href);
    const tableKey = getTableKey();
    if (!tableKey) return null;

    // Extract filters from URL
    const filters = {};
    url.searchParams.forEach((value, key) => {
        if (key.startsWith('filter[') && key.endsWith(']')) {
            const col = key.slice(7, -1);
            filters[col] = value;
        }
    });

    // Extract sort
    const sort = {
        column: url.searchParams.get('sort') || '',
        dir: url.searchParams.get('dir') || 'asc'
    };

    // Extract search
    const search = url.searchParams.get('search') || '';

    // Get visible columns from localStorage
    const allColumns = getAllColumns();
    const columns = getVisibleColumns(tableKey, allColumns);

    return { filters, sort, search, columns };
}

// Add a new view
function addView(name) {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const views = getSavedViews(tableKey);
    const state = captureCurrentView();
    if (!state) return;

    views.push({
        id: 'v_' + Date.now(),
        name: name,
        ...state
    });

    saveViews(tableKey, views);
    renderViewsDropdown();
}

// Delete a view by ID
function deleteView(viewId) {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const views = getSavedViews(tableKey).filter(v => v.id !== viewId);
    saveViews(tableKey, views);
    renderViewsDropdown();
}

// Apply a saved view
function applyView(viewId) {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const views = getSavedViews(tableKey);
    const view = views.find(v => v.id === viewId);
    if (!view) return;

    // Build URL with filters, sort, search
    const url = new URL(window.location.origin + '/table/' + tableKey);
    url.searchParams.set('page', '1');

    if (view.sort && view.sort.column) {
        url.searchParams.set('sort', view.sort.column);
        url.searchParams.set('dir', view.sort.dir || 'asc');
    }

    if (view.search) {
        url.searchParams.set('search', view.search);
    }

    if (view.filters) {
        for (const [col, opVal] of Object.entries(view.filters)) {
            url.searchParams.append('filter[' + col + ']', opVal);
        }
    }

    // Apply column visibility
    if (view.columns && view.columns.length > 0) {
        saveVisibleColumns(tableKey, view.columns);
    }

    // Navigate via HTMX
    htmx.ajax('GET', url.pathname + url.search, {
        target: '#table-container',
        swap: 'innerHTML',
        pushUrl: true
    });

    closeViewsDropdown();
}

// Render the views dropdown content
function renderViewsDropdown() {
    const dropdown = document.getElementById('views-dropdown');
    if (!dropdown) return;

    const tableKey = getTableKey();
    if (!tableKey) return;

    const views = getSavedViews(tableKey);

    let html = `
        <button type="button" onclick="showSaveViewDialog()"
                class="w-full text-left px-3 py-2 text-sm text-blue-600 hover:bg-gray-50 flex items-center gap-2">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11.049 2.927c.3-.921 1.603-.921 1.902 0l1.519 4.674a1 1 0 00.95.69h4.915c.969 0 1.371 1.24.588 1.81l-3.976 2.888a1 1 0 00-.363 1.118l1.518 4.674c.3.922-.755 1.688-1.538 1.118l-3.976-2.888a1 1 0 00-1.176 0l-3.976 2.888c-.783.57-1.838-.197-1.538-1.118l1.518-4.674a1 1 0 00-.363-1.118l-3.976-2.888c-.784-.57-.38-1.81.588-1.81h4.914a1 1 0 00.951-.69l1.519-4.674z"></path>
            </svg>
            Save current view
        </button>
    `;

    if (views.length > 0) {
        html += '<div class="border-t border-gray-200 my-1"></div>';

        views.forEach(view => {
            html += `
                <div class="group flex items-center justify-between px-3 py-2 hover:bg-gray-50">
                    <button type="button" onclick="applyView('${escapeHtml(view.id)}')"
                            class="flex-1 text-left text-sm text-gray-700 truncate" title="${escapeHtml(view.name)}">
                        ${escapeHtml(view.name)}
                    </button>
                    <button type="button" onclick="event.stopPropagation(); deleteView('${escapeHtml(view.id)}')"
                            class="hidden group-hover:block p-1 text-gray-400 hover:text-red-600 flex-shrink-0"
                            title="Delete view">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                        </svg>
                    </button>
                </div>
            `;
        });
    }

    dropdown.innerHTML = html;
}

// Toggle views dropdown visibility
function toggleViewsDropdown() {
    const dropdown = document.getElementById('views-dropdown');
    if (dropdown) {
        const wasHidden = dropdown.classList.contains('hidden');
        dropdown.classList.toggle('hidden');
        if (wasHidden) {
            renderViewsDropdown();
        }
    }
}

// Close views dropdown
function closeViewsDropdown() {
    const dropdown = document.getElementById('views-dropdown');
    if (dropdown) dropdown.classList.add('hidden');
}

// Show save view dialog (simple prompt for now)
function showSaveViewDialog() {
    const name = prompt('Enter view name:');
    if (name && name.trim()) {
        addView(name.trim());
        showToast('View saved');
    }
}

// Initialize views dropdown (close on outside click)
function initViewsDropdown() {
    document.addEventListener('click', function(e) {
        if (!e.target.closest('#views-container')) {
            closeViewsDropdown();
        }
    });
}

// ============================================================================
// Keyboard Shortcuts
// ============================================================================

function initKeyboardShortcuts() {
    document.addEventListener('keydown', function(e) {
        // Ignore if typing in input/textarea (edit inputs have their own handlers)
        if (e.target.matches('input, textarea, select')) {
            // Allow Esc to blur and clear (unless in edit mode - handled by edit keydown)
            if (e.key === 'Escape' && !e.target.classList.contains('edit-input')) {
                e.target.blur();
                clearSearch();
                closeColumnDropdown();
            }
            return;
        }

        switch (e.key) {
            case '/':
                e.preventDefault();
                focusSearch();
                break;
            case 'Escape':
                cancelEdit();
                cancelPreview();
                closeColumnDropdown();
                closeViewsDropdown();
                hideDeleteModal();
                hideKeyboardHelp();
                break;
            case 'c':
                toggleColumnDropdown();
                break;
            case 'v':
                toggleViewsDropdown();
                break;
            case 'e':
                triggerExport();
                break;
            case 'ArrowLeft':
                goToPrevPage();
                break;
            case 'ArrowRight':
                goToNextPage();
                break;
            case '?':
                showShortcutsHelp();
                break;
        }
    });
}

function focusSearch() {
    const search = document.querySelector('input[name="search"]');
    if (search) search.focus();
}

function clearSearch() {
    const search = document.querySelector('input[name="search"]');
    if (search && search.value) {
        search.value = '';
        search.dispatchEvent(new Event('search', { bubbles: true }));
    }
}

function triggerExport() {
    const exportLink = document.querySelector('a[href*="/api/export/"]');
    if (exportLink) exportLink.click();
}

function goToPrevPage() {
    const buttons = document.querySelectorAll('button[hx-get]');
    for (const btn of buttons) {
        if (btn.textContent.trim() === 'Previous') {
            btn.click();
            break;
        }
    }
}

function goToNextPage() {
    const buttons = document.querySelectorAll('button[hx-get]');
    for (const btn of buttons) {
        if (btn.textContent.trim() === 'Next') {
            btn.click();
            break;
        }
    }
}

function showShortcutsHelp() {
    // Show a more comprehensive help modal
    const helpHtml = `
        <div class="text-left text-sm space-y-3">
            <div>
                <div class="font-semibold text-gray-700 mb-1">Navigation</div>
                <div class="text-gray-600 space-y-0.5">
                    <div><kbd class="px-1 py-0.5 bg-gray-100 rounded text-xs">↑↓←→</kbd> Move between cells</div>
                    <div><kbd class="px-1 py-0.5 bg-gray-100 rounded text-xs">Tab</kbd> Next cell</div>
                    <div><kbd class="px-1 py-0.5 bg-gray-100 rounded text-xs">Enter</kbd> Edit cell</div>
                    <div><kbd class="px-1 py-0.5 bg-gray-100 rounded text-xs">Esc</kbd> Cancel / Clear focus</div>
                </div>
            </div>
            <div>
                <div class="font-semibold text-gray-700 mb-1">Global</div>
                <div class="text-gray-600 space-y-0.5">
                    <div><kbd class="px-1 py-0.5 bg-gray-100 rounded text-xs">/</kbd> Focus search</div>
                    <div><kbd class="px-1 py-0.5 bg-gray-100 rounded text-xs">c</kbd> Toggle columns</div>
                    <div><kbd class="px-1 py-0.5 bg-gray-100 rounded text-xs">v</kbd> Saved views</div>
                    <div><kbd class="px-1 py-0.5 bg-gray-100 rounded text-xs">e</kbd> Export CSV</div>
                    <div><kbd class="px-1 py-0.5 bg-gray-100 rounded text-xs">?</kbd> Show this help</div>
                </div>
            </div>
        </div>
    `;

    // Create help modal if it doesn't exist
    let modal = document.getElementById('keyboard-help-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'keyboard-help-modal';
        modal.className = 'fixed inset-0 bg-gray-500 bg-opacity-75 flex items-center justify-center z-50';
        modal.innerHTML = `
            <div class="bg-white rounded-lg shadow-xl max-w-sm w-full mx-4">
                <div class="flex items-center justify-between p-4 border-b">
                    <h3 class="text-lg font-semibold text-gray-900">Keyboard Shortcuts</h3>
                    <button onclick="hideKeyboardHelp()" class="text-gray-400 hover:text-gray-600">
                        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                        </svg>
                    </button>
                </div>
                <div class="p-4" id="keyboard-help-content"></div>
            </div>
        `;
        document.body.appendChild(modal);
    }

    document.getElementById('keyboard-help-content').innerHTML = helpHtml;
    modal.classList.remove('hidden');
}

function hideKeyboardHelp() {
    const modal = document.getElementById('keyboard-help-modal');
    if (modal) modal.classList.add('hidden');
}

// ============================================================================
// Utility Functions
// ============================================================================

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ============================================================================
// CSV Preview & Column Mapping
// ============================================================================

let currentPreviewForm = null;
let currentPreviewCSVHeaders = [];

function showPreview(input) {
    if (!input.files || !input.files[0]) return;

    const file = input.files[0];
    const form = input.closest('form');
    const expectedColumns = JSON.parse(form.dataset.columns || '[]');
    const uniqueKey = JSON.parse(form.dataset.uniqueKey || '[]');
    const tableLabel = form.dataset.tableLabel || 'Table';
    const tableKey = form.id.replace('upload-form-', '');

    currentPreviewForm = form;
    currentPreviewFile = file;
    currentPreviewTableKey = tableKey;

    const reader = new FileReader();
    reader.onload = function(e) {
        const text = e.target.result;
        const { headers, rows, allRows, totalRows } = parseCSV(text);
        renderPreview(tableLabel, tableKey, expectedColumns, uniqueKey, headers, rows, allRows, totalRows, file.name);
    };
    reader.readAsText(file);
}

function parseCSV(text, previewOnly = false) {
    const lines = text.trim().split('\n');
    if (lines.length === 0) return { headers: [], rows: [], allRows: [], totalRows: 0 };

    const headers = parseCSVLine(lines[0]);
    const rows = [];
    const allRows = [];
    const previewCount = Math.min(5, lines.length - 1);

    for (let i = 1; i < lines.length; i++) {
        if (lines[i]) {
            const parsed = parseCSVLine(lines[i]);
            allRows.push({ data: parsed, lineNumber: i + 1 });
            if (i <= previewCount) rows.push(parsed);
        }
    }

    return { headers, rows, allRows, totalRows: lines.length - 1 };
}

function parseCSVLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
        const char = line[i];
        if (char === '"') {
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            result.push(current.trim());
            current = '';
        } else {
            current += char;
        }
    }
    result.push(current.trim());
    return result;
}

// Levenshtein distance for fuzzy matching
function levenshtein(a, b) {
    const matrix = Array(b.length + 1).fill().map(() => Array(a.length + 1).fill(0));
    for (let i = 0; i <= a.length; i++) matrix[0][i] = i;
    for (let j = 0; j <= b.length; j++) matrix[j][0] = j;
    for (let j = 1; j <= b.length; j++) {
        for (let i = 1; i <= a.length; i++) {
            const cost = a[i-1] === b[j-1] ? 0 : 1;
            matrix[j][i] = Math.min(
                matrix[j][i-1] + 1,
                matrix[j-1][i] + 1,
                matrix[j-1][i-1] + cost
            );
        }
    }
    return matrix[b.length][a.length];
}

// Find best matching CSV column for an expected column
function suggestMapping(expected, csvHeaders) {
    const expNorm = expected.toLowerCase().replace(/[_\s]+/g, '');
    let best = { index: -1, score: Infinity };

    csvHeaders.forEach((h, i) => {
        const hNorm = h.toLowerCase().replace(/[_\s]+/g, '');

        // Exact match (ignoring case/spaces/underscores)
        if (expNorm === hNorm) {
            best = { index: i, score: 0 };
            return;
        }

        // Contains match
        if (hNorm.includes(expNorm) || expNorm.includes(hNorm)) {
            const score = Math.abs(hNorm.length - expNorm.length) * 0.5;
            if (score < best.score) best = { index: i, score };
            return;
        }

        // Levenshtein distance
        const dist = levenshtein(expNorm, hNorm);
        const norm = dist / Math.max(expNorm.length, hNorm.length) * 10;
        if (norm < best.score) best = { index: i, score: norm };
    });

    return best.score <= 5 ? best.index : -1;
}

// Build auto-mapping suggestions
function buildAutoMapping(expected, csvHeaders) {
    const mapping = {};
    const used = new Set();

    expected.forEach(exp => {
        const idx = suggestMapping(exp, csvHeaders);
        if (idx >= 0 && !used.has(idx)) {
            mapping[exp] = idx;
            used.add(idx);
        } else {
            mapping[exp] = -1;
        }
    });

    return mapping;
}

// Render mapping UI with dropdowns
function renderMappingUI(expected, csvHeaders, autoMapping) {
    return `
        <div class="space-y-2">
            <div class="text-sm font-medium text-gray-700 mb-3">Map CSV columns to expected fields:</div>
            ${expected.map(exp => {
                const suggested = autoMapping[exp];
                return `
                <div class="flex items-center gap-3">
                    <span class="w-1/3 text-sm text-gray-600 text-right truncate" title="${escapeHtml(exp)}">${escapeHtml(exp)}</span>
                    <span class="text-gray-400">→</span>
                    <select class="mapping-select flex-1 text-sm border border-gray-300 rounded px-2 py-1.5 focus:ring-2 focus:ring-blue-500 focus:border-blue-500" data-expected="${escapeHtml(exp)}">
                        <option value="-1">(skip - leave empty)</option>
                        ${csvHeaders.map((h, i) =>
                            `<option value="${i}" ${i === suggested ? 'selected' : ''}>${escapeHtml(h)}</option>`
                        ).join('')}
                    </select>
                    ${suggested >= 0 ? '<span class="text-green-500 text-sm">✓</span>' : '<span class="text-gray-300 text-sm">–</span>'}
                </div>`;
            }).join('')}
        </div>
    `;
}

// Collect current mapping from UI
function collectMapping() {
    const mapping = {};
    document.querySelectorAll('.mapping-select').forEach(select => {
        const expected = select.dataset.expected;
        const csvIdx = parseInt(select.value, 10);
        if (csvIdx >= 0) {
            mapping[expected] = csvIdx;
        }
    });
    return Object.keys(mapping).length > 0 ? mapping : null;
}

async function renderPreview(tableLabel, tableKey, expected, uniqueKey, actual, rows, allRows, totalRows, fileName) {
    // Store CSV headers for template saving
    currentPreviewCSVHeaders = actual;

    const columnsMatch = expected.length === actual.length &&
        expected.every((col, i) => col.toLowerCase() === actual[i].toLowerCase());

    let columnSection;
    let templateSection = '';
    let showSaveButton = false;

    if (columnsMatch) {
        // Columns match - show simple green checkmark
        columnSection = `
            <div class="mb-4 p-3 rounded-lg bg-green-50 border border-green-200 dark:bg-green-900/20 dark:border-green-800">
                <div class="flex items-center gap-2 mb-2">
                    <svg class="w-5 h-5 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
                    </svg>
                    <span class="text-sm font-medium text-green-800 dark:text-green-200">Columns match</span>
                </div>
                <div class="grid grid-cols-2 gap-4 text-xs">
                    <div>
                        <div class="font-medium text-gray-600 dark:text-gray-400 mb-1">Expected (${expected.length})</div>
                        ${expected.map(c => `<div class="text-gray-700 dark:text-gray-300">${escapeHtml(c)}</div>`).join('')}
                    </div>
                    <div>
                        <div class="font-medium text-gray-600 dark:text-gray-400 mb-1">Found in CSV (${actual.length})</div>
                        ${actual.map(c => `<div class="text-gray-700 dark:text-gray-300">${escapeHtml(c)}</div>`).join('')}
                    </div>
                </div>
            </div>
        `;
    } else {
        // Columns don't match - show mapping UI
        showSaveButton = true;

        // Fetch matching templates
        const matches = await fetchMatchingTemplates(tableKey, actual);
        templateSection = renderTemplateSelector(matches);

        // Determine initial mapping: from best template (90%+) or auto-suggest
        let initialMapping;
        const bestMatch = matches.find(m => m.matchScore >= 0.9);
        if (bestMatch) {
            initialMapping = bestMatch.template.columnMapping;
            // Auto-select the best template in dropdown after render
            setTimeout(() => {
                const selector = document.getElementById('template-selector');
                if (selector) {
                    selector.value = bestMatch.template.id;
                }
            }, 0);
        } else {
            initialMapping = buildAutoMapping(expected, actual);
        }

        columnSection = `
            <div class="mb-4 p-3 rounded-lg bg-yellow-50 border border-yellow-200 dark:bg-yellow-900/20 dark:border-yellow-800">
                <div class="flex items-center gap-2 mb-3">
                    <svg class="w-5 h-5 text-yellow-600 dark:text-yellow-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path>
                    </svg>
                    <span class="text-sm font-medium text-yellow-800 dark:text-yellow-200">Column mismatch - map columns below</span>
                </div>
                ${renderMappingUI(expected, actual, initialMapping)}
            </div>
        `;
    }

    // Show save template button if mapping UI is visible
    const saveTemplateContainer = document.getElementById('save-template-container');
    if (saveTemplateContainer) {
        saveTemplateContainer.innerHTML = showSaveButton ? renderSaveTemplateButton() : '';
    }

    // Detect CSV duplicates
    let csvDuplicatesSection = '';
    let dbDuplicatesSection = '';
    const hasUniqueKey = uniqueKey && uniqueKey.length > 0;

    if (hasUniqueKey) {
        const { duplicates, keyColsMissing } = detectCSVDuplicates(actual, allRows, uniqueKey);
        if (!keyColsMissing) {
            csvDuplicatesSection = formatCSVDuplicatesWarning(duplicates, uniqueKey);
            // Show loading state for DB check
            dbDuplicatesSection = formatDBDuplicatesWarning([], uniqueKey, true);
        }
    }

    let html = `
        <div class="mb-4">
            <div class="text-sm text-gray-600 dark:text-gray-400">File: <span class="font-medium text-gray-900 dark:text-white">${escapeHtml(fileName)}</span></div>
            <div class="text-sm text-gray-600 dark:text-gray-400">Table: <span class="font-medium text-gray-900 dark:text-white">${escapeHtml(tableLabel)}</span></div>
            <div class="text-sm text-gray-600 dark:text-gray-400">Rows: <span class="font-medium text-gray-900 dark:text-white">${totalRows}</span></div>
        </div>

        ${templateSection}
        ${columnSection}
        ${csvDuplicatesSection}
        ${dbDuplicatesSection}

        <div class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Preview (first ${rows.length} rows)</div>
        <div class="overflow-x-auto border rounded-lg dark:border-gray-700">
            <table class="min-w-full text-xs">
                <thead class="bg-gray-50 dark:bg-gray-800">
                    <tr>
                        ${actual.map(h => `<th class="px-3 py-2 text-left font-medium text-gray-600 dark:text-gray-400">${escapeHtml(h)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody class="divide-y dark:divide-gray-700">
                    ${rows.map(row => `
                        <tr>
                            ${row.map(cell => `<td class="px-3 py-2 text-gray-700 dark:text-gray-300 whitespace-nowrap">${escapeHtml(cell || '-')}</td>`).join('')}
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>

        <div id="analysis-section">${renderAnalysisLoading()}</div>
    `;

    document.getElementById('preview-content').innerHTML = html;
    showModal('preview-modal');

    // Start async database check if we have unique keys
    if (hasUniqueKey) {
        const keys = extractAllKeys(actual, allRows, uniqueKey);
        if (keys.length > 0) {
            checkDatabaseDuplicates(tableKey, keys).then(result => {
                const section = document.getElementById('db-duplicates-section');
                if (section) {
                    section.outerHTML = formatDBDuplicatesWarning(
                        result.existing,
                        uniqueKey,
                        false,
                        result.error,
                        result.skipped
                    );
                }
            });
        } else {
            // No valid keys to check, remove loading state
            const section = document.getElementById('db-duplicates-section');
            if (section) section.remove();
        }
    }

    // Auto-trigger upload analysis
    triggerAnalysis();
}

function confirmUpload() {
    if (currentPreviewForm) {
        // Collect mapping if present
        const mapping = collectMapping();
        if (mapping) {
            // Add or update hidden input with mapping JSON
            let input = currentPreviewForm.querySelector('input[name="mapping"]');
            if (!input) {
                input = document.createElement('input');
                input.type = 'hidden';
                input.name = 'mapping';
                currentPreviewForm.appendChild(input);
            }
            input.value = JSON.stringify(mapping);
        }

        hideModal('preview-modal');
        // Clear save template container
        const saveTemplateContainer = document.getElementById('save-template-container');
        if (saveTemplateContainer) saveTemplateContainer.innerHTML = '';

        htmx.trigger(currentPreviewForm, 'upload');
        currentPreviewForm = null;
        // Reset analysis state
        currentPreviewFile = null;
        currentPreviewTableKey = null;
        currentPreviewMapping = null;
        currentPreviewCSVHeaders = [];
    }
}

function cancelPreview() {
    hideModal('preview-modal');
    // Clear save template container
    const saveTemplateContainer = document.getElementById('save-template-container');
    if (saveTemplateContainer) saveTemplateContainer.innerHTML = '';

    if (currentPreviewForm) {
        const input = currentPreviewForm.querySelector('input[type="file"]');
        if (input) input.value = '';
        // Remove mapping input if present
        const mappingInput = currentPreviewForm.querySelector('input[name="mapping"]');
        if (mappingInput) mappingInput.remove();
        currentPreviewForm = null;
    }
    // Reset analysis state
    currentPreviewFile = null;
    currentPreviewTableKey = null;
    currentPreviewMapping = null;
    currentPreviewCSVHeaders = [];
}

// Close preview modal on outside click
document.addEventListener('click', function(e) {
    const modal = document.getElementById('preview-modal');
    if (modal && e.target === modal) {
        cancelPreview();
    }
});

// ============================================================================
// Import Templates
// ============================================================================

// Fetch templates for a table
async function fetchTemplates(tableKey) {
    try {
        const response = await fetch(`/api/import-templates/${encodeURIComponent(tableKey)}`);
        if (!response.ok) return [];
        return await response.json();
    } catch (e) {
        console.error('Failed to fetch templates:', e);
        return [];
    }
}

// Fetch matching templates based on CSV headers
async function fetchMatchingTemplates(tableKey, csvHeaders) {
    try {
        const headersParam = csvHeaders.map(h => encodeURIComponent(h)).join(',');
        const response = await fetch(`/api/import-templates/${encodeURIComponent(tableKey)}/match?headers=${headersParam}`);
        if (!response.ok) return [];
        const result = await response.json();
        return Array.isArray(result) ? result : [];
    } catch (e) {
        console.error('Failed to fetch matching templates:', e);
        return [];
    }
}

// Render template selector dropdown
function renderTemplateSelector(matches) {
    if (!matches || matches.length === 0) {
        return '';
    }

    const hasHighMatch = matches.some(m => m.matchScore >= 0.9);

    return `
        <div class="mb-4 p-3 rounded-lg bg-blue-50 border border-blue-200 dark:bg-blue-900/20 dark:border-blue-800">
            <div class="flex items-center gap-2 mb-2">
                <svg class="w-5 h-5 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"></path>
                </svg>
                <span class="text-sm font-medium text-blue-800 dark:text-blue-200">
                    ${hasHighMatch ? 'Suggested template found' : 'Available templates'}
                </span>
            </div>
            <select id="template-selector" onchange="applySelectedTemplate()" class="w-full text-sm border border-blue-300 rounded px-2 py-1.5 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:border-blue-600 dark:text-white">
                <option value="">-- Select a template --</option>
                ${matches.map(m => {
                    const pct = Math.round(m.matchScore * 100);
                    const label = pct >= 90 ? ` (${pct}% match)` : ` (${pct}%)`;
                    return `<option value="${escapeHtml(m.template.id)}" data-mapping='${JSON.stringify(m.template.columnMapping)}'>${escapeHtml(m.template.name)}${label}</option>`;
                }).join('')}
            </select>
        </div>
    `;
}

// Apply selected template to mapping dropdowns
function applySelectedTemplate() {
    const selector = document.getElementById('template-selector');
    if (!selector || !selector.value) return;

    const option = selector.options[selector.selectedIndex];
    const mappingStr = option.dataset.mapping;
    if (!mappingStr) return;

    try {
        const mapping = JSON.parse(mappingStr);
        // Apply mapping to each dropdown
        document.querySelectorAll('.mapping-select').forEach(select => {
            const expected = select.dataset.expected;
            if (mapping.hasOwnProperty(expected)) {
                select.value = mapping[expected];
            } else {
                select.value = -1;
            }
        });
    } catch (e) {
        console.error('Failed to apply template:', e);
    }
}

// Show save template modal
function showSaveTemplateModal() {
    document.getElementById('template-name-input').value = '';
    showModal('save-template-modal');
    document.getElementById('template-name-input').focus();
}

// Hide save template modal
function hideSaveTemplateModal() {
    hideModal('save-template-modal');
}

// Save current mapping as a new template
async function saveTemplate() {
    const nameInput = document.getElementById('template-name-input');
    const name = nameInput.value.trim();

    if (!name) {
        showToast('Please enter a template name', true);
        nameInput.focus();
        return;
    }

    if (!currentPreviewTableKey) {
        showToast('No table context available', true);
        return;
    }

    const mapping = collectMapping();
    if (!mapping || Object.keys(mapping).length === 0) {
        showToast('No column mapping to save', true);
        return;
    }

    try {
        const response = await fetch('/api/import-template', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                tableKey: currentPreviewTableKey,
                name: name,
                columnMapping: mapping,
                csvHeaders: currentPreviewCSVHeaders
            })
        });

        if (response.status === 409) {
            showToast('A template with this name already exists', true);
            return;
        }

        if (!response.ok) {
            const data = await response.json();
            showToast(data.error || 'Failed to save template', true);
            return;
        }

        hideSaveTemplateModal();
        showToast('Template saved successfully');
    } catch (e) {
        console.error('Failed to save template:', e);
        showToast('Failed to save template', true);
    }
}

// Render save template button
function renderSaveTemplateButton() {
    return `
        <button onclick="showSaveTemplateModal()" class="flex items-center gap-2 px-3 py-2 text-sm font-medium text-green-700 bg-green-50 border border-green-200 rounded-md hover:bg-green-100 dark:bg-green-900/20 dark:text-green-400 dark:border-green-800 dark:hover:bg-green-900/40">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7H5a2 2 0 00-2 2v9a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-3m-1 4l-3 3m0 0l-3-3m3 3V4"></path>
            </svg>
            Save as Template
        </button>
    `;
}

// Show templates management modal
let currentTemplatesTableKey = null;

async function showTemplatesModal(tableKey) {
    currentTemplatesTableKey = tableKey;
    showModal('templates-modal');
    await loadTemplatesList();
}

function hideTemplatesModal() {
    hideModal('templates-modal');
    currentTemplatesTableKey = null;
}

// Load and render templates list
async function loadTemplatesList() {
    const container = document.getElementById('templates-modal-content');
    if (!currentTemplatesTableKey) return;

    try {
        const templates = await fetchTemplates(currentTemplatesTableKey);

        if (templates.length === 0) {
            container.innerHTML = `
                <div class="text-center py-8">
                    <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7v8a2 2 0 002 2h6M8 7V5a2 2 0 012-2h4.586a1 1 0 01.707.293l4.414 4.414a1 1 0 01.293.707V15a2 2 0 01-2 2h-2M8 7H6a2 2 0 00-2 2v10a2 2 0 002 2h8a2 2 0 002-2v-2"></path>
                    </svg>
                    <h3 class="mt-2 text-sm font-medium text-gray-900 dark:text-white">No templates yet</h3>
                    <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">
                        Templates are created when you upload a CSV with column mapping.<br/>
                        Use "Save as Template" to save your mappings for reuse.
                    </p>
                </div>
            `;
            return;
        }

        container.innerHTML = `
            <div class="space-y-2">
                ${templates.map(t => `
                    <div class="flex items-center justify-between p-3 bg-gray-50 rounded-lg dark:bg-gray-700/50">
                        <div class="flex-1 min-w-0">
                            <div class="font-medium text-gray-900 dark:text-white truncate">${escapeHtml(t.name)}</div>
                            <div class="text-xs text-gray-500 dark:text-gray-400">
                                ${Object.keys(t.columnMapping).length} columns mapped • Updated ${formatRelativeTime(t.updatedAt)}
                            </div>
                        </div>
                        <div class="flex items-center gap-2 ml-4">
                            <button
                                onclick="deleteTemplateFromModal('${escapeHtml(t.id)}', '${escapeHtml(t.name)}')"
                                class="p-1.5 text-red-600 hover:bg-red-100 rounded dark:text-red-400 dark:hover:bg-red-900/30"
                                title="Delete template"
                            >
                                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path>
                                </svg>
                            </button>
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    } catch (e) {
        console.error('Failed to load templates:', e);
        container.innerHTML = `
            <div class="text-center py-8 text-red-600 dark:text-red-400">
                Failed to load templates. Please try again.
            </div>
        `;
    }
}

// Delete template from management modal
async function deleteTemplateFromModal(id, name) {
    if (!confirm(`Delete template "${name}"?`)) return;

    try {
        const response = await fetch(`/api/import-template/${encodeURIComponent(id)}`, {
            method: 'DELETE'
        });

        if (!response.ok) {
            throw new Error('Failed to delete');
        }

        showToast('Template deleted');
        await loadTemplatesList();
    } catch (e) {
        console.error('Failed to delete template:', e);
        showToast('Failed to delete template', true);
    }
}

// Format relative time (e.g., "2 days ago")
function formatRelativeTime(dateStr) {
    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now - date;
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

    if (diffDays === 0) return 'today';
    if (diffDays === 1) return 'yesterday';
    if (diffDays < 7) return `${diffDays} days ago`;
    if (diffDays < 30) return `${Math.floor(diffDays / 7)} weeks ago`;
    if (diffDays < 365) return `${Math.floor(diffDays / 30)} months ago`;
    return `${Math.floor(diffDays / 365)} years ago`;
}

// ============================================================================
// Duplicate Detection
// ============================================================================

// Extract unique key value from a row
function extractKeyValue(row, headers, uniqueKey) {
    if (!uniqueKey || uniqueKey.length === 0) return null;

    const keyParts = [];
    for (const keyCol of uniqueKey) {
        const idx = headers.findIndex(h => h.toLowerCase() === keyCol.toLowerCase());
        if (idx === -1) return null; // Key column not found in CSV
        const val = row[idx] || '';
        if (!val.trim()) return null; // Empty key value - skip
        keyParts.push(val.trim());
    }
    return keyParts.join('|');
}

// Detect duplicates within CSV rows
function detectCSVDuplicates(headers, allRows, uniqueKey) {
    if (!uniqueKey || uniqueKey.length === 0) {
        return { duplicates: [], keyColsMissing: false };
    }

    // Check if key columns exist in headers
    const missingCols = uniqueKey.filter(k =>
        !headers.some(h => h.toLowerCase() === k.toLowerCase())
    );
    if (missingCols.length > 0) {
        return { duplicates: [], keyColsMissing: true };
    }

    const seen = new Map(); // key -> first line number
    const duplicates = [];  // { key, lines: [lineNumber, ...] }

    for (const { data, lineNumber } of allRows) {
        const key = extractKeyValue(data, headers, uniqueKey);
        if (!key) continue;

        if (seen.has(key)) {
            // Find or create duplicate entry
            let entry = duplicates.find(d => d.key === key);
            if (!entry) {
                entry = { key, lines: [seen.get(key)] };
                duplicates.push(entry);
            }
            entry.lines.push(lineNumber);
        } else {
            seen.set(key, lineNumber);
        }
    }

    return { duplicates, keyColsMissing: false };
}

// Extract all unique keys from CSV rows
function extractAllKeys(headers, allRows, uniqueKey) {
    if (!uniqueKey || uniqueKey.length === 0) return [];

    const keys = new Set();
    for (const { data } of allRows) {
        const key = extractKeyValue(data, headers, uniqueKey);
        if (key) keys.add(key);
    }
    return Array.from(keys);
}

// Check for existing records in database
async function checkDatabaseDuplicates(tableKey, keys) {
    if (!keys || keys.length === 0) return { existing: [], count: 0 };

    // Skip for large files (>10k unique keys)
    if (keys.length > 10000) {
        return { existing: [], count: 0, skipped: true };
    }

    try {
        const response = await fetch(`/api/check-duplicates/${tableKey}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ keys })
        });

        if (!response.ok) {
            console.error('Duplicate check failed:', response.status);
            return { existing: [], count: 0, error: true };
        }

        const data = await response.json();
        return { existing: data.existing || [], count: data.count || 0 };
    } catch (e) {
        console.error('Duplicate check error:', e);
        return { existing: [], count: 0, error: true };
    }
}

// Format CSV duplicates warning HTML
function formatCSVDuplicatesWarning(duplicates, uniqueKey) {
    if (!duplicates || duplicates.length === 0) return '';

    const totalDupes = duplicates.reduce((sum, d) => sum + d.lines.length - 1, 0);
    const keyLabel = uniqueKey.length > 1 ? uniqueKey.join(' + ') : uniqueKey[0];

    let html = `
        <div class="mb-4 p-3 rounded-lg bg-amber-50 border border-amber-200">
            <div class="flex items-center gap-2 mb-2">
                <svg class="w-5 h-5 text-amber-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path>
                </svg>
                <span class="text-sm font-medium text-amber-800">${totalDupes} duplicate${totalDupes === 1 ? '' : 's'} within CSV (by ${escapeHtml(keyLabel)})</span>
            </div>
            <div class="text-xs text-amber-700 space-y-1 max-h-24 overflow-y-auto">
    `;

    // Show first 5 duplicates
    const shown = duplicates.slice(0, 5);
    for (const d of shown) {
        html += `<div>"<span class="font-medium">${escapeHtml(d.key)}</span>" on rows ${d.lines.join(', ')}</div>`;
    }
    if (duplicates.length > 5) {
        html += `<div class="text-amber-600 italic">...and ${duplicates.length - 5} more</div>`;
    }

    html += '</div></div>';
    return html;
}

// Format database duplicates warning HTML
function formatDBDuplicatesWarning(existing, uniqueKey, loading = false, error = false, skipped = false) {
    if (loading) {
        return `
            <div id="db-duplicates-section" class="mb-4 p-3 rounded-lg bg-gray-50 border border-gray-200">
                <div class="flex items-center gap-2">
                    <svg class="w-5 h-5 text-gray-400 animate-spin" fill="none" viewBox="0 0 24 24">
                        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
                    <span class="text-sm text-gray-600">Checking for existing records in database...</span>
                </div>
            </div>
        `;
    }

    if (error) {
        return `
            <div id="db-duplicates-section" class="mb-4 p-3 rounded-lg bg-gray-50 border border-gray-200">
                <div class="flex items-center gap-2">
                    <svg class="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                    <span class="text-sm text-gray-500">Could not check database for duplicates</span>
                </div>
            </div>
        `;
    }

    if (skipped) {
        return `
            <div id="db-duplicates-section" class="mb-4 p-3 rounded-lg bg-gray-50 border border-gray-200">
                <div class="flex items-center gap-2">
                    <svg class="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                    <span class="text-sm text-gray-500">Large file - database duplicate check skipped</span>
                </div>
            </div>
        `;
    }

    if (!existing || existing.length === 0) {
        return `
            <div id="db-duplicates-section" class="mb-4 p-3 rounded-lg bg-green-50 border border-green-200">
                <div class="flex items-center gap-2">
                    <svg class="w-5 h-5 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
                    </svg>
                    <span class="text-sm font-medium text-green-800">No existing records found in database</span>
                </div>
            </div>
        `;
    }

    const keyLabel = uniqueKey.length > 1 ? uniqueKey.join(' + ') : uniqueKey[0];

    let html = `
        <div id="db-duplicates-section" class="mb-4 p-3 rounded-lg bg-red-50 border border-red-200">
            <div class="flex items-center gap-2 mb-2">
                <svg class="w-5 h-5 text-red-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path>
                </svg>
                <span class="text-sm font-medium text-red-800">${existing.length} record${existing.length === 1 ? '' : 's'} already in database</span>
            </div>
            <div class="text-xs text-red-700 mb-2">These will create duplicates or be skipped (by ${escapeHtml(keyLabel)}):</div>
            <div class="text-xs text-red-600 space-y-0.5 max-h-24 overflow-y-auto font-mono">
    `;

    // Show first 8 existing keys
    const shown = existing.slice(0, 8);
    for (const key of shown) {
        html += `<div>${escapeHtml(key)}</div>`;
    }
    if (existing.length > 8) {
        html += `<div class="text-red-500 italic font-sans">...and ${existing.length - 8} more</div>`;
    }

    html += '</div></div>';
    return html;
}

// ============================================================================
// Upload Preview Analysis
// ============================================================================

// State for current preview
let currentPreviewFile = null;
let currentPreviewTableKey = null;
let currentPreviewMapping = null;

// Analyze upload and show detailed preview
async function analyzeUpload(tableKey, file, mapping) {
    const formData = new FormData();
    formData.append('file', file);
    if (mapping) {
        formData.append('mapping', JSON.stringify(mapping));
    }

    try {
        const response = await fetch(`/api/preview/${tableKey}`, {
            method: 'POST',
            body: formData
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'Analysis failed');
        }

        return await response.json();
    } catch (e) {
        console.error('Analysis error:', e);
        throw e;
    }
}

// Trigger analysis after column mapping is set
function triggerAnalysis() {
    if (!currentPreviewFile || !currentPreviewTableKey) return;

    const mapping = collectMapping();
    currentPreviewMapping = mapping;

    // Show loading state
    const analysisSection = document.getElementById('analysis-section');
    if (analysisSection) {
        analysisSection.innerHTML = renderAnalysisLoading();
    }

    analyzeUpload(currentPreviewTableKey, currentPreviewFile, mapping)
        .then(result => {
            if (analysisSection) {
                analysisSection.innerHTML = renderAnalysisResults(result);
                setupAnalysisTabs();
            }
        })
        .catch(err => {
            if (analysisSection) {
                analysisSection.innerHTML = renderAnalysisError(err.message);
            }
        });
}

// Render loading spinner for analysis
function renderAnalysisLoading() {
    return `
        <div class="mt-4 p-4 rounded-lg bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700">
            <div class="flex items-center justify-center gap-3">
                <svg class="w-5 h-5 text-blue-500 animate-spin" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                <span class="text-sm text-gray-600 dark:text-gray-400">Analyzing upload...</span>
            </div>
        </div>
    `;
}

// Render analysis error
function renderAnalysisError(message) {
    return `
        <div class="mt-4 p-4 rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800">
            <div class="flex items-center gap-2">
                <svg class="w-5 h-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                </svg>
                <span class="text-sm text-red-700 dark:text-red-400">Analysis failed: ${escapeHtml(message)}</span>
            </div>
        </div>
    `;
}

// Render complete analysis results
function renderAnalysisResults(result) {
    const { summary, newRowSamples, updateDiffs, errorSamples, duplicateSamples, processingTimeMs } = result;

    // Summary cards
    const summaryHtml = `
        <div class="grid grid-cols-4 gap-3 mb-4">
            <div class="p-3 rounded-lg bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 text-center">
                <div class="text-2xl font-bold text-green-600 dark:text-green-400">${summary.newRows}</div>
                <div class="text-xs text-green-700 dark:text-green-500">New</div>
            </div>
            <div class="p-3 rounded-lg bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 text-center">
                <div class="text-2xl font-bold text-blue-600 dark:text-blue-400">${summary.updateRows}</div>
                <div class="text-xs text-blue-700 dark:text-blue-500">Updates</div>
            </div>
            <div class="p-3 rounded-lg ${summary.errorRows > 0 ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800' : 'bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700'} border text-center">
                <div class="text-2xl font-bold ${summary.errorRows > 0 ? 'text-red-600 dark:text-red-400' : 'text-gray-400 dark:text-gray-500'}">${summary.errorRows}</div>
                <div class="text-xs ${summary.errorRows > 0 ? 'text-red-700 dark:text-red-500' : 'text-gray-500 dark:text-gray-400'}">Errors</div>
            </div>
            <div class="p-3 rounded-lg ${summary.duplicateInFile > 0 ? 'bg-amber-50 dark:bg-amber-900/20 border-amber-200 dark:border-amber-800' : 'bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700'} border text-center">
                <div class="text-2xl font-bold ${summary.duplicateInFile > 0 ? 'text-amber-600 dark:text-amber-400' : 'text-gray-400 dark:text-gray-500'}">${summary.duplicateInFile}</div>
                <div class="text-xs ${summary.duplicateInFile > 0 ? 'text-amber-700 dark:text-amber-500' : 'text-gray-500 dark:text-gray-400'}">Duplicates</div>
            </div>
        </div>
    `;

    // Determine which tabs to show
    const hasUpdates = updateDiffs && updateDiffs.length > 0;
    const hasErrors = errorSamples && errorSamples.length > 0;
    const hasNew = newRowSamples && newRowSamples.length > 0;
    const hasDuplicates = duplicateSamples && duplicateSamples.length > 0;

    // Tabs
    const tabsHtml = `
        <div class="border-b border-gray-200 dark:border-gray-700 mb-3">
            <nav class="flex gap-1 -mb-px" id="analysis-tabs">
                ${hasUpdates ? `<button class="analysis-tab px-3 py-2 text-sm font-medium border-b-2 border-blue-500 text-blue-600 dark:text-blue-400" data-tab="updates">Updates (${updateDiffs.length}${summary.updateRows > updateDiffs.length ? '+' : ''})</button>` : ''}
                ${hasErrors ? `<button class="analysis-tab px-3 py-2 text-sm font-medium border-b-2 border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300" data-tab="errors">Errors (${errorSamples.length}${summary.errorRows > errorSamples.length ? '+' : ''})</button>` : ''}
                ${hasNew ? `<button class="analysis-tab px-3 py-2 text-sm font-medium border-b-2 border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300" data-tab="new">New (${newRowSamples.length}${summary.newRows > newRowSamples.length ? '+' : ''})</button>` : ''}
                ${hasDuplicates ? `<button class="analysis-tab px-3 py-2 text-sm font-medium border-b-2 border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300" data-tab="duplicates">Duplicates</button>` : ''}
            </nav>
        </div>
    `;

    // Tab content
    const updatesContent = hasUpdates ? renderUpdateDiffs(updateDiffs, summary.updateRows) : '';
    const errorsContent = hasErrors ? renderErrorSamples(errorSamples, summary.errorRows) : '';
    const newContent = hasNew ? renderNewRowSamples(newRowSamples, summary.newRows) : '';
    const duplicatesContent = hasDuplicates ? renderDuplicateSamples(duplicateSamples, summary.duplicateInFile) : '';

    // Default to first available tab
    const defaultTab = hasUpdates ? 'updates' : hasErrors ? 'errors' : hasNew ? 'new' : 'duplicates';

    const tabContentHtml = `
        <div id="analysis-tab-content">
            ${hasUpdates ? `<div class="tab-panel ${defaultTab === 'updates' ? '' : 'hidden'}" data-panel="updates">${updatesContent}</div>` : ''}
            ${hasErrors ? `<div class="tab-panel ${defaultTab === 'errors' ? '' : 'hidden'}" data-panel="errors">${errorsContent}</div>` : ''}
            ${hasNew ? `<div class="tab-panel ${defaultTab === 'new' ? '' : 'hidden'}" data-panel="new">${newContent}</div>` : ''}
            ${hasDuplicates ? `<div class="tab-panel ${defaultTab === 'duplicates' ? '' : 'hidden'}" data-panel="duplicates">${duplicatesContent}</div>` : ''}
        </div>
    `;

    // No data message
    const noDataHtml = !hasUpdates && !hasErrors && !hasNew && !hasDuplicates ? `
        <div class="text-center text-gray-500 dark:text-gray-400 py-4">
            <svg class="w-8 h-8 mx-auto mb-2 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
            </svg>
            <div>All ${summary.newRows} rows are new and valid</div>
        </div>
    ` : '';

    const processingTime = processingTimeMs ? `<div class="text-xs text-gray-400 dark:text-gray-500 mt-2 text-right">Analyzed in ${processingTimeMs}ms</div>` : '';

    return `
        <div class="mt-4 p-4 rounded-lg bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700">
            <div class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">Upload Analysis</div>
            ${summaryHtml}
            ${hasUpdates || hasErrors || hasNew || hasDuplicates ? tabsHtml + tabContentHtml : noDataHtml}
            ${processingTime}
        </div>
    `;
}

// Render update diffs table
function renderUpdateDiffs(diffs, totalUpdates) {
    const showingInfo = diffs.length < totalUpdates ? `<div class="text-xs text-gray-500 dark:text-gray-400 mb-2">Showing ${diffs.length} of ${totalUpdates} updates</div>` : '';

    const rows = diffs.map(diff => {
        const changedCols = new Set(diff.changed || []);
        const allCols = Object.keys(diff.incoming);

        return `
            <div class="mb-3 p-2 rounded border border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/50">
                <div class="text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                    Row ${diff.lineNumber} • Key: <span class="font-mono">${escapeHtml(diff.rowKey)}</span>
                </div>
                <table class="w-full text-xs">
                    <thead>
                        <tr class="text-gray-500 dark:text-gray-400">
                            <th class="text-left py-1 pr-2 w-1/4">Column</th>
                            <th class="text-left py-1 pr-2 w-1/3">Current</th>
                            <th class="text-left py-1 w-1/3">Incoming</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${allCols.map(col => {
                            const isChanged = changedCols.has(col);
                            const current = diff.current[col] || '';
                            const incoming = diff.incoming[col] || '';
                            return `
                                <tr class="${isChanged ? 'bg-blue-50 dark:bg-blue-900/20' : ''}">
                                    <td class="py-0.5 pr-2 font-medium ${isChanged ? 'text-blue-700 dark:text-blue-400' : 'text-gray-600 dark:text-gray-400'}">${escapeHtml(col)}</td>
                                    <td class="py-0.5 pr-2 ${isChanged ? 'text-red-600 dark:text-red-400 line-through' : 'text-gray-600 dark:text-gray-400'}">${escapeHtml(current) || '<span class="text-gray-300 dark:text-gray-600">empty</span>'}</td>
                                    <td class="py-0.5 ${isChanged ? 'text-green-600 dark:text-green-400 font-medium' : 'text-gray-600 dark:text-gray-400'}">${escapeHtml(incoming) || '<span class="text-gray-300 dark:text-gray-600">empty</span>'}</td>
                                </tr>
                            `;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }).join('');

    return `
        <div class="max-h-64 overflow-y-auto">
            ${showingInfo}
            ${rows}
        </div>
    `;
}

// Render error samples
function renderErrorSamples(errors, totalErrors) {
    const showingInfo = errors.length < totalErrors ? `<div class="text-xs text-gray-500 dark:text-gray-400 mb-2">Showing ${errors.length} of ${totalErrors} errors</div>` : '';

    const rows = errors.map(err => `
        <div class="mb-2 p-2 rounded border border-red-100 dark:border-red-900 bg-red-50 dark:bg-red-900/20">
            <div class="text-xs font-medium text-red-700 dark:text-red-400 mb-1">
                Row ${err.lineNumber}${err.rowKey ? ` • Key: <span class="font-mono">${escapeHtml(err.rowKey)}</span>` : ''}
            </div>
            <ul class="text-xs text-red-600 dark:text-red-400 list-disc list-inside">
                ${err.errors.map(e => `<li>${escapeHtml(e)}</li>`).join('')}
            </ul>
        </div>
    `).join('');

    return `
        <div class="max-h-64 overflow-y-auto">
            ${showingInfo}
            ${rows}
        </div>
    `;
}

// Render new row samples
function renderNewRowSamples(samples, totalNew) {
    const showingInfo = samples.length < totalNew ? `<div class="text-xs text-gray-500 dark:text-gray-400 mb-2">Showing ${samples.length} of ${totalNew} new rows</div>` : '';

    const rows = samples.map(sample => {
        const cols = Object.entries(sample.values);
        return `
            <div class="mb-2 p-2 rounded border border-green-100 dark:border-green-900 bg-green-50 dark:bg-green-900/20">
                <div class="text-xs font-medium text-green-700 dark:text-green-400 mb-1">
                    Row ${sample.lineNumber}${sample.rowKey ? ` • Key: <span class="font-mono">${escapeHtml(sample.rowKey)}</span>` : ''}
                </div>
                <div class="text-xs text-green-600 dark:text-green-400 grid grid-cols-2 gap-x-4 gap-y-0.5">
                    ${cols.slice(0, 6).map(([k, v]) => `<div><span class="text-green-500 dark:text-green-500">${escapeHtml(k)}:</span> ${escapeHtml(v) || '<span class="text-gray-400">empty</span>'}</div>`).join('')}
                    ${cols.length > 6 ? `<div class="text-green-400 dark:text-green-500 italic">+${cols.length - 6} more fields</div>` : ''}
                </div>
            </div>
        `;
    }).join('');

    return `
        <div class="max-h-64 overflow-y-auto">
            ${showingInfo}
            ${rows}
        </div>
    `;
}

// Render duplicate samples
function renderDuplicateSamples(duplicates, totalDuplicates) {
    const rows = duplicates.map(dup => `
        <div class="mb-2 p-2 rounded border border-amber-100 dark:border-amber-900 bg-amber-50 dark:bg-amber-900/20">
            <div class="text-xs">
                <span class="font-medium text-amber-700 dark:text-amber-400 font-mono">${escapeHtml(dup.rowKey)}</span>
                <span class="text-amber-600 dark:text-amber-500"> appears on rows: ${dup.lineNumbers.join(', ')}</span>
            </div>
        </div>
    `).join('');

    return `
        <div class="max-h-64 overflow-y-auto">
            <div class="text-xs text-gray-500 dark:text-gray-400 mb-2">${totalDuplicates} duplicate keys in file (only first occurrence will be imported)</div>
            ${rows}
        </div>
    `;
}

// Setup tab switching
function setupAnalysisTabs() {
    const tabs = document.querySelectorAll('.analysis-tab');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const tabName = tab.dataset.tab;

            // Update tab styles
            tabs.forEach(t => {
                if (t.dataset.tab === tabName) {
                    t.classList.add('border-blue-500', 'text-blue-600', 'dark:text-blue-400');
                    t.classList.remove('border-transparent', 'text-gray-500', 'dark:text-gray-400');
                } else {
                    t.classList.remove('border-blue-500', 'text-blue-600', 'dark:text-blue-400');
                    t.classList.add('border-transparent', 'text-gray-500', 'dark:text-gray-400');
                }
            });

            // Show/hide panels
            document.querySelectorAll('.tab-panel').forEach(panel => {
                if (panel.dataset.panel === tabName) {
                    panel.classList.remove('hidden');
                } else {
                    panel.classList.add('hidden');
                }
            });
        });
    });
}

// ============================================================================
// Row Selection & Bulk Delete
// ============================================================================

// Set to track selected row keys
let selectedRows = new Set();

// Handle "Select All" checkbox toggle
function toggleSelectAll(checkbox) {
    document.querySelectorAll('.row-checkbox').forEach(cb => {
        cb.checked = checkbox.checked;
    });
    updateSelectionFromCheckboxes();
    updateSelectionUI();
}

// Handle individual row checkbox change
function updateSelection() {
    updateSelectionFromCheckboxes();
    updateSelectionUI();

    // Update "select all" checkbox state
    const allCheckboxes = document.querySelectorAll('.row-checkbox');
    const checkedCheckboxes = document.querySelectorAll('.row-checkbox:checked');
    const selectAll = document.getElementById('select-all');
    if (selectAll) {
        selectAll.checked = allCheckboxes.length > 0 && allCheckboxes.length === checkedCheckboxes.length;
        selectAll.indeterminate = checkedCheckboxes.length > 0 && checkedCheckboxes.length < allCheckboxes.length;
    }
}

// Sync selectedRows Set with current checkbox states
function updateSelectionFromCheckboxes() {
    selectedRows.clear();
    document.querySelectorAll('.row-checkbox:checked').forEach(cb => {
        const key = cb.dataset.key;
        if (key) selectedRows.add(key);
    });
}

// Update the selection bar visibility and count
function updateSelectionUI() {
    const count = selectedRows.size;
    const bar = document.getElementById('selection-bar');
    const countSpan = document.getElementById('selection-count');

    if (!bar) return;

    if (count > 0) {
        // Use inline style to override any CSS class conflicts (hidden + flex)
        bar.style.display = 'flex';
        if (countSpan) {
            countSpan.textContent = `${count} row${count !== 1 ? 's' : ''} selected`;
        }
    } else {
        bar.style.display = 'none';
    }
}

// Show delete confirmation modal
function showDeleteModal() {
    const deleteCount = document.getElementById('delete-count');
    if (deleteCount) {
        deleteCount.textContent = selectedRows.size;
    }
    showModal('delete-modal');
}

// Hide delete confirmation modal
function hideDeleteModal() {
    hideModal('delete-modal');
}

// Perform the delete operation
async function confirmDelete() {
    const tableKey = getTableKey();
    if (!tableKey || selectedRows.size === 0) {
        hideDeleteModal();
        return;
    }

    const keys = Array.from(selectedRows);

    try {
        const response = await fetch(`/api/delete/${tableKey}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ keys })
        });

        if (response.ok) {
            const result = await response.json();
            showToast(`Deleted ${result.deleted} row${result.deleted !== 1 ? 's' : ''}`);
            hideDeleteModal();
            selectedRows.clear();
            updateSelectionUI();

            // Refresh the table via HTMX
            const container = document.getElementById('table-container');
            if (container) {
                htmx.trigger(container, 'htmx:load');
                // Reload current page
                const url = new URL(window.location.href);
                htmx.ajax('GET', url.pathname + url.search, { target: '#table-container', swap: 'innerHTML' });
            }
        } else {
            const err = await response.json();
            showToast(err.error || 'Delete failed', true);
        }
    } catch (e) {
        console.error('Delete error:', e);
        showToast('Delete failed', true);
    }
}

// Close delete modal on outside click
document.addEventListener('click', function(e) {
    const modal = document.getElementById('delete-modal');
    if (modal && e.target === modal) {
        hideDeleteModal();
    }
});

// Clear selection on page change (HTMX navigation)
document.body.addEventListener('htmx:afterSwap', function(e) {
    if (e.detail.target.id === 'table-container') {
        selectedRows.clear();
        updateSelectionUI();
        // Reset select-all checkbox
        const selectAll = document.getElementById('select-all');
        if (selectAll) {
            selectAll.checked = false;
            selectAll.indeterminate = false;
        }
    }
});

// Close column dropdown helper (for keyboard shortcuts)
function closeColumnDropdown() {
    const dropdown = document.getElementById('column-dropdown');
    if (dropdown) dropdown.classList.add('hidden');
}

// ============================================================================
// Inline Cell Editing
// ============================================================================

let currentEditCell = null;
let originalCellHTML = null;
let columnsMeta = null;

// Initialize inline editing
function initInlineEditing() {
    // Parse column metadata from data attribute
    const container = document.getElementById('table-container');
    if (container && container.dataset.columnsMeta) {
        try {
            columnsMeta = JSON.parse(container.dataset.columnsMeta);
        } catch (e) {
            console.error('Failed to parse columns meta:', e);
            columnsMeta = [];
        }
    }

    // Add double-click handler for editable cells
    document.addEventListener('dblclick', handleCellDoubleClick);
}

// Get column metadata by name
function getColumnMeta(colName) {
    if (!columnsMeta) return null;
    return columnsMeta.find(m => m.name === colName);
}

// Handle double-click on a cell
function handleCellDoubleClick(event) {
    const cell = event.target.closest('td.editable-cell');
    if (!cell || currentEditCell) return;

    // Don't edit if clicking on existing input/button
    if (event.target.matches('input, button, select')) return;

    startEdit(cell);
}

// Start editing a cell
function startEdit(cell) {
    const row = cell.closest('tr');
    const rowKey = row.dataset.rowKey;
    const colName = cell.dataset.colName;
    const rawValue = cell.dataset.rawValue || '';

    if (!rowKey || !colName) return;

    const meta = getColumnMeta(colName);
    if (!meta) {
        // Fallback to text type
        meta = { type: 'text', allowEmpty: true };
    }

    currentEditCell = cell;
    originalCellHTML = cell.innerHTML;

    // Build edit UI based on column type
    const inputHTML = buildEditInput(meta, rawValue);
    const isKeyColumn = meta.isUniqueKey;

    cell.innerHTML = `
        <div class="flex items-center gap-1">
            ${inputHTML}
            <button type="button" onclick="saveEdit()" class="p-1 text-green-600 hover:text-green-800" title="Save (Enter)">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
                </svg>
            </button>
            <button type="button" onclick="cancelEdit()" class="p-1 text-red-600 hover:text-red-800" title="Cancel (Esc)">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                </svg>
            </button>
        </div>
        ${isKeyColumn ? '<div class="text-xs text-amber-600 mt-1">Warning: Editing unique key</div>' : ''}
    `;

    // Remove truncate class and add min-width
    cell.classList.remove('truncate', 'max-w-xs');
    cell.style.minWidth = '200px';

    // Focus the input
    const input = cell.querySelector('input, select');
    if (input) {
        input.focus();
        if (input.type !== 'checkbox' && input.select) {
            input.select();
        }
    }
}

// Build input HTML based on column type
function buildEditInput(meta, value) {
    const escapedValue = escapeHtml(value);

    switch (meta.type) {
        case 'numeric':
            return `<input type="number" step="any" value="${escapedValue}"
                    class="edit-input w-full px-2 py-1 text-sm border border-blue-400 rounded focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    onkeydown="handleEditKeydown(event)">`;

        case 'date':
            return `<input type="date" value="${escapedValue}"
                    class="edit-input w-full px-2 py-1 text-sm border border-blue-400 rounded focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    onkeydown="handleEditKeydown(event)">`;

        case 'bool':
            return `<select class="edit-input w-full px-2 py-1 text-sm border border-blue-400 rounded focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    onkeydown="handleEditKeydown(event)">
                <option value="" ${value === '' ? 'selected' : ''}>-</option>
                <option value="true" ${value === 'true' ? 'selected' : ''}>Yes</option>
                <option value="false" ${value === 'false' ? 'selected' : ''}>No</option>
            </select>`;

        case 'enum':
            const options = (meta.enumValues || []).map(v =>
                `<option value="${escapeHtml(v)}" ${v === value ? 'selected' : ''}>${escapeHtml(v)}</option>`
            ).join('');
            return `<select class="edit-input w-full px-2 py-1 text-sm border border-blue-400 rounded focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    onkeydown="handleEditKeydown(event)">
                <option value="">-</option>
                ${options}
            </select>`;

        case 'text':
        default:
            return `<input type="text" value="${escapedValue}"
                    class="edit-input w-full px-2 py-1 text-sm border border-blue-400 rounded focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    onkeydown="handleEditKeydown(event)">`;
    }
}

// Handle keyboard events in edit input
function handleEditKeydown(event) {
    if (event.key === 'Enter') {
        event.preventDefault();
        // Store position before save (table refreshes after save)
        const savedPosition = focusedCell ? { ...focusedCell } : null;
        saveEdit();
        // After save, move down one row (spreadsheet behavior)
        if (savedPosition) {
            const { rows } = getTableDimensions();
            const newRow = Math.min(savedPosition.row + 1, rows - 1);
            // Wait for HTMX refresh then focus
            setTimeout(() => {
                const cell = getCellAt(newRow, savedPosition.col);
                if (cell) focusCell(cell);
            }, 150);
        }
    } else if (event.key === 'Escape') {
        event.preventDefault();
        cancelEdit();
        // Restore focus to the cell
        if (focusedElement) {
            setTimeout(() => focusCell(focusedElement), 50);
        }
    } else if (event.key === 'Tab') {
        event.preventDefault();
        // Store position before save
        const savedPosition = focusedCell ? { ...focusedCell } : null;
        saveEdit();
        // Move to next/previous cell and start editing
        if (savedPosition) {
            const { rows, cols } = getTableDimensions();
            let { row, col } = savedPosition;
            if (event.shiftKey) {
                col--;
                if (col < 0) { col = cols - 1; row--; }
            } else {
                col++;
                if (col >= cols) { col = 0; row++; }
            }
            if (row >= 0 && row < rows) {
                setTimeout(() => {
                    const cell = getCellAt(row, col);
                    if (cell) {
                        focusCell(cell);
                        startEdit(cell);
                    }
                }, 150);
            }
        }
    }
}

// Save the edited value
async function saveEdit() {
    if (!currentEditCell) return;

    const row = currentEditCell.closest('tr');
    const rowKey = row.dataset.rowKey;
    const colName = currentEditCell.dataset.colName;
    const input = currentEditCell.querySelector('.edit-input');
    const newValue = input ? input.value : '';

    const tableKey = getTableKey();
    if (!tableKey) {
        cancelEdit();
        return;
    }

    // Show loading state
    const buttons = currentEditCell.querySelectorAll('button');
    buttons.forEach(b => b.disabled = true);
    input.disabled = true;

    try {
        const response = await fetch(`/api/update/${tableKey}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                rowKey: rowKey,
                column: colName,
                value: newValue
            })
        });

        const result = await response.json();

        if (result.success) {
            showToast('Cell updated');
            // Refresh the table to show updated value
            const url = new URL(window.location.href);
            htmx.ajax('GET', url.pathname + url.search, { target: '#table-container', swap: 'innerHTML' });
        } else if (result.duplicateKey) {
            showToast(`Duplicate key: ${result.conflictingKey || 'value already exists'}`, true);
            // Re-enable editing
            buttons.forEach(b => b.disabled = false);
            input.disabled = false;
            input.focus();
        } else if (result.validationError) {
            showToast(`Validation error: ${result.validationError}`, true);
            // Re-enable editing
            buttons.forEach(b => b.disabled = false);
            input.disabled = false;
            input.focus();
        } else if (result.error) {
            showToast(result.error, true);
            cancelEdit();
        } else {
            showToast('Update failed', true);
            cancelEdit();
        }
    } catch (e) {
        console.error('Update error:', e);
        showToast('Update failed', true);
        cancelEdit();
    }
}

// Cancel editing and restore original cell
function cancelEdit() {
    if (!currentEditCell) return;

    currentEditCell.innerHTML = originalCellHTML;
    currentEditCell.classList.add('truncate', 'max-w-xs');
    currentEditCell.style.minWidth = '';

    currentEditCell = null;
    originalCellHTML = null;
}

// Clear edit state on table refresh and save focus position
document.body.addEventListener('htmx:beforeSwap', function(e) {
    if (e.detail.target.id === 'table-container') {
        // Clear edit state before swap
        currentEditCell = null;
        originalCellHTML = null;

        // Save focus position by row key and column name for restoration
        if (focusedElement) {
            const row = focusedElement.closest('tr');
            window._savedTableFocus = {
                rowKey: row?.dataset.rowKey,
                colName: focusedElement.dataset.colName
            };
        }
    }
});

// Re-initialize inline editing after HTMX swap and restore focus
document.body.addEventListener('htmx:afterSwap', function(e) {
    if (e.detail.target.id === 'table-container') {
        // Re-parse column metadata
        initInlineEditing();

        // Restore focus if we saved a position
        if (window._savedTableFocus) {
            const { rowKey, colName } = window._savedTableFocus;
            window._savedTableFocus = null;

            // Find cell by row key and column name
            const row = document.querySelector(`tr[data-row-key="${CSS.escape(rowKey)}"]`);
            if (row) {
                const cell = row.querySelector(`td[data-col-name="${colName}"]`);
                if (cell) {
                    focusCell(cell);
                    return;
                }
            }
            // Row might have been deleted or moved to different page - clear focus
            focusCell(null);
        }
    }
});

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    initInlineEditing();
});

// ============================================================================
// Column Filters
// ============================================================================

let activeFilterDropdown = null;

// Convert column name to valid HTML ID
function sanitizeID(col) {
    return col.toLowerCase().replace(/\s+/g, '-');
}

// Initialize filter event handlers via event delegation
function initFilterHandlers() {
    // Filter toggle button click handler
    document.addEventListener('click', function(e) {
        const toggleBtn = e.target.closest('.filter-toggle-btn');
        if (toggleBtn) {
            e.stopPropagation();
            const colName = toggleBtn.dataset.col;
            if (!colName) return;

            const dropdownId = 'filter-dropdown-' + sanitizeID(colName);
            const dropdown = document.getElementById(dropdownId);
            if (!dropdown) return;

            // Close any other open dropdown
            if (activeFilterDropdown && activeFilterDropdown !== dropdown) {
                activeFilterDropdown.classList.add('hidden');
            }

            // Toggle this dropdown
            dropdown.classList.toggle('hidden');
            activeFilterDropdown = dropdown.classList.contains('hidden') ? null : dropdown;
            return;
        }

        // Apply button click handler
        const applyBtn = e.target.closest('.filter-apply-btn');
        if (applyBtn) {
            const container = applyBtn.closest('[data-filter-type]');
            if (container) {
                applyFilter(container);
            }
            return;
        }

        // Clear button click handler
        const clearBtn = e.target.closest('.filter-clear-btn');
        if (clearBtn) {
            const container = clearBtn.closest('[data-filter-type]');
            if (container) {
                clearFilter(container.dataset.col);
            }
            return;
        }

        // Close dropdown when clicking outside
        if (activeFilterDropdown && !e.target.closest('[id^="filter-dropdown-"]') && !e.target.closest('.filter-toggle-btn')) {
            activeFilterDropdown.classList.add('hidden');
            activeFilterDropdown = null;
        }
    });
}

// Apply filter based on type
function applyFilter(container) {
    const filterType = container.dataset.filterType;
    const colName = container.dataset.col;
    const tableKey = container.dataset.table;

    switch (filterType) {
        case 'text':
            applyTextFilter(container, colName, tableKey);
            break;
        case 'numeric':
            applyNumericFilter(container, colName, tableKey);
            break;
        case 'date':
            applyDateFilter(container, colName, tableKey);
            break;
        case 'bool':
            applyBoolFilter(container, colName, tableKey);
            break;
        case 'enum':
            applyEnumFilter(container, colName, tableKey);
            break;
    }
}

// Apply text filter
function applyTextFilter(container, colName, tableKey) {
    const opSelect = container.querySelector('.filter-op');
    const valInput = container.querySelector('.filter-val');

    if (!opSelect || !valInput) return;

    const op = opSelect.value;
    const val = valInput.value.trim();

    if (!val) {
        showToast('Please enter a filter value', true);
        return;
    }

    navigateWithFilter(colName, op + ':' + val);
}

// Apply numeric filter (min/max range)
function applyNumericFilter(container, colName, tableKey) {
    const minInput = container.querySelector('.filter-min');
    const maxInput = container.querySelector('.filter-max');

    if (!minInput || !maxInput) return;

    const min = minInput.value.trim();
    const max = maxInput.value.trim();

    if (!min && !max) {
        showToast('Please enter a min or max value', true);
        return;
    }

    // Build URL with both filters if provided
    const url = new URL(window.location.href);
    removeFilterParams(url, colName);

    if (min) {
        url.searchParams.append('filter[' + colName + ']', 'gte:' + min);
    }
    if (max) {
        url.searchParams.append('filter[' + colName + ']', 'lte:' + max);
    }

    url.searchParams.set('page', '1');

    htmx.ajax('GET', url.pathname + url.search, {
        target: '#table-container',
        swap: 'innerHTML',
        pushUrl: true
    });

    closeFilterDropdowns();
}

// Apply date filter (from/to range)
function applyDateFilter(container, colName, tableKey) {
    const fromInput = container.querySelector('.filter-from');
    const toInput = container.querySelector('.filter-to');

    if (!fromInput || !toInput) return;

    const from = fromInput.value;
    const to = toInput.value;

    if (!from && !to) {
        showToast('Please enter a from or to date', true);
        return;
    }

    // Build URL with both filters if provided
    const url = new URL(window.location.href);
    removeFilterParams(url, colName);

    if (from) {
        url.searchParams.append('filter[' + colName + ']', 'gte:' + from);
    }
    if (to) {
        url.searchParams.append('filter[' + colName + ']', 'lte:' + to);
    }

    url.searchParams.set('page', '1');

    htmx.ajax('GET', url.pathname + url.search, {
        target: '#table-container',
        swap: 'innerHTML',
        pushUrl: true
    });

    closeFilterDropdowns();
}

// Apply bool filter
function applyBoolFilter(container, colName, tableKey) {
    const radios = container.querySelectorAll('.filter-bool-radio');
    let selectedValue = '';

    radios.forEach(radio => {
        if (radio.checked) selectedValue = radio.value;
    });

    if (!selectedValue) {
        clearFilter(colName);
        return;
    }

    navigateWithFilter(colName, 'eq:' + selectedValue);
}

// Apply enum filter (multiple checkboxes)
function applyEnumFilter(container, colName, tableKey) {
    const checkboxes = container.querySelectorAll('.filter-enum-checkbox:checked');
    const values = Array.from(checkboxes).map(cb => cb.value);

    if (values.length === 0) {
        clearFilter(colName);
        return;
    }

    navigateWithFilter(colName, 'in:' + values.join(','));
}

// Navigate with a new filter applied
function navigateWithFilter(colName, filterValue) {
    const url = new URL(window.location.href);
    removeFilterParams(url, colName);
    url.searchParams.append('filter[' + colName + ']', filterValue);
    url.searchParams.set('page', '1');

    htmx.ajax('GET', url.pathname + url.search, {
        target: '#table-container',
        swap: 'innerHTML',
        pushUrl: true
    });

    closeFilterDropdowns();
}

// Clear a specific filter
function clearFilter(colName) {
    const url = new URL(window.location.href);
    removeFilterParams(url, colName);
    url.searchParams.set('page', '1');

    htmx.ajax('GET', url.pathname + url.search, {
        target: '#table-container',
        swap: 'innerHTML',
        pushUrl: true
    });

    closeFilterDropdowns();
}

// Remove all filter params for a column from URL
function removeFilterParams(url, colName) {
    const keysToDelete = [];
    url.searchParams.forEach((value, key) => {
        if (key === 'filter[' + colName + ']') {
            keysToDelete.push(key);
        }
    });
    keysToDelete.forEach(key => url.searchParams.delete(key));
}

// Close all filter dropdowns
function closeFilterDropdowns() {
    document.querySelectorAll('[id^="filter-dropdown-"]').forEach(dropdown => {
        dropdown.classList.add('hidden');
    });
    activeFilterDropdown = null;
}

// Initialize filter handlers on page load
document.addEventListener('DOMContentLoaded', initFilterHandlers);

// Re-init filters after HTMX swap
document.body.addEventListener('htmx:afterSwap', function(e) {
    if (e.detail.target.id === 'table-container') {
        closeFilterDropdowns();
    }
});

// ============================================================================
// Table Keyboard Navigation
// ============================================================================

// Focus state
let focusedCell = null;    // { row: number, col: number }
let focusedElement = null; // DOM reference

// Inject focus CSS styles
(function injectFocusStyles() {
    const style = document.createElement('style');
    style.textContent = `
        .cell-focused {
            outline: 2px solid #3b82f6 !important;
            outline-offset: -2px;
            background-color: #eff6ff !important;
        }
        td.editable-cell:focus {
            outline: 2px solid #3b82f6;
            outline-offset: -2px;
        }
    `;
    document.head.appendChild(style);
})();

// Get table dimensions (only editable cells)
function getTableDimensions() {
    const rows = document.querySelectorAll('#table-container tbody tr');
    const firstRow = rows[0];
    const cols = firstRow ? firstRow.querySelectorAll('td.editable-cell').length : 0;
    return { rows: rows.length, cols };
}

// Get cell at position
function getCellAt(row, col) {
    const rows = document.querySelectorAll('#table-container tbody tr');
    if (row < 0 || row >= rows.length) return null;
    const cells = rows[row].querySelectorAll('td.editable-cell');
    if (col < 0 || col >= cells.length) return null;
    return cells[col];
}

// Get position of cell
function getCellPosition(cell) {
    const row = cell.closest('tr');
    const rows = Array.from(document.querySelectorAll('#table-container tbody tr'));
    const rowIndex = rows.indexOf(row);
    const cells = Array.from(row.querySelectorAll('td.editable-cell'));
    const colIndex = cells.indexOf(cell);
    return { row: rowIndex, col: colIndex };
}

// Apply focus to cell
function focusCell(cell) {
    // Remove previous focus
    if (focusedElement) {
        focusedElement.classList.remove('cell-focused');
        focusedElement.removeAttribute('tabindex');
    }

    if (!cell) {
        focusedCell = null;
        focusedElement = null;
        return;
    }

    // Apply new focus
    cell.classList.add('cell-focused');
    cell.setAttribute('tabindex', '0');
    cell.focus();

    focusedElement = cell;
    focusedCell = getCellPosition(cell);

    // Scroll into view if needed
    cell.scrollIntoView({ block: 'nearest', inline: 'nearest' });
}

// Handle table navigation keydown
function handleTableKeydown(e) {
    // Skip if in input (unless it's the edit input which has its own handler)
    if (e.target.matches('input:not(.edit-input), textarea, select:not(.edit-input)')) {
        return;
    }

    // Skip if currently editing
    if (currentEditCell) {
        return;
    }

    // Skip if modal is open
    if (document.getElementById('delete-modal') && !document.getElementById('delete-modal').classList.contains('hidden')) {
        return;
    }

    const { rows, cols } = getTableDimensions();
    if (rows === 0 || cols === 0) return;

    // Initialize focus if none and navigation key pressed
    if (!focusedCell) {
        if (['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'Enter', 'Tab'].includes(e.key)) {
            // Only capture if focus is on the table or body
            if (e.target === document.body || e.target.closest('#table-container')) {
                e.preventDefault();
                focusCell(getCellAt(0, 0));
            }
        }
        return;
    }

    let { row, col } = focusedCell;
    let handled = true;

    switch (e.key) {
        case 'ArrowUp':
            row = Math.max(0, row - 1);
            break;
        case 'ArrowDown':
            row = Math.min(rows - 1, row + 1);
            break;
        case 'ArrowLeft':
            col = Math.max(0, col - 1);
            break;
        case 'ArrowRight':
            col = Math.min(cols - 1, col + 1);
            break;
        case 'Tab':
            if (e.shiftKey) {
                col--;
                if (col < 0) {
                    col = cols - 1;
                    row--;
                }
                if (row < 0) {
                    handled = false; // Let Tab leave table
                    focusCell(null);
                }
            } else {
                col++;
                if (col >= cols) {
                    col = 0;
                    row++;
                }
                if (row >= rows) {
                    handled = false; // Let Tab leave table
                    focusCell(null);
                }
            }
            break;
        case 'Home':
            if (e.ctrlKey) {
                row = 0;
                col = 0;
            } else {
                col = 0;
            }
            break;
        case 'End':
            if (e.ctrlKey) {
                row = rows - 1;
                col = cols - 1;
            } else {
                col = cols - 1;
            }
            break;
        case 'Enter':
            // Start editing the focused cell
            const cell = getCellAt(row, col);
            if (cell) {
                startEdit(cell);
            }
            break;
        case 'Escape':
            focusCell(null); // Clear focus
            break;
        default:
            handled = false;
    }

    if (handled) {
        e.preventDefault();
        if (e.key !== 'Enter' && e.key !== 'Escape') {
            focusCell(getCellAt(row, col));
        }
    }
}

// Initialize table navigation
function initTableNavigation() {
    document.addEventListener('keydown', handleTableKeydown);

    // Click on cell to focus it (single click)
    document.addEventListener('click', function(e) {
        const cell = e.target.closest('td.editable-cell');
        if (cell && !currentEditCell) {
            focusCell(cell);
        } else if (!e.target.closest('#table-container') && !e.target.closest('.modal')) {
            // Clear focus when clicking outside table
            focusCell(null);
        }
    });
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', initTableNavigation);

// ============================================================================
// Aggregation Metrics Toggle
// ============================================================================

const DEFAULT_METRICS = ['sum'];  // Default: Sum only

function getMetricsStorageKey(tableKey) {
    return STORAGE_KEYS.metrics(tableKey);
}

function getSelectedMetrics(tableKey) {
    return getStorage(STORAGE_KEYS.metrics(tableKey), DEFAULT_METRICS);
}

function saveSelectedMetrics(tableKey, metrics) {
    setStorage(STORAGE_KEYS.metrics(tableKey), metrics);
}

function toggleMetricsDropdown() {
    const dropdown = document.getElementById('metrics-dropdown');
    if (dropdown) {
        dropdown.classList.toggle('hidden');
    }
}

function initMetricsToggle() {
    const container = document.getElementById('table-container');
    if (!container) return;

    const tableKey = container.dataset.tableKey;
    if (!tableKey) return;

    const selected = getSelectedMetrics(tableKey);

    // Update toggle states
    document.querySelectorAll('.metrics-toggle').forEach(toggle => {
        toggle.checked = selected.includes(toggle.value);
    });

    // Apply visibility to rows
    applyMetricsVisibility(selected);
}

function applyMetricsSelection() {
    const container = document.getElementById('table-container');
    if (!container) return;

    const tableKey = container.dataset.tableKey;
    const selected = Array.from(document.querySelectorAll('.metrics-toggle:checked'))
        .map(cb => cb.value);

    saveSelectedMetrics(tableKey, selected);
    applyMetricsVisibility(selected);
}

function applyMetricsVisibility(selected) {
    document.querySelectorAll('.aggregation-row').forEach(row => {
        const metric = row.dataset.metric;
        row.style.display = selected.includes(metric) ? '' : 'none';
    });
}

function selectAllMetrics() {
    document.querySelectorAll('.metrics-toggle').forEach(cb => cb.checked = true);
    applyMetricsSelection();
}

function clearAllMetrics() {
    document.querySelectorAll('.metrics-toggle').forEach(cb => cb.checked = false);
    applyMetricsSelection();
}

// Close metrics dropdown on outside click
document.addEventListener('click', function(e) {
    const dropdown = document.getElementById('metrics-dropdown');
    if (dropdown && !dropdown.classList.contains('hidden')) {
        if (!e.target.closest('#metrics-dropdown') &&
            !e.target.closest('[onclick*="toggleMetricsDropdown"]')) {
            dropdown.classList.add('hidden');
        }
    }
});

// Reinitialize on HTMX swap
document.body.addEventListener('htmx:afterSwap', function(e) {
    if (e.detail.target.id === 'table-container') {
        initMetricsToggle();
    }
});

// ============================================================================
// Upload Rollback
// ============================================================================

// Store current rollback context
let pendingRollback = null;

function confirmRollback(btn) {
    const uploadId = btn.dataset.uploadId;
    const fileName = btn.dataset.fileName;
    const rowCount = parseInt(btn.dataset.rowCount, 10);

    pendingRollback = { uploadId, fileName, rowCount };
    document.getElementById('rollback-row-count').textContent = rowCount;
    document.getElementById('rollback-file-name').textContent = fileName || 'Unknown file';
    showModal('rollback-modal');
}

function hideRollbackModal() {
    hideModal('rollback-modal');
    pendingRollback = null;
}

async function executeRollback() {
    if (!pendingRollback) return;

    const { uploadId, rowCount } = pendingRollback;
    const confirmBtn = document.getElementById('rollback-confirm-btn');

    // Disable button and show loading state
    confirmBtn.disabled = true;
    confirmBtn.textContent = 'Deleting...';

    try {
        const response = await fetch(`/api/rollback/${uploadId}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });

        const result = await response.json();

        if (result.success) {
            hideRollbackModal();
            showToast(`Rolled back ${result.rowsDeleted} rows`);

            // Refresh the upload history to show updated status
            const tableKey = getTableKey();
            if (tableKey) {
                htmx.ajax('GET', `/api/history/${tableKey}`, {
                    target: '#upload-history',
                    swap: 'innerHTML'
                });

                // Also refresh the table view
                const url = new URL(window.location.href);
                htmx.ajax('GET', url.pathname + url.search, {
                    target: '#table-container',
                    swap: 'innerHTML'
                });
            }
        } else {
            showToast('Rollback failed: ' + (result.error || 'Unknown error'), true);
        }
    } catch (e) {
        console.error('Rollback error:', e);
        showToast('Rollback failed: ' + e.message, true);
    } finally {
        // Reset button state
        confirmBtn.disabled = false;
        confirmBtn.textContent = `Delete ${rowCount} Rows`;
        pendingRollback = null;
    }
}

// ============================================================================
// Reset All Data Modal
// ============================================================================

function showResetAllModal() {
    const input = document.getElementById('reset-all-confirm-input');
    const btn = document.getElementById('reset-all-confirm-btn');
    if (input) input.value = '';
    if (btn) btn.disabled = true;
    showModal('reset-all-modal');
    // Focus the input after showing
    setTimeout(() => input && input.focus(), 100);
}

function hideResetAllModal() {
    hideModal('reset-all-modal');
    const input = document.getElementById('reset-all-confirm-input');
    if (input) input.value = '';
}

function validateResetAllInput() {
    const input = document.getElementById('reset-all-confirm-input');
    const btn = document.getElementById('reset-all-confirm-btn');
    if (!input || !btn) return;
    btn.disabled = input.value.trim() !== 'DELETE ALL';
}

async function executeResetAll() {
    const input = document.getElementById('reset-all-confirm-input');
    const btn = document.getElementById('reset-all-confirm-btn');

    if (!input || input.value.trim() !== 'DELETE ALL') return;

    // Disable button and show loading state
    if (btn) {
        btn.disabled = true;
        btn.textContent = 'Deleting...';
    }

    try {
        const response = await fetch('/api/reset', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });

        if (response.ok) {
            hideResetAllModal();
            showToast('All data has been reset');
            // Refresh the page to show updated state
            window.location.reload();
        } else {
            const result = await response.json();
            showToast('Reset failed: ' + (result.error || 'Unknown error'), true);
        }
    } catch (e) {
        console.error('Reset all error:', e);
        showToast('Reset failed: ' + e.message, true);
    } finally {
        // Reset button state
        if (btn) {
            btn.disabled = false;
            btn.textContent = 'Reset All Data';
        }
    }
}

// ============================================================================
// Bulk Edit
// ============================================================================

// Show bulk edit modal
function showBulkEditModal() {
    const count = selectedRows.size;
    if (count === 0) return;

    // Update count in modal
    const countEl = document.getElementById('bulk-edit-count');
    if (countEl) countEl.textContent = count;

    // Update singular/plural label
    const labelEl = document.getElementById('bulk-edit-rows-label');
    if (labelEl) labelEl.textContent = count === 1 ? 'Row' : 'Rows';

    // Populate column dropdown with editable columns (exclude unique key columns)
    populateBulkEditColumns();

    // Show modal
    showModal('bulk-edit-modal');
}

// Hide bulk edit modal
function hideBulkEditModal() {
    hideModal('bulk-edit-modal');

    // Clear value container and hide wrapper
    const valueContainer = document.getElementById('bulk-edit-value-container');
    const valueWrapper = document.getElementById('bulk-edit-value-wrapper');
    if (valueContainer) valueContainer.innerHTML = '';
    if (valueWrapper) valueWrapper.classList.add('hidden');

    // Reset column dropdown
    const columnSelect = document.getElementById('bulk-edit-column');
    if (columnSelect) columnSelect.selectedIndex = 0;

    // Disable submit button
    const submitBtn = document.getElementById('bulk-edit-submit');
    if (submitBtn) submitBtn.disabled = true;
}

// Populate column dropdown with editable columns
function populateBulkEditColumns() {
    const columnSelect = document.getElementById('bulk-edit-column');
    if (!columnSelect) return;

    // Re-parse column metadata if not available
    if (!columnsMeta || columnsMeta.length === 0) {
        const container = document.getElementById('table-container');
        if (container && container.dataset.columnsMeta) {
            try {
                columnsMeta = JSON.parse(container.dataset.columnsMeta);
            } catch (e) {
                console.error('Failed to parse columns meta:', e);
                columnsMeta = [];
            }
        }
    }

    // Still no metadata, can't populate
    if (!columnsMeta || columnsMeta.length === 0) return;

    // Clear existing options except the placeholder
    columnSelect.innerHTML = '<option value="">Select a column...</option>';

    // Add editable columns (exclude unique key columns)
    for (const meta of columnsMeta) {
        if (meta.isUniqueKey) continue;

        const option = document.createElement('option');
        option.value = meta.name;
        option.textContent = meta.name;
        columnSelect.appendChild(option);
    }
}

// Handle column selection change
function onBulkEditColumnChange() {
    const columnSelect = document.getElementById('bulk-edit-column');
    const valueContainer = document.getElementById('bulk-edit-value-container');
    const valueWrapper = document.getElementById('bulk-edit-value-wrapper');
    const submitBtn = document.getElementById('bulk-edit-submit');

    if (!columnSelect || !valueContainer) return;

    const colName = columnSelect.value;
    if (!colName) {
        valueContainer.innerHTML = '';
        if (valueWrapper) valueWrapper.classList.add('hidden');
        if (submitBtn) submitBtn.disabled = true;
        return;
    }

    const meta = getColumnMeta(colName);
    if (!meta) {
        valueContainer.innerHTML = '';
        if (valueWrapper) valueWrapper.classList.add('hidden');
        if (submitBtn) submitBtn.disabled = true;
        return;
    }

    // Build appropriate input based on column type
    const inputHTML = buildBulkEditInput(meta);
    valueContainer.innerHTML = inputHTML;

    // Show the value wrapper
    if (valueWrapper) valueWrapper.classList.remove('hidden');

    // Enable submit button
    if (submitBtn) submitBtn.disabled = false;

    // Focus the input
    const input = valueContainer.querySelector('input, select');
    if (input) input.focus();
}

// Build input for bulk edit (similar to buildEditInput but without keydown handlers)
function buildBulkEditInput(meta) {
    const inputClass = 'w-full px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:text-white';

    switch (meta.type) {
        case 'numeric':
            return `<input type="number" step="any" id="bulk-edit-value" class="${inputClass}" placeholder="Enter a number">`;

        case 'date':
            return `<input type="date" id="bulk-edit-value" class="${inputClass}">`;

        case 'bool':
            return `<select id="bulk-edit-value" class="${inputClass}">
                <option value="">-</option>
                <option value="true">Yes</option>
                <option value="false">No</option>
            </select>`;

        case 'enum':
            const options = (meta.enumValues || []).map(v =>
                `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`
            ).join('');
            return `<select id="bulk-edit-value" class="${inputClass}">
                <option value="">-</option>
                ${options}
            </select>`;

        case 'text':
        default:
            return `<input type="text" id="bulk-edit-value" class="${inputClass}" placeholder="Enter text">`;
    }
}

// Submit bulk edit
async function confirmBulkEdit() {
    const tableKey = getTableKey();
    if (!tableKey || selectedRows.size === 0) {
        hideBulkEditModal();
        return;
    }

    const columnSelect = document.getElementById('bulk-edit-column');
    const valueInput = document.getElementById('bulk-edit-value');
    const submitBtn = document.getElementById('bulk-edit-submit');

    if (!columnSelect || !valueInput) return;

    const column = columnSelect.value;
    const value = valueInput.value;
    const keys = Array.from(selectedRows);

    if (!column) {
        showToast('Please select a column', true);
        return;
    }

    // Disable submit button during request
    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = 'Updating...';
    }

    try {
        const response = await fetch(`/api/bulk-edit/${tableKey}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ keys, column, value })
        });

        const result = await response.json();

        if (response.ok) {
            hideBulkEditModal();
            selectedRows.clear();
            updateSelectionUI();

            if (result.updated > 0) {
                showToast(`Updated ${result.updated} row${result.updated !== 1 ? 's' : ''}`);
            }
            if (result.failed > 0) {
                showToast(`${result.failed} row${result.failed !== 1 ? 's' : ''} failed`, true);
                console.error('Bulk edit errors:', result.errors);
            }

            // Refresh the table
            const url = new URL(window.location.href);
            htmx.ajax('GET', url.pathname + url.search, { target: '#table-container', swap: 'innerHTML' });
        } else {
            showToast(result.error || 'Bulk edit failed', true);
        }
    } catch (e) {
        console.error('Bulk edit error:', e);
        showToast('Bulk edit failed', true);
    } finally {
        // Reset button state
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = `Update ${keys.length} Row${keys.length !== 1 ? 's' : ''}`;
        }
    }
}

// Close bulk edit modal on outside click
document.addEventListener('click', function(e) {
    const modal = document.getElementById('bulk-edit-modal');
    if (modal && e.target === modal) {
        hideBulkEditModal();
    }
});
