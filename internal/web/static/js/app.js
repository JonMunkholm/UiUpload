// Upload modal handling
function showUploadModal() {
    document.getElementById('upload-modal').classList.remove('hidden');
}

function hideUploadModal() {
    document.getElementById('upload-modal').classList.add('hidden');
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

// Start SSE stream for upload progress
function startProgressStream(uploadId) {
    const container = document.getElementById('upload-progress-container');
    const eventSource = new EventSource(`/api/upload/${uploadId}/progress`);

    eventSource.addEventListener('progress', function(e) {
        const progress = JSON.parse(e.data);
        container.innerHTML = renderProgress(progress);
    });

    eventSource.addEventListener('complete', function(e) {
        eventSource.close();
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
    });

    eventSource.onerror = function(e) {
        eventSource.close();
        showToast('Connection lost', true);
        hideUploadModal();
    };
}

// Render progress HTML
function renderProgress(progress) {
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

    let html = `
        <div class="space-y-3">
            <div class="flex items-center justify-between">
                <span class="text-sm font-medium text-gray-700">${progress.file_name || 'Uploading...'}</span>
                <span class="text-sm text-gray-500">${phaseLabels[progress.phase] || progress.phase}</span>
            </div>
            <div class="w-full bg-gray-200 rounded-full h-2.5">
                <div class="h-2.5 rounded-full transition-all duration-300 ${barColor}" style="width: ${percent}%"></div>
            </div>
            <div class="flex justify-between text-xs text-gray-500">
                <span>${progress.current_row} / ${progress.total_rows} rows</span>
                <span>${progress.inserted} inserted, ${progress.skipped} skipped</span>
            </div>
    `;

    if (progress.error) {
        html += `<div class="mt-2 text-sm text-red-600 bg-red-50 rounded p-2">${progress.error}</div>`;
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
    return 'columns_' + tableKey;
}

// Get visible columns from localStorage (default: all visible)
function getVisibleColumns(tableKey, allColumns) {
    const stored = localStorage.getItem(getColumnStorageKey(tableKey));
    if (stored) {
        try {
            return JSON.parse(stored);
        } catch (e) {
            return allColumns;
        }
    }
    return allColumns;
}

// Save visible columns to localStorage
function saveVisibleColumns(tableKey, columns) {
    localStorage.setItem(getColumnStorageKey(tableKey), JSON.stringify(columns));
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
// Sort Persistence Feature
// ============================================================================

// Get localStorage key for a table's sort preference
function getSortStorageKey(tableKey) {
    return 'sort_' + tableKey;
}

// Get saved sort from localStorage
function getSavedSort(tableKey) {
    const stored = localStorage.getItem(getSortStorageKey(tableKey));
    if (stored) {
        try {
            return JSON.parse(stored);
        } catch (e) {
            return null;
        }
    }
    return null;
}

// Save sort preference to localStorage
function saveSort(tableKey, column, dir) {
    localStorage.setItem(getSortStorageKey(tableKey), JSON.stringify({ column, dir }));
}

// On page load: redirect to saved sort if no URL sort params
function initSortPersistence() {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const url = new URL(window.location.href);
    const hasUrlSort = url.searchParams.has('sort');

    if (!hasUrlSort) {
        const saved = getSavedSort(tableKey);
        if (saved) {
            url.searchParams.set('sort', saved.column);
            url.searchParams.set('dir', saved.dir);
            url.searchParams.set('page', '1');
            window.location.replace(url.toString());
        }
    }
}

// Save sort when HTMX request includes sort params
document.body.addEventListener('htmx:configRequest', function(e) {
    const tableKey = getTableKey();
    if (!tableKey) return;

    const path = e.detail.path;
    if (path.includes('/table/' + tableKey) && path.includes('sort=')) {
        const url = new URL(path, window.location.origin);
        const column = url.searchParams.get('sort');
        const dir = url.searchParams.get('dir');
        if (column && dir) {
            saveSort(tableKey, column, dir);
        }
    }
});

// ============================================================================
// Keyboard Shortcuts
// ============================================================================

function initKeyboardShortcuts() {
    document.addEventListener('keydown', function(e) {
        // Ignore if typing in input/textarea
        if (e.target.matches('input, textarea, select')) {
            // But allow Esc to blur and clear
            if (e.key === 'Escape') {
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
                hideRowModal();
                cancelPreview();
                closeColumnDropdown();
                break;
            case 'c':
                toggleColumnDropdown();
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
    showToast('Shortcuts: / search, Esc clear, c columns, e export, ←→ pages');
}

// ============================================================================
// Row Details Modal
// ============================================================================

function showRowDetails(row) {
    const columns = getAllColumns();
    const cells = row.querySelectorAll('td');

    let html = '<dl class="space-y-3">';
    columns.forEach((col, i) => {
        const value = cells[i]?.getAttribute('title') || cells[i]?.textContent?.trim() || '-';
        html += `
            <div class="grid grid-cols-3 gap-4 py-2 border-b border-gray-100 last:border-0">
                <dt class="text-sm font-medium text-gray-500">${escapeHtml(col)}</dt>
                <dd class="text-sm text-gray-900 col-span-2 break-words">${escapeHtml(value)}</dd>
            </div>
        `;
    });
    html += '</dl>';

    document.getElementById('row-modal-content').innerHTML = html;
    document.getElementById('row-modal').classList.remove('hidden');
}

function hideRowModal() {
    const modal = document.getElementById('row-modal');
    if (modal) modal.classList.add('hidden');
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Close row modal on outside click
document.addEventListener('click', function(e) {
    const modal = document.getElementById('row-modal');
    if (modal && e.target === modal) {
        hideRowModal();
    }
});

// ============================================================================
// CSV Preview
// ============================================================================

let currentPreviewForm = null;

function showPreview(input) {
    if (!input.files || !input.files[0]) return;

    const file = input.files[0];
    const form = input.closest('form');
    const expectedColumns = JSON.parse(form.dataset.columns || '[]');
    const tableLabel = form.dataset.tableLabel || 'Table';

    currentPreviewForm = form;

    const reader = new FileReader();
    reader.onload = function(e) {
        const text = e.target.result;
        const { headers, rows, totalRows } = parseCSV(text);
        renderPreview(tableLabel, expectedColumns, headers, rows, totalRows, file.name);
    };
    reader.readAsText(file);
}

function parseCSV(text) {
    const lines = text.trim().split('\n');
    if (lines.length === 0) return { headers: [], rows: [], totalRows: 0 };

    const headers = parseCSVLine(lines[0]);
    const rows = [];
    const previewCount = Math.min(5, lines.length - 1);

    for (let i = 1; i <= previewCount; i++) {
        if (lines[i]) rows.push(parseCSVLine(lines[i]));
    }

    return { headers, rows, totalRows: lines.length - 1 };
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

function renderPreview(tableLabel, expected, actual, rows, totalRows, fileName) {
    const columnsMatch = expected.length === actual.length &&
        expected.every((col, i) => col === actual[i]);

    let html = `
        <div class="mb-4">
            <div class="text-sm text-gray-600">File: <span class="font-medium">${escapeHtml(fileName)}</span></div>
            <div class="text-sm text-gray-600">Table: <span class="font-medium">${escapeHtml(tableLabel)}</span></div>
            <div class="text-sm text-gray-600">Rows: <span class="font-medium">${totalRows}</span></div>
        </div>

        <div class="mb-4 p-3 rounded-lg ${columnsMatch ? 'bg-green-50 border border-green-200' : 'bg-yellow-50 border border-yellow-200'}">
            <div class="flex items-center gap-2 mb-2">
                ${columnsMatch
                    ? '<svg class="w-5 h-5 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg><span class="text-sm font-medium text-green-800">Columns match</span>'
                    : '<svg class="w-5 h-5 text-yellow-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg><span class="text-sm font-medium text-yellow-800">Column mismatch</span>'
                }
            </div>
            <div class="grid grid-cols-2 gap-4 text-xs">
                <div>
                    <div class="font-medium text-gray-600 mb-1">Expected (${expected.length})</div>
                    ${expected.map(c => `<div class="text-gray-700">${escapeHtml(c)}</div>`).join('')}
                </div>
                <div>
                    <div class="font-medium text-gray-600 mb-1">Found in CSV (${actual.length})</div>
                    ${actual.map(c => `<div class="text-gray-700">${escapeHtml(c)}</div>`).join('')}
                </div>
            </div>
        </div>

        <div class="text-sm font-medium text-gray-700 mb-2">Preview (first ${rows.length} rows)</div>
        <div class="overflow-x-auto border rounded-lg">
            <table class="min-w-full text-xs">
                <thead class="bg-gray-50">
                    <tr>
                        ${actual.map(h => `<th class="px-3 py-2 text-left font-medium text-gray-600">${escapeHtml(h)}</th>`).join('')}
                    </tr>
                </thead>
                <tbody class="divide-y">
                    ${rows.map(row => `
                        <tr>
                            ${row.map(cell => `<td class="px-3 py-2 text-gray-700 whitespace-nowrap">${escapeHtml(cell || '-')}</td>`).join('')}
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;

    document.getElementById('preview-content').innerHTML = html;
    document.getElementById('preview-modal').classList.remove('hidden');
}

function confirmUpload() {
    if (currentPreviewForm) {
        document.getElementById('preview-modal').classList.add('hidden');
        htmx.trigger(currentPreviewForm, 'upload');
        currentPreviewForm = null;
    }
}

function cancelPreview() {
    document.getElementById('preview-modal').classList.add('hidden');
    if (currentPreviewForm) {
        const input = currentPreviewForm.querySelector('input[type="file"]');
        if (input) input.value = '';
        currentPreviewForm = null;
    }
}

// Close preview modal on outside click
document.addEventListener('click', function(e) {
    const modal = document.getElementById('preview-modal');
    if (modal && e.target === modal) {
        cancelPreview();
    }
});
