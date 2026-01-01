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
// Saved Views Feature
// ============================================================================

// Get localStorage key for a table's saved views
function getViewsStorageKey(tableKey) {
    return 'views_' + tableKey;
}

// Get all saved views for a table
function getSavedViews(tableKey) {
    const stored = localStorage.getItem(getViewsStorageKey(tableKey));
    if (stored) {
        try {
            return JSON.parse(stored);
        } catch (e) {
            return [];
        }
    }
    return [];
}

// Save views array to localStorage
function saveViews(tableKey, views) {
    localStorage.setItem(getViewsStorageKey(tableKey), JSON.stringify(views));
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

function showPreview(input) {
    if (!input.files || !input.files[0]) return;

    const file = input.files[0];
    const form = input.closest('form');
    const expectedColumns = JSON.parse(form.dataset.columns || '[]');
    const uniqueKey = JSON.parse(form.dataset.uniqueKey || '[]');
    const tableLabel = form.dataset.tableLabel || 'Table';
    const tableKey = form.id.replace('upload-form-', '');

    currentPreviewForm = form;

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

function renderPreview(tableLabel, tableKey, expected, uniqueKey, actual, rows, allRows, totalRows, fileName) {
    const columnsMatch = expected.length === actual.length &&
        expected.every((col, i) => col.toLowerCase() === actual[i].toLowerCase());

    let columnSection;
    if (columnsMatch) {
        // Columns match - show simple green checkmark
        columnSection = `
            <div class="mb-4 p-3 rounded-lg bg-green-50 border border-green-200">
                <div class="flex items-center gap-2 mb-2">
                    <svg class="w-5 h-5 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path>
                    </svg>
                    <span class="text-sm font-medium text-green-800">Columns match</span>
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
        `;
    } else {
        // Columns don't match - show mapping UI
        const autoMapping = buildAutoMapping(expected, actual);
        columnSection = `
            <div class="mb-4 p-3 rounded-lg bg-yellow-50 border border-yellow-200">
                <div class="flex items-center gap-2 mb-3">
                    <svg class="w-5 h-5 text-yellow-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path>
                    </svg>
                    <span class="text-sm font-medium text-yellow-800">Column mismatch - map columns below</span>
                </div>
                ${renderMappingUI(expected, actual, autoMapping)}
            </div>
        `;
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
            <div class="text-sm text-gray-600">File: <span class="font-medium">${escapeHtml(fileName)}</span></div>
            <div class="text-sm text-gray-600">Table: <span class="font-medium">${escapeHtml(tableLabel)}</span></div>
            <div class="text-sm text-gray-600">Rows: <span class="font-medium">${totalRows}</span></div>
        </div>

        ${columnSection}
        ${csvDuplicatesSection}
        ${dbDuplicatesSection}

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
        // Remove mapping input if present
        const mappingInput = currentPreviewForm.querySelector('input[name="mapping"]');
        if (mappingInput) mappingInput.remove();
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
        bar.classList.remove('hidden');
        if (countSpan) {
            countSpan.textContent = `${count} row${count > 1 ? 's' : ''} selected`;
        }
    } else {
        bar.classList.add('hidden');
    }
}

// Show delete confirmation modal
function showDeleteModal() {
    const deleteCount = document.getElementById('delete-count');
    if (deleteCount) {
        deleteCount.textContent = selectedRows.size;
    }
    document.getElementById('delete-modal').classList.remove('hidden');
}

// Hide delete confirmation modal
function hideDeleteModal() {
    const modal = document.getElementById('delete-modal');
    if (modal) modal.classList.add('hidden');
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
    return 'agg_metrics_' + tableKey;
}

function getSelectedMetrics(tableKey) {
    const stored = localStorage.getItem(getMetricsStorageKey(tableKey));
    if (stored) {
        try {
            return JSON.parse(stored);
        } catch (e) {
            return DEFAULT_METRICS;
        }
    }
    return DEFAULT_METRICS;
}

function saveSelectedMetrics(tableKey, metrics) {
    localStorage.setItem(getMetricsStorageKey(tableKey), JSON.stringify(metrics));
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
