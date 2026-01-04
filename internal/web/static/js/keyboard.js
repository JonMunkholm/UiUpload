// ============================================================================
// Keyboard Navigation System - Enhanced Power User Navigation
// ============================================================================

(function() {
    'use strict';

    // Keyboard navigation state
    const state = {
        pendingKey: null,          // For multi-key sequences (e.g., 'g')
        pendingTimeout: null,      // Timeout to clear pending key
        selectedRowIndex: -1,      // Currently focused row index
        modalOpen: false           // Track if any modal is open
    };

    // Initialize on DOM ready
    document.addEventListener('DOMContentLoaded', init);

    function init() {
        // Inject keyboard focus styles
        injectStyles();

        // Main keyboard handler
        document.addEventListener('keydown', handleKeyboardShortcut);

        // Track modal state changes
        const observer = new MutationObserver(() => {
            state.modalOpen = isAnyModalOpen();
        });
        observer.observe(document.body, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ['class']
        });

        // Reset row focus on HTMX swaps
        document.body.addEventListener('htmx:afterSwap', function(e) {
            if (e.detail.target.id === 'table-container' || e.detail.target.id === 'audit-content') {
                state.selectedRowIndex = -1;
            }
        });
    }

    // Inject CSS for keyboard focus states
    function injectStyles() {
        const style = document.createElement('style');
        style.id = 'keyboard-nav-styles';
        style.textContent = `
            /* Row focus for keyboard navigation */
            .row-focused {
                background-color: #e0f2fe !important;
                outline: 2px solid #0ea5e9;
                outline-offset: -2px;
            }

            .dark .row-focused {
                background-color: rgba(14, 165, 233, 0.15) !important;
                outline-color: #38bdf8;
            }

            /* Keyboard badge styling */
            .kbd {
                display: inline-block;
                padding: 2px 6px;
                font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
                font-size: 0.75rem;
                font-weight: 500;
                background: #f3f4f6;
                border: 1px solid #d1d5db;
                border-radius: 4px;
                box-shadow: 0 1px 2px rgba(0,0,0,0.05);
            }

            .dark .kbd {
                background: #374151;
                border-color: #4b5563;
                color: #e5e7eb;
            }

            /* Skip to main content link */
            .skip-link {
                position: absolute;
                top: -40px;
                left: 0;
                padding: 8px 16px;
                background: #1f2937;
                color: #fff;
                z-index: 100;
                transition: top 0.2s;
            }

            .skip-link:focus {
                top: 0;
            }

            /* Key hint indicator */
            #key-hint {
                transition: opacity 0.15s;
            }
        `;
        document.head.appendChild(style);
    }

    // Check if any modal is currently open
    function isAnyModalOpen() {
        const modals = document.querySelectorAll('[id$="-modal"]');
        for (const modal of modals) {
            if (!modal.classList.contains('hidden')) return true;
        }
        return false;
    }

    // Check if user is typing in an input
    function isTypingInInput(target) {
        return target.matches('input, textarea, select') || target.isContentEditable;
    }

    // Main keyboard handler
    function handleKeyboardShortcut(e) {
        // Allow Escape to work everywhere
        if (e.key === 'Escape') {
            if (isTypingInInput(e.target) && !e.target.classList.contains('edit-input')) {
                e.target.blur();
                if (typeof clearSearch === 'function') clearSearch();
                if (typeof closeColumnDropdown === 'function') closeColumnDropdown();
                return;
            }
            handleEscape();
            return;
        }

        // Ignore shortcuts when typing
        if (isTypingInInput(e.target)) return;

        // Handle multi-key sequences (g + key)
        if (state.pendingKey === 'g') {
            clearPendingKey();
            e.preventDefault();

            switch (e.key) {
                case 'h': navigate('/'); break;
                case 'u': navigate('/'); break;  // Dashboard has upload
                case 'a': navigate('/audit-log'); break;
                case 't': navigateToTable(); break;
            }
            return;
        }

        // Start multi-key sequence
        if (e.key === 'g') {
            e.preventDefault();
            state.pendingKey = 'g';
            state.pendingTimeout = setTimeout(clearPendingKey, 1500);
            showKeyHint('g');
            return;
        }

        // Single key shortcuts
        switch (e.key) {
            case '/':
                e.preventDefault();
                focusSearch();
                break;

            case '?':
                e.preventDefault();
                showShortcutsHelp();
                break;

            // Vim-like row navigation
            case 'j':
                e.preventDefault();
                navigateRow(1);
                break;

            case 'k':
                e.preventDefault();
                navigateRow(-1);
                break;

            // Row actions
            case 'x':
                e.preventDefault();
                toggleRowSelect();
                break;

            case 'Enter':
                if (state.selectedRowIndex >= 0) {
                    e.preventDefault();
                    openSelectedRow();
                }
                break;

            // Table controls
            case 'c':
                if (!e.ctrlKey && !e.metaKey) {
                    e.preventDefault();
                    if (typeof toggleColumnDropdown === 'function') toggleColumnDropdown();
                }
                break;

            case 'v':
                if (!e.ctrlKey && !e.metaKey) {
                    e.preventDefault();
                    if (typeof toggleViewsDropdown === 'function') toggleViewsDropdown();
                }
                break;

            case 'E':
                // Shift+E for export
                e.preventDefault();
                triggerExport();
                break;

            case 'm':
                e.preventDefault();
                if (typeof toggleMetricsDropdown === 'function') toggleMetricsDropdown();
                break;

            // Pagination with n/p (vim-like)
            case 'n':
                e.preventDefault();
                goToNextPage();
                break;

            case 'p':
                if (!e.ctrlKey && !e.metaKey) {
                    e.preventDefault();
                    goToPrevPage();
                }
                break;

            // First/last page
            case 'G':
                // Shift+G goes to last page
                e.preventDefault();
                goToLastPage();
                break;

            // Delete selected
            case 'd':
                if (typeof selectedRows !== 'undefined' && selectedRows.size > 0) {
                    e.preventDefault();
                    if (typeof showDeleteModal === 'function') showDeleteModal();
                }
                break;

            // Bulk edit selected
            case 'b':
                if (typeof selectedRows !== 'undefined' && selectedRows.size > 0) {
                    e.preventDefault();
                    if (typeof showBulkEditModal === 'function') showBulkEditModal();
                }
                break;
        }
    }

    // Clear pending multi-key state
    function clearPendingKey() {
        state.pendingKey = null;
        if (state.pendingTimeout) {
            clearTimeout(state.pendingTimeout);
            state.pendingTimeout = null;
        }
        hideKeyHint();
    }

    // Show hint for multi-key sequence
    function showKeyHint(key) {
        let hint = document.getElementById('key-hint');
        if (!hint) {
            hint = document.createElement('div');
            hint.id = 'key-hint';
            hint.className = 'fixed bottom-4 left-4 bg-gray-900 text-white px-3 py-2 rounded-lg shadow-lg text-sm font-mono z-50';
            document.body.appendChild(hint);
        }
        hint.textContent = `${key} + ...`;
        hint.classList.remove('hidden');
    }

    // Hide key hint
    function hideKeyHint() {
        const hint = document.getElementById('key-hint');
        if (hint) hint.classList.add('hidden');
    }

    // Handle Escape key
    function handleEscape() {
        // Close modals
        hideShortcutsHelp();
        if (typeof hideDeleteModal === 'function') hideDeleteModal();
        if (typeof hideBulkEditModal === 'function') hideBulkEditModal();
        if (typeof hideTemplatesModal === 'function') hideTemplatesModal();
        if (typeof cancelPreview === 'function') cancelPreview();

        // Close dropdowns
        if (typeof closeColumnDropdown === 'function') closeColumnDropdown();
        if (typeof closeViewsDropdown === 'function') closeViewsDropdown();
        if (typeof closeFilterDropdowns === 'function') closeFilterDropdowns();

        // Clear edit mode
        if (typeof cancelEdit === 'function') cancelEdit();

        // Clear row focus
        clearRowFocus();

        // Clear cell focus
        if (typeof focusCell === 'function') focusCell(null);
    }

    // Navigate to URL
    function navigate(path) {
        window.location.href = path;
    }

    // Navigate to first table
    function navigateToTable() {
        const firstTableLink = document.querySelector('a[href^="/table/"]');
        if (firstTableLink) {
            window.location.href = firstTableLink.href;
        }
    }

    // Row navigation
    function getTableRows() {
        return Array.from(document.querySelectorAll('#table-container tbody tr, table tbody tr'));
    }

    function navigateRow(delta) {
        const rows = getTableRows();
        if (rows.length === 0) return;

        let newIndex = state.selectedRowIndex + delta;
        if (newIndex < 0) newIndex = 0;
        if (newIndex >= rows.length) newIndex = rows.length - 1;

        focusRowByIndex(newIndex);
    }

    function focusRowByIndex(index) {
        const rows = getTableRows();
        if (index < 0 || index >= rows.length) return;

        rows.forEach(r => r.classList.remove('row-focused'));

        const row = rows[index];
        row.classList.add('row-focused');
        row.scrollIntoView({ block: 'nearest', behavior: 'smooth' });

        state.selectedRowIndex = index;
    }

    function clearRowFocus() {
        const rows = getTableRows();
        rows.forEach(r => r.classList.remove('row-focused'));
        state.selectedRowIndex = -1;
    }

    function toggleRowSelect() {
        const rows = getTableRows();
        if (state.selectedRowIndex < 0 || state.selectedRowIndex >= rows.length) {
            if (rows.length > 0) focusRowByIndex(0);
            return;
        }

        const row = rows[state.selectedRowIndex];
        const checkbox = row.querySelector('.row-checkbox');
        if (checkbox) {
            checkbox.checked = !checkbox.checked;
            if (typeof updateSelection === 'function') updateSelection();
        }
    }

    function openSelectedRow() {
        const rows = getTableRows();
        if (state.selectedRowIndex < 0 || state.selectedRowIndex >= rows.length) return;

        const row = rows[state.selectedRowIndex];

        // Check for details element (audit log)
        const details = row.querySelector('details') || row.closest('details');
        if (details) {
            details.open = !details.open;
            return;
        }

        // Check for link
        const link = row.querySelector('a[href]');
        if (link) {
            link.click();
        }
    }

    // Search focus
    function focusSearch() {
        const search = document.querySelector('input[name="search"], [data-search-input], input[type="search"]');
        if (search) {
            search.focus();
            search.select();
        }
    }

    // Export trigger
    function triggerExport() {
        const exportLink = document.querySelector('a[href*="/api/export/"], a[href*="/api/audit-log/export"]');
        if (exportLink) exportLink.click();
    }

    // Pagination
    function goToPrevPage() {
        const prevBtn = document.querySelector('[data-prev-page]');
        if (prevBtn && !prevBtn.disabled) {
            prevBtn.click();
            return;
        }

        const buttons = document.querySelectorAll('button[hx-get]');
        for (const btn of buttons) {
            if (btn.textContent.trim() === 'Previous') {
                btn.click();
                break;
            }
        }
    }

    function goToNextPage() {
        const nextBtn = document.querySelector('[data-next-page]');
        if (nextBtn && !nextBtn.disabled) {
            nextBtn.click();
            return;
        }

        const buttons = document.querySelectorAll('button[hx-get]');
        for (const btn of buttons) {
            if (btn.textContent.trim() === 'Next') {
                btn.click();
                break;
            }
        }
    }

    function goToLastPage() {
        const pageButtons = document.querySelectorAll('button[hx-get*="page="]');
        if (pageButtons.length > 0) {
            pageButtons[pageButtons.length - 1].click();
        }
    }

    // Comprehensive keyboard shortcuts help modal
    function showShortcutsHelp() {
        let modal = document.getElementById('keyboard-help-modal');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'keyboard-help-modal';
            modal.className = 'hidden fixed inset-0 bg-gray-500 bg-opacity-75 dark:bg-gray-900 dark:bg-opacity-80 flex items-center justify-center z-50';
            modal.setAttribute('role', 'dialog');
            modal.setAttribute('aria-labelledby', 'keyboard-help-title');
            modal.onclick = (e) => { if (e.target === modal) hideShortcutsHelp(); };
            document.body.appendChild(modal);
        }

        modal.innerHTML = `
            <div class="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-2xl w-full mx-4 max-h-[80vh] overflow-hidden">
                <div class="flex items-center justify-between p-4 border-b dark:border-gray-700">
                    <h3 id="keyboard-help-title" class="text-lg font-semibold text-gray-900 dark:text-white">Keyboard Shortcuts</h3>
                    <button onclick="window.keyboardNavHideHelp()" class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300" aria-label="Close">
                        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"></path>
                        </svg>
                    </button>
                </div>
                <div class="p-4 overflow-y-auto max-h-[60vh]">
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <!-- Navigation -->
                        <section>
                            <h4 class="font-semibold text-gray-700 dark:text-gray-300 mb-3 text-sm uppercase tracking-wide">Navigation</h4>
                            <dl class="space-y-2 text-sm">
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">g</kbd> <kbd class="kbd">h</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Go to Dashboard</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">g</kbd> <kbd class="kbd">a</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Go to Audit Log</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">g</kbd> <kbd class="kbd">t</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Go to first Table</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">/</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Focus search</dd>
                                </div>
                            </dl>
                        </section>

                        <!-- Table/Row Navigation -->
                        <section>
                            <h4 class="font-semibold text-gray-700 dark:text-gray-300 mb-3 text-sm uppercase tracking-wide">Table Navigation</h4>
                            <dl class="space-y-2 text-sm">
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">j</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Move to next row</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">k</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Move to previous row</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">x</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Toggle row selection</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">Enter</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Open/expand row</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">n</kbd> / <kbd class="kbd">p</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Next/Previous page</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">G</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Go to last page</dd>
                                </div>
                            </dl>
                        </section>

                        <!-- Cell Navigation -->
                        <section>
                            <h4 class="font-semibold text-gray-700 dark:text-gray-300 mb-3 text-sm uppercase tracking-wide">Cell Editing</h4>
                            <dl class="space-y-2 text-sm">
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">Arrow Keys</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Move between cells</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">Tab</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Next cell</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">Enter</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Edit cell / Save</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">Esc</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Cancel edit</dd>
                                </div>
                            </dl>
                        </section>

                        <!-- Actions -->
                        <section>
                            <h4 class="font-semibold text-gray-700 dark:text-gray-300 mb-3 text-sm uppercase tracking-wide">Actions</h4>
                            <dl class="space-y-2 text-sm">
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">b</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Bulk edit selected</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">d</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Delete selected</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">E</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Export CSV</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">c</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Toggle columns</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">v</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Saved views</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">m</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Toggle metrics</dd>
                                </div>
                            </dl>
                        </section>

                        <!-- General -->
                        <section class="md:col-span-2">
                            <h4 class="font-semibold text-gray-700 dark:text-gray-300 mb-3 text-sm uppercase tracking-wide">General</h4>
                            <dl class="space-y-2 text-sm grid grid-cols-2 gap-x-8">
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">?</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Show this help</dd>
                                </div>
                                <div class="flex justify-between">
                                    <dt><kbd class="kbd">Esc</kbd></dt>
                                    <dd class="text-gray-600 dark:text-gray-400">Close modal / Clear</dd>
                                </div>
                            </dl>
                        </section>
                    </div>
                </div>
            </div>
        `;

        modal.classList.remove('hidden');
        state.modalOpen = true;

        // Focus close button for accessibility
        const closeBtn = modal.querySelector('button');
        if (closeBtn) closeBtn.focus();
    }

    function hideShortcutsHelp() {
        const modal = document.getElementById('keyboard-help-modal');
        if (modal) {
            modal.classList.add('hidden');
            state.modalOpen = false;
        }
    }

    // Expose functions for onclick handlers and external calls
    window.keyboardNavHideHelp = hideShortcutsHelp;
    window.showShortcutsHelp = showShortcutsHelp;

})();
