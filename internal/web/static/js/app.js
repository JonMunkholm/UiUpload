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
