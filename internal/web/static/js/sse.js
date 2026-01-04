// ============================================================================
// SSE CLIENT WITH EXPONENTIAL BACKOFF RECONNECTION
// ============================================================================
//
// A robust Server-Sent Events client that automatically reconnects on failure
// with exponential backoff and jitter. Supports event ID tracking for seamless
// resumption after reconnection.
//
// Usage:
//   const client = new SSEClient('/api/upload/123/progress', {
//       onProgress: (data) => updateUI(data),
//       onComplete: (data) => handleComplete(data),
//       onError: (data) => showError(data),
//   });
//   client.connect();
//   // Later: client.close();
//

/**
 * SSEClient provides robust SSE connection management with automatic reconnection.
 */
class SSEClient {
    /**
     * @param {string} url - The SSE endpoint URL
     * @param {Object} options - Configuration options
     * @param {number} [options.maxRetries=10] - Maximum reconnection attempts
     * @param {number} [options.baseDelay=1000] - Base delay in ms for backoff
     * @param {number} [options.maxDelay=30000] - Maximum delay in ms
     * @param {function} [options.onProgress] - Called on progress events
     * @param {function} [options.onComplete] - Called on completion
     * @param {function} [options.onError] - Called on error events
     * @param {function} [options.onConnectionChange] - Called when connection state changes
     */
    constructor(url, options = {}) {
        this.url = url;
        this.maxRetries = options.maxRetries ?? 10;
        this.baseDelay = options.baseDelay ?? 1000;
        this.maxDelay = options.maxDelay ?? 30000;

        // Callbacks
        this.onProgress = options.onProgress || (() => {});
        this.onComplete = options.onComplete || (() => {});
        this.onError = options.onError || (() => {});
        this.onConnectionChange = options.onConnectionChange || (() => {});

        // Internal state
        this.eventSource = null;
        this.retryCount = 0;
        this.lastEventId = null;
        this.closed = false;
        this.reconnectTimer = null;
    }

    /**
     * Establishes the SSE connection.
     * If already connected, this is a no-op.
     */
    connect() {
        if (this.closed || this.eventSource) {
            return;
        }

        // Build URL with lastEventId for resumption
        let url = this.url;
        if (this.lastEventId !== null) {
            const separator = url.includes('?') ? '&' : '?';
            url += `${separator}lastEventId=${encodeURIComponent(this.lastEventId)}`;
        }

        this.eventSource = new EventSource(url);

        this.eventSource.onopen = () => {
            console.log('[SSE] Connected');
            this.retryCount = 0; // Reset retry count on successful connection
            this.onConnectionChange({ connected: true });
        };

        // Handle named events
        this.eventSource.addEventListener('progress', (e) => {
            this.trackEventId(e);
            try {
                const data = JSON.parse(e.data);
                this.onProgress(data);
            } catch (err) {
                console.error('[SSE] Failed to parse progress event:', err);
            }
        });

        this.eventSource.addEventListener('complete', (e) => {
            this.trackEventId(e);
            try {
                const data = JSON.parse(e.data);
                this.onComplete(data);
            } catch (err) {
                console.error('[SSE] Failed to parse complete event:', err);
            }
            // Upload is done, close gracefully
            this.close();
        });

        this.eventSource.addEventListener('error', (e) => {
            this.trackEventId(e);
            try {
                const data = JSON.parse(e.data);
                this.onError(data);
            } catch (err) {
                // Generic error without data
                this.onError({ message: 'An error occurred' });
            }
        });

        // Handle connection errors
        this.eventSource.onerror = (e) => {
            console.warn('[SSE] Connection error');
            this.cleanup();
            this.onConnectionChange({ connected: false, reconnecting: true });

            if (!this.closed) {
                this.scheduleReconnect();
            }
        };
    }

    /**
     * Tracks the event ID for resumption support.
     * @param {MessageEvent} event - The SSE event
     */
    trackEventId(event) {
        if (event.lastEventId) {
            this.lastEventId = event.lastEventId;
        }
    }

    /**
     * Schedules a reconnection attempt with exponential backoff.
     */
    scheduleReconnect() {
        if (this.retryCount >= this.maxRetries) {
            console.error('[SSE] Max retries reached, giving up');
            this.onConnectionChange({
                connected: false,
                reconnecting: false,
                failed: true
            });
            this.onError({
                message: 'Connection lost. Please refresh the page.',
                code: 'MAX_RETRIES'
            });
            return;
        }

        // Exponential backoff with jitter
        // delay = min(baseDelay * 2^retryCount + random(0-1000), maxDelay)
        const exponentialDelay = this.baseDelay * Math.pow(2, this.retryCount);
        const jitter = Math.random() * 1000;
        const delay = Math.min(exponentialDelay + jitter, this.maxDelay);

        this.retryCount++;
        console.log(`[SSE] Reconnecting in ${Math.round(delay)}ms (attempt ${this.retryCount}/${this.maxRetries})`);

        this.reconnectTimer = setTimeout(() => {
            if (!this.closed) {
                this.connect();
            }
        }, delay);
    }

    /**
     * Cleans up the current EventSource without closing the client.
     */
    cleanup() {
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
        }
    }

    /**
     * Closes the SSE connection permanently.
     * No reconnection will be attempted after calling close().
     */
    close() {
        this.closed = true;

        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }

        this.cleanup();
        this.onConnectionChange({ connected: false, reconnecting: false });
        console.log('[SSE] Closed');
    }

    /**
     * Returns whether the client is currently connected.
     * @returns {boolean}
     */
    isConnected() {
        return this.eventSource !== null &&
               this.eventSource.readyState === EventSource.OPEN;
    }
}

// Export for use in other modules
if (typeof window !== 'undefined') {
    window.SSEClient = SSEClient;
}
