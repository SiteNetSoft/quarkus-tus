import { LitElement, html, css } from 'lit';
import { JsonRpc } from 'jsonrpc';

export class QwcTusUploads extends LitElement {

    static styles = css`
        .config-table {
            width: 100%;
            border-collapse: collapse;
        }
        .config-table th, .config-table td {
            text-align: left;
            padding: 8px 12px;
            border-bottom: 1px solid var(--lumo-contrast-10pct);
        }
        .config-table th {
            font-weight: 600;
            color: var(--lumo-secondary-text-color);
        }
        .section-title {
            font-size: 1.1em;
            font-weight: 600;
            margin: 16px 0 8px 0;
        }
        .badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.85em;
        }
        .badge-enabled {
            background: var(--lumo-success-color-10pct);
            color: var(--lumo-success-text-color);
        }
        .badge-disabled {
            background: var(--lumo-error-color-10pct);
            color: var(--lumo-error-text-color);
        }
    `;

    static properties = {
        _config: { state: true },
        _storeType: { state: true },
        _tusPath: { state: true },
        _sseEnabled: { state: true },
        _authEnabled: { state: true }
    };

    constructor() {
        super();
        this._config = null;
        this._storeType = null;
        this._tusPath = null;
        this._sseEnabled = null;
        this._authEnabled = null;
    }

    connectedCallback() {
        super.connectedCallback();
        this._tusPath = this.jsonRpc ? undefined : this.getAttribute('tusPath');
        this._sseEnabled = this.jsonRpc ? undefined : this.getAttribute('sseEnabled');
        this._authEnabled = this.jsonRpc ? undefined : this.getAttribute('authEnabled');

        this.jsonRpc = new JsonRpc(this);
        this._fetchData();
    }

    async _fetchData() {
        try {
            const configResponse = await this.jsonRpc.getConfig();
            this._config = configResponse.result;
            const storeResponse = await this.jsonRpc.getStoreType();
            this._storeType = storeResponse.result;
        } catch (e) {
            console.error('Failed to fetch TUS config', e);
        }
    }

    render() {
        return html`
            <div class="section-title">Build-Time Configuration</div>
            <table class="config-table">
                <tr><th>Property</th><th>Value</th></tr>
                <tr><td>TUS Path</td><td>${this._tusPath ?? 'loading...'}</td></tr>
                <tr><td>SSE Enabled</td><td>${this._renderBadge(this._sseEnabled)}</td></tr>
                <tr><td>Auth Enabled</td><td>${this._renderBadge(this._authEnabled)}</td></tr>
            </table>

            <div class="section-title">Runtime Configuration</div>
            ${this._config ? html`
                <table class="config-table">
                    <tr><th>Property</th><th>Value</th></tr>
                    <tr><td>Version</td><td>${this._config.version}</td></tr>
                    <tr><td>Max Size</td><td>${this._formatBytes(this._config.maxSize)}</td></tr>
                    <tr><td>Max Chunk Size</td><td>${this._formatBytes(this._config.maxChunkSize)}</td></tr>
                    <tr><td>Expiration</td><td>${this._config.expirationHours}h</td></tr>
                    <tr><td>Extensions</td><td>${this._config.extensions}</td></tr>
                    <tr><td>Checksum Algorithms</td><td>${this._config.checksumAlgorithms}</td></tr>
                </table>
            ` : html`<p>Loading runtime config...</p>`}

            <div class="section-title">Store</div>
            <table class="config-table">
                <tr><th>Implementation</th><td>${this._storeType ?? 'loading...'}</td></tr>
            </table>
        `;
    }

    _renderBadge(value) {
        if (value === null || value === undefined) return 'loading...';
        const enabled = value === true || value === 'true';
        return html`<span class="badge ${enabled ? 'badge-enabled' : 'badge-disabled'}">${enabled ? 'Enabled' : 'Disabled'}</span>`;
    }

    _formatBytes(bytes) {
        if (bytes === null || bytes === undefined) return '';
        if (bytes >= 1073741824) return (bytes / 1073741824).toFixed(1) + ' GB';
        if (bytes >= 1048576) return (bytes / 1048576).toFixed(1) + ' MB';
        if (bytes >= 1024) return (bytes / 1024).toFixed(1) + ' KB';
        return bytes + ' B';
    }
}

customElements.define('qwc-tus-uploads', QwcTusUploads);
