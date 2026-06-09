/**
 * modules/transports.js — WebSocket / WebTransport drivers
 * --------------------------------------------------------
 *
 * Two Transport drivers — the WebSocket driver (default, talks to the Go
 * relay's /ws/data?role=browser endpoint) and the WebTransport driver
 * (HTTP/3 datagrams to /wt?role=browser). createTransport(kind) dispatches.
 *
 *   interface Transport {
 *     onOpen, onClose, onMessage callbacks
 *     connect(): Promise<void>
 *     send(data: ArrayBuffer | string): void
 *     close(): void
 *     get isOpen(): boolean
 *   }
 *
 * Both drivers surface incoming binary frames as ArrayBuffer and incoming
 * text frames as strings; the JSON codec accepts either, so the wire codec
 * round-trips regardless of which driver is in use.
 */

import { CONFIG } from './config.js';
import { logDebug, logInfo, logWarn, logError } from './logger.js';

class Transport {
    constructor() {
        this.onOpen = null;
        this.onClose = null;
        this.onMessage = null;
    }
    async connect() { throw new Error('not implemented'); }
    send(_data) { throw new Error('not implemented'); }
    close() {}
    get isOpen() { return false; }
    get label() { return 'transport'; }
}

class WebTransportTransport extends Transport {
    constructor() {
        super();
        this._wt = null;
        this._writer = null;
        this._reading = false;
        this._enc = new TextEncoder();
    }

    get label() { return 'webtransport'; }
    get isOpen() { return !!this._wt && !!this._writer; }

    static get supported() { return typeof window !== 'undefined' && 'WebTransport' in window; }

    async connect() {
        if (!WebTransportTransport.supported)
            throw new Error('WebTransport is not supported in this browser');

        const url = CONFIG.wtUrl;
        // Fetch the relay's cert SPKI hash and pin it via serverCertificateHashes
        // so Chrome accepts the self-signed cert without any --ignore-* flag.
        // Requires the cert to be ECDSA P-256 and ≤14d validity; the Go relay
        // exposes the hash at /cert-hash so we don't have to bake it in here.
        const opts = await this._buildOptions();
        logInfo('wt', `Opening WebTransport: ${url}`);
        this._wt = new WebTransport(url, opts);
        await this._wt.ready;
        logInfo('wt', 'WebTransport session ready');

        this._writer = this._wt.datagrams.writable.getWriter();
        this._readLoop();

        this._wt.closed
            .then(() => { logInfo('wt', 'WebTransport closed'); this.onClose && this.onClose(); })
            .catch((e) => { logWarn('wt', `WebTransport closed: ${e?.message || e}`); this.onClose && this.onClose(); });

        this.onOpen && this.onOpen();
    }

    async _readLoop() {
        this._reading = true;
        const reader = this._wt.datagrams.readable.getReader();
        try {
            while (this._reading) {
                const { value, done } = await reader.read();
                if (done) break;
                if (value && this.onMessage) {
                    this.onMessage(value.slice().buffer);
                }
            }
        } catch (e) {
            logDebug('wt', `datagram read ended: ${e?.message || e}`);
        } finally {
            try { reader.releaseLock(); } catch (_) {}
        }
    }

    send(data) {
        const bytes = (typeof data === 'string') ? this._enc.encode(data)
                    : (data instanceof ArrayBuffer) ? new Uint8Array(data)
                    : data;
        this._writer.write(bytes).catch((e) => logError('wt', `datagram write error: ${e?.message || e}`));
    }

    close() {
        this._reading = false;
        try { this._writer?.releaseLock(); } catch (_) {}
        try { this._wt?.close(); } catch (_) {}
        this._wt = null; this._writer = null;
    }

    /** Fetch the relay's /cert-hash and convert it into the byte buffer that
     *  WebTransport's serverCertificateHashes option expects.  Falls back to
     *  an empty options object (= default Chrome cert verification) if the
     *  endpoint is unreachable, so a properly-trusted cert still works. */
    async _buildOptions() {
        // Escape hatch: when the page is loaded with ?nohash=1 (or
        // localStorage.wtNoHash === '1'), skip serverCertificateHashes entirely
        // so Chrome falls back to its normal cert verifier — useful when
        // pairing with --ignore-certificate-errors-spki-list on the command
        // line for Chrome versions that misbehave with serverCertificateHashes.
        const skip = (typeof location !== 'undefined' &&
                      new URLSearchParams(location.search).get('nohash') === '1')
                  || (typeof localStorage !== 'undefined' &&
                      localStorage.getItem('wtNoHash') === '1');
        if (skip) {
            logInfo('wt', 'serverCertificateHashes DISABLED (?nohash=1) — using default cert verifier');
            return {};
        }
        try {
            const origin   = new URL(CONFIG.wtUrl).origin;     // https://host:port
            const resp     = await fetch(origin + '/cert-hash', { cache: 'no-store' });
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const { spki_sha256_b64 } = await resp.json();
            if (!spki_sha256_b64) throw new Error('missing spki_sha256_b64');
            const raw = Uint8Array.from(atob(spki_sha256_b64), c => c.charCodeAt(0));
            if (raw.length !== 32) throw new Error(`expected 32-byte SHA-256, got ${raw.length}`);
            logInfo('wt', `Pinned cert SPKI sha256: ${spki_sha256_b64} (${raw.length} B)`);
            // Pass the Uint8Array view directly — the spec accepts BufferSource,
            // and some Chrome versions misinterpret a bare ArrayBuffer here.
            return {
                serverCertificateHashes: [{ algorithm: 'sha-256', value: raw }],
            };
        } catch (e) {
            logWarn('wt', `cert-hash fetch failed (${e.message}) — falling back to default cert verification`);
            return {};
        }
    }
}

class WebSocketTransport extends Transport {
    constructor() {
        super();
        this._ws = null;
        this._enc = new TextEncoder();
    }

    get label() { return 'websocket'; }
    get isOpen() { return !!this._ws && this._ws.readyState === WebSocket.OPEN; }

    async connect() {
        const url = CONFIG.dataWsUrl;
        logInfo('ws', `Opening WebSocket: ${url}`);
        return new Promise((resolve, reject) => {
            let ws;
            try {
                ws = new WebSocket(url);
            } catch (e) {
                reject(e);
                return;
            }
            ws.binaryType = 'arraybuffer';

            ws.onopen = () => {
                this._ws = ws;
                logInfo('ws', 'WebSocket open');
                this.onOpen && this.onOpen();
                resolve();
            };
            ws.onmessage = (ev) => {
                if (!this.onMessage) return;
                // ArrayBuffer for binary frames, string for text frames.
                this.onMessage(ev.data);
            };
            ws.onclose = (ev) => {
                logInfo('ws', `WebSocket closed code=${ev.code} reason=${ev.reason || '(none)'}`);
                this._ws = null;
                this.onClose && this.onClose();
            };
            ws.onerror = (ev) => {
                logWarn('ws', `WebSocket error: ${ev?.message || 'unknown'}`);
                if (ws.readyState !== WebSocket.OPEN) reject(new Error('WebSocket handshake failed'));
            };
        });
    }

    send(data) {
        if (!this.isOpen) return;
        // WebSocket.send takes ArrayBuffer / TypedArray / Blob / string directly.
        try { this._ws.send(data); }
        catch (e) { logError('ws', `send error: ${e?.message || e}`); }
    }

    close() {
        try { this._ws?.close(); } catch (_) {}
        this._ws = null;
    }
}

export function createTransport(kind) {
    if (kind === 'websocket')    return new WebSocketTransport();
    if (kind === 'webtransport') return new WebTransportTransport();
    logWarn('transport', `Unknown transport kind '${kind}' — defaulting to websocket`);
    return new WebSocketTransport();
}

export { WebTransportTransport, WebSocketTransport };
