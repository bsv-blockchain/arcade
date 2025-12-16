import {
  TransactionStatus,
  SubmitOptions,
  Policy,
  ClientOptions,
} from './types';

/**
 * Arcade REST API client.
 */
export class ArcadeClient {
  private baseUrl: string;
  private timeout: number;
  private fetchFn: typeof fetch;

  /**
   * Creates a new Arcade client.
   * @param baseUrl - Base URL of the Arcade server (e.g., "http://localhost:3000")
   * @param options - Client configuration options
   */
  constructor(baseUrl: string, options: ClientOptions = {}) {
    // Remove trailing slash
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.timeout = options.timeout ?? 30000;
    this.fetchFn = options.fetch ?? fetch;
  }

  /**
   * Submits a single transaction for broadcast.
   * @param rawTx - Raw transaction as hex string or Uint8Array
   * @param options - Submit options
   * @returns Transaction status
   */
  async submitTransaction(
    rawTx: string | Uint8Array,
    options?: SubmitOptions
  ): Promise<TransactionStatus> {
    // Send as hex string via JSON for cross-platform compatibility
    const hexTx = typeof rawTx === 'string' ? rawTx : bytesToHex(rawTx);

    const response = await this.request('/tx', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...this.buildSubmitHeaders(options),
      },
      body: JSON.stringify({ rawTx: hexTx }),
    });

    return response.json();
  }

  /**
   * Submits multiple transactions for broadcast.
   * @param rawTxs - Array of raw transactions as hex strings or Uint8Arrays
   * @param options - Submit options
   * @returns Array of transaction statuses
   */
  async submitTransactions(
    rawTxs: Array<string | Uint8Array>,
    options?: SubmitOptions
  ): Promise<TransactionStatus[]> {
    const body = rawTxs.map((tx) => ({
      rawTx: typeof tx === 'string' ? tx : bytesToHex(tx),
    }));

    const response = await this.request('/txs', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...this.buildSubmitHeaders(options),
      },
      body: JSON.stringify(body),
    });

    return response.json();
  }

  /**
   * Gets the current status of a transaction.
   * @param txid - Transaction ID
   * @returns Transaction status
   */
  async getStatus(txid: string): Promise<TransactionStatus> {
    const response = await this.request(`/tx/${txid}`);
    return response.json();
  }

  /**
   * Gets the transaction policy configuration.
   * @returns Policy configuration
   */
  async getPolicy(): Promise<Policy> {
    const response = await this.request('/policy');
    return response.json();
  }

  /**
   * Subscribes to transaction status updates.
   * @param callbackToken - Optional callback token to filter events
   * @returns Async iterable of transaction status updates
   */
  subscribe(callbackToken?: string): AsyncIterable<TransactionStatus> {
    const url = callbackToken
      ? `${this.baseUrl}/events/${callbackToken}`
      : `${this.baseUrl}/events`;

    return new SSESubscription(url, this.fetchFn);
  }

  /**
   * Builds HTTP headers from submit options.
   */
  private buildSubmitHeaders(options?: SubmitOptions): Record<string, string> {
    const headers: Record<string, string> = {};
    if (!options) return headers;

    if (options.callbackUrl) {
      headers['X-CallbackUrl'] = options.callbackUrl;
    }
    if (options.callbackToken) {
      headers['X-CallbackToken'] = options.callbackToken;
    }
    if (options.fullStatusUpdates) {
      headers['X-FullStatusUpdates'] = 'true';
    }
    if (options.skipFeeValidation) {
      headers['X-SkipFeeValidation'] = 'true';
    }
    if (options.skipScriptValidation) {
      headers['X-SkipScriptValidation'] = 'true';
    }

    return headers;
  }

  /**
   * Makes an HTTP request to the Arcade API.
   */
  private async request(
    path: string,
    init?: RequestInit
  ): Promise<Response> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const response = await this.fetchFn(`${this.baseUrl}${path}`, {
        ...init,
        signal: controller.signal,
      });

      if (!response.ok) {
        const error = await this.parseError(response);
        throw new Error(error);
      }

      return response;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Parses an error response from the API.
   */
  private async parseError(response: Response): Promise<string> {
    try {
      const body = await response.json();
      if (body.error) {
        return `${response.status}: ${body.error}`;
      }
    } catch {
      // Ignore JSON parse errors
    }

    const text = await response.text();
    if (text) {
      return `${response.status}: ${text}`;
    }

    return `${response.status}: ${response.statusText}`;
  }
}

/**
 * SSE subscription that implements AsyncIterable.
 */
class SSESubscription implements AsyncIterable<TransactionStatus> {
  private url: string;
  private fetchFn: typeof fetch;
  private abortController: AbortController | null = null;
  private lastEventId = '';

  constructor(url: string, fetchFn: typeof fetch) {
    this.url = url;
    this.fetchFn = fetchFn;
  }

  /**
   * Cancels the subscription.
   */
  cancel(): void {
    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<TransactionStatus> {
    this.abortController = new AbortController();

    while (!this.abortController.signal.aborted) {
      try {
        const headers: Record<string, string> = {
          Accept: 'text/event-stream',
          'Cache-Control': 'no-cache',
        };

        if (this.lastEventId) {
          headers['Last-Event-ID'] = this.lastEventId;
        }

        const response = await this.fetchFn(this.url, {
          headers,
          signal: this.abortController.signal,
        });

        if (!response.ok) {
          throw new Error(`SSE connection failed: ${response.status}`);
        }

        if (!response.body) {
          throw new Error('Response body is null');
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            let currentId = '';
            let currentEvent = '';
            let currentData = '';

            for (const line of lines) {
              if (line === '') {
                // End of event
                if (currentData && currentEvent === 'status') {
                  if (currentId) {
                    this.lastEventId = currentId;
                  }
                  try {
                    const status = JSON.parse(currentData) as TransactionStatus;
                    yield status;
                  } catch {
                    // Ignore parse errors
                  }
                }
                currentId = '';
                currentEvent = '';
                currentData = '';
              } else if (line.startsWith('id:')) {
                currentId = line.slice(3).trim();
              } else if (line.startsWith('event:')) {
                currentEvent = line.slice(6).trim();
              } else if (line.startsWith('data:')) {
                currentData = line.slice(5);
              }
            }
          }
        } finally {
          reader.releaseLock();
        }
      } catch (error) {
        if (this.abortController?.signal.aborted) {
          return;
        }
        // Wait before reconnecting
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }
}

/**
 * Converts a hex string to Uint8Array.
 */
function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

/**
 * Converts a Uint8Array to hex string.
 */
function bytesToHex(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}
