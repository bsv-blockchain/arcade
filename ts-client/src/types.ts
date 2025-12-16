/**
 * Transaction status values.
 */
export type Status =
  | 'UNKNOWN'
  | 'RECEIVED'
  | 'SENT_TO_NETWORK'
  | 'ACCEPTED_BY_NETWORK'
  | 'SEEN_ON_NETWORK'
  | 'DOUBLE_SPEND_ATTEMPTED'
  | 'REJECTED'
  | 'MINED'
  | 'IMMUTABLE';

/**
 * Represents the current status of a transaction.
 */
export interface TransactionStatus {
  /** Transaction ID (hex string) */
  txid: string;
  /** Current status */
  txStatus: Status;
  /** Timestamp of last status update */
  timestamp: string;
  /** Block hash (if mined) */
  blockHash?: string;
  /** Block height (if mined) */
  blockHeight?: number;
  /** Merkle path (hex string, if mined) */
  merklePath?: string;
  /** Additional information */
  extraInfo?: string;
  /** Competing transaction IDs (if double spend) */
  competingTxs?: string[];
}

/**
 * Options for transaction submission.
 */
export interface SubmitOptions {
  /** Webhook URL for status callbacks */
  callbackUrl?: string;
  /** Token for SSE event filtering */
  callbackToken?: string;
  /** Send all status updates (not just final) */
  fullStatusUpdates?: boolean;
  /** Skip fee validation */
  skipFeeValidation?: boolean;
  /** Skip script validation */
  skipScriptValidation?: boolean;
}

/**
 * Transaction policy configuration.
 */
export interface Policy {
  /** Maximum script size in bytes */
  maxscriptsizepolicy: number;
  /** Maximum signature operations count */
  maxtxsigopscountspolicy: number;
  /** Maximum transaction size in bytes */
  maxtxsizepolicy: number;
  /** Mining fee rate - bytes */
  miningFeeBytes: number;
  /** Mining fee rate - satoshis */
  miningFeeSatoshis: number;
}

/**
 * Client configuration options.
 */
export interface ClientOptions {
  /** Request timeout in milliseconds */
  timeout?: number;
  /** Custom fetch implementation */
  fetch?: typeof fetch;
}
