/**
 * Transport error shape. Kept separate from the client so a caller can catch
 * `ApiError` without pulling the request machinery into its module graph.
 */

/** Error envelope a server may return alongside a non-2xx status. */
export interface ApiErrorBody {
  error?: string;
  code?: string;
  details?: unknown;
  message?: string;
}

/**
 * A non-2xx response, carrying the status and the server's error envelope.
 *
 * The fields are declared and assigned explicitly rather than written as
 * constructor parameter properties: those are TypeScript sugar that EMITS code,
 * so a runtime that only strips types (`node --experimental-strip-types`)
 * rejects them outright. Spelling them out keeps this class loadable anywhere.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details?: unknown;

  constructor(message: string, status: number, code = 'API_ERROR', details?: unknown) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

const HTML_PAGE = /<\s*(?:!doctype|html|head|body)\b/i;

/**
 * A server's error message as a person can read it. A proxy or WAF that refuses a request answers
 * with an HTML page, and a server relaying it put that page in its message: osionos showed
 * nginx's `<html>…403 Forbidden…</html>` in a toast and the console (osionos#287). Such a message
 * becomes one sentence with the status; any other message is returned unchanged.
 */
export function readableServerMessage(message: string, status: number): string {
  if (!HTML_PAGE.test(message)) return message;
  return status === 403
    ? 'The server\'s request filter refused this request (403).'
    : `The server answered ${status} with an error page instead of a message.`;
}

