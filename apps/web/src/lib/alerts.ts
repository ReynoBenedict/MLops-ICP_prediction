/**
 * One unread-notification signal for the whole app. Anything that changes alert rules or events
 * (create, delete, evaluate, mark read) calls `notifyAlertsChanged()`; the shell badge and the
 * plan counters listen and reload at once instead of waiting for the 60 s poll.
 */
export const ALERTS_CHANGED_EVENT = "icp:alerts-changed";

export function notifyAlertsChanged(): void {
  try { window.dispatchEvent(new Event(ALERTS_CHANGED_EVENT)); } catch { /* non-browser */ }
}

/** Subscribe to alert changes; returns the unsubscribe function. */
export function onAlertsChanged(fn: () => void): () => void {
  window.addEventListener(ALERTS_CHANGED_EVENT, fn);
  return () => window.removeEventListener(ALERTS_CHANGED_EVENT, fn);
}
