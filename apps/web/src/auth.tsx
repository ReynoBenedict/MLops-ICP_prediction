/**
 * Authentication state. The server decides everything: on load the stored credential is re-verified
 * with /auth/whoami; the UI only mirrors the principal (kind, role, plan, horizon_max) it returns.
 */
import type { AuthResponse, WhoAmI } from "./api/types";
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { api, ApiError, AUTH_EXPIRED_EVENT, PASSWORD_CHANGE_EVENT, TIMEOUTS, getOperatorToken, getSessionToken, setOperatorToken, setSessionToken } from "./api/client";
import { useToast } from "./components/toast";
import { useT } from "./i18n";

export interface Principal {
  kind: "public" | "user" | "operator";
  role: WhoAmI["role"];
  name: string | null;
  orgId: string | null;
  orgName: string | null;
  plan: string | null;
  horizonMax: number;
  mustChangePassword: boolean;
}

export const ANONYMOUS: Principal = { kind: "public", role: "public", name: null, orgId: null, orgName: null, plan: null, horizonMax: 3, mustChangePassword: false };

interface AuthState {
  principal: Principal;
  ready: boolean;
  /**
   * Why a stored credential could not be checked, when the server gave no definite answer (timeout,
   * 5xx, offline); otherwise null. The principal is then the guest principal, but the credential is
   * kept and re-checked in the background, so workspace routes explain the situation instead of
   * asking the member to sign in again.
   */
  sessionCheckError: unknown;
  isUser: boolean;
  isOperator: boolean;
  /** Minimum-role check for organisation members (operators always pass). */
  hasRole: (minimum: "viewer" | "analyst" | "admin" | "owner") => boolean;
  signInOperator: (token: string) => Promise<void>;
  login: (email: string, password: string) => Promise<AuthResponse>;
  register: (body: { organization_name: string; email: string; name: string; password: string; accept_terms: boolean }) => Promise<AuthResponse>;
  /**
   * Ends the session on the server, then clears the client. Resolves when the session is over
   * (also when the server says the token was already invalid). Rejects - leaving the session in
   * place - when the server could not be reached, so the UI never pretends to have signed out.
   */
  signOut: (leave?: () => void | Promise<void>) => Promise<void>;
  refresh: () => Promise<void>;
}

const RANK = { public: -1, viewer: 0, analyst: 1, admin: 2, owner: 3, operator: 4 } as const;

function toPrincipal(w: WhoAmI): Principal {
  return { kind: w.kind, role: w.role, name: w.name, orgId: w.org_id ?? null, orgName: w.org_name ?? null, plan: w.plan ?? null,
    horizonMax: w.horizon_max, mustChangePassword: w.must_change_password };
}

/**
 * Background re-checks of a credential the server could not confirm: after these delays (the last
 * one repeats), only while the page is visible, and at once when the browser comes back online or
 * the tab becomes visible again.
 */
export const SESSION_RECHECK_DELAYS_MS = [5_000, 15_000, 30_000, 60_000] as const;

const AuthContext = createContext<AuthState>({
  principal: ANONYMOUS, ready: true, sessionCheckError: null, isUser: false, isOperator: false, hasRole: () => false,
  signInOperator: async () => undefined, login: async () => { throw new Error("no provider"); }, register: async () => { throw new Error("no provider"); },
  signOut: async () => undefined, refresh: async () => undefined,
});

/** Per-tab caches of organisation data (e.g. scenario comparisons) must not outlive the session. */
function clearSessionCaches(): void {
  try {
    for (const k of Object.keys(sessionStorage)) if (k.startsWith("icp.compare.")) sessionStorage.removeItem(k);
  } catch { /* storage unavailable */ }
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [principal, setPrincipal] = useState<Principal>(ANONYMOUS);
  const [ready, setReady] = useState(false);
  const [sessionCheckError, setSessionCheckError] = useState<unknown>(null);
  // Every check gets a number and only the newest may change the state: a slow background re-check
  // must never overwrite the result of a later sign-in, retry or sign-out.
  const checkSeq = useRef(0);

  const refresh = useCallback(async () => {
    const seq = ++checkSeq.current;
    const current = () => seq === checkSeq.current;
    const settle = (next: Principal, failure: unknown = null) => {
      if (!current()) return;
      setPrincipal(next); setSessionCheckError(failure); setReady(true);
    };
    if (!getOperatorToken() && !getSessionToken()) { settle(ANONYMOUS); return; }
    // The whole check, retry included, ends within the whoami timeout: a hanging or failing backend
    // never holds the app on its boot skeleton longer than that. Only a 401 forgets the credential;
    // after a timeout, 5xx or network failure the app continues as a guest, the stored credential is
    // kept and `sessionCheckError` says why (it is re-checked in the background, see below).
    const deadline = Date.now() + TIMEOUTS.whoami;
    try {
      // A rate limit or a restarting server is not a signed-out user: retry once if time is left.
      const who = await api<WhoAmI>("/auth/whoami", { timeoutMs: TIMEOUTS.whoami }).catch(async (e: unknown) => {
        if (e instanceof ApiError && (e.status === 429 || e.status >= 500)) {
          const wait = Math.min(5000, (Number(e.details?.retry_after_seconds) || 1.5) * 1000);
          const left = deadline - Date.now() - wait;
          if (left >= 1000) {
            await new Promise((r) => setTimeout(r, wait));
            return api<WhoAmI>("/auth/whoami", { timeoutMs: left });
          }
        }
        throw e;
      });
      settle(who.authenticated ? toPrincipal(who) : ANONYMOUS);
    } catch (e) {
      const definite = e instanceof ApiError && e.status === 401;
      if (definite && current()) { setOperatorToken(null); setSessionToken(null); }
      settle(ANONYMOUS, definite ? null : e);
    }
  }, []);

  useEffect(() => { void refresh(); }, [refresh]);

  // A credential that could not be checked is re-checked in the background (SESSION_RECHECK_DELAYS_MS),
  // so the app recovers by itself once the server answers again, without a reload.
  const recheckAttempt = useRef(0);
  useEffect(() => {
    if (!sessionCheckError) { recheckAttempt.current = 0; return; }
    const delay = SESSION_RECHECK_DELAYS_MS[Math.min(recheckAttempt.current, SESSION_RECHECK_DELAYS_MS.length - 1)];
    let started = false;
    const recheck = () => {
      if (started || document.visibilityState === "hidden") return;
      started = true;
      recheckAttempt.current += 1;
      void refresh();
    };
    const timer = window.setTimeout(recheck, delay);
    const onReturn = () => { if (document.visibilityState !== "hidden") recheck(); };
    window.addEventListener("online", onReturn);
    document.addEventListener("visibilitychange", onReturn);
    return () => {
      window.clearTimeout(timer);
      window.removeEventListener("online", onReturn);
      document.removeEventListener("visibilitychange", onReturn);
    };
  }, [sessionCheckError, refresh]);

  /** The state was decided outside refresh(): checks still in flight must not overwrite it. */
  const decided = useCallback((next: Principal) => {
    checkSeq.current += 1;
    setSessionCheckError(null);
    setPrincipal(next);
  }, []);

  const signInOperator = useCallback(async (token: string) => {
    setOperatorToken(token.trim());
    try {
      const who = await api<WhoAmI>("/auth/whoami");
      if (!who.authenticated || who.kind !== "operator") throw new ApiError(401, "AUTH_INVALID", "Invalid credentials.", null);
      decided(toPrincipal(who));
    } catch (e) {
      setOperatorToken(null);
      await refresh();
      throw e;
    }
  }, [refresh, decided]);

  const login = useCallback(async (email: string, password: string) => {
    const res = await api<AuthResponse>("/auth/login", { method: "POST", json: { email, password }, anonymous: true });
    setOperatorToken(null);
    setSessionToken(res.token);
    await refresh();
    return res;
  }, [refresh]);

  const register = useCallback(async (body: { organization_name: string; email: string; name: string; password: string; accept_terms: boolean }) => {
    const res = await api<AuthResponse>("/auth/register", { method: "POST", json: body, anonymous: true });
    setOperatorToken(null);
    setSessionToken(res.token);
    await refresh();
    return res;
  }, [refresh]);

  // `leave` runs after the server ended the session and before the client state is cleared, so the
  // caller can move to a public page first (otherwise the private-route guard would redirect to sign-in).
  const signOut = useCallback(async (leave?: () => void | Promise<void>) => {
    if (getSessionToken() && !getOperatorToken()) {
      try {
        await api("/auth/logout", { method: "POST", quietAuth: true });
      } catch (e) {
        // 401: the session was already over on the server - finishing locally is truthful.
        if (!(e instanceof ApiError && e.status === 401)) throw e;
      }
    }
    await leave?.();
    setOperatorToken(null);
    setSessionToken(null);
    clearSessionCaches();
    decided(ANONYMOUS);
  }, [decided]);

  // A request was rejected with 401 although a credential is stored: re-check with the server.
  const { show } = useToast();
  const { t } = useT();
  useEffect(() => {
    let checking = false;
    const onExpired = async () => {
      if (checking || (!getOperatorToken() && !getSessionToken())) return;
      checking = true;
      try {
        // Only a definite answer ends the session: a timeout or an unreachable server is not a sign-out.
        const who = await api<WhoAmI>("/auth/whoami").catch((e: unknown) => (e instanceof ApiError && e.status === 401 ? null : undefined));
        if (who !== undefined && !who?.authenticated) {
          setOperatorToken(null); setSessionToken(null); clearSessionCaches(); decided(ANONYMOUS);
          show(t("toast.sessionExpired"), "info");
        }
      } finally { checking = false; }
    };
    window.addEventListener(AUTH_EXPIRED_EVENT, onExpired);
    return () => window.removeEventListener(AUTH_EXPIRED_EVENT, onExpired);
  }, [show, t, decided]);

  // The server refused a request because the member still has a temporary password: mirror that in
  // the principal (the route guard then sends the member to /akun#keamanan) and say why, once.
  // The update is functional so it never depends on a ref that is only synced after render (a 403
  // arriving right after the first render used to be ignored); the toast follows the state change.
  const passwordNoticePending = useRef(false);
  useEffect(() => {
    const onPasswordChange = () => {
      setPrincipal((p) => {
        if (p.kind !== "user" || p.mustChangePassword) return p;
        passwordNoticePending.current = true;
        return { ...p, mustChangePassword: true };
      });
    };
    window.addEventListener(PASSWORD_CHANGE_EVENT, onPasswordChange);
    return () => window.removeEventListener(PASSWORD_CHANGE_EVENT, onPasswordChange);
  }, []);
  useEffect(() => {
    if (principal.mustChangePassword && passwordNoticePending.current) {
      passwordNoticePending.current = false;
      show(t("shell.passwordRequired"), "info");
    }
  }, [principal.mustChangePassword, show, t]);

  const value = useMemo<AuthState>(() => ({
    principal, ready, sessionCheckError, isUser: principal.kind === "user", isOperator: principal.kind === "operator",
    hasRole: (minimum) => RANK[principal.role] >= RANK[minimum],
    signInOperator, login, register, signOut, refresh,
  }), [principal, ready, sessionCheckError, signInOperator, login, register, signOut, refresh]);
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthState {
  return useContext(AuthContext);
}
