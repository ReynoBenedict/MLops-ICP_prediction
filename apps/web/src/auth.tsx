/**
 * Authentication state. The server decides everything: on load the stored credential is re-verified
 * with /auth/whoami; the UI only mirrors the principal (kind, role, plan, horizon_max) it returns.
 */
import type { AuthResponse, WhoAmI } from "./api/types";
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { api, ApiError, AUTH_EXPIRED_EVENT, PASSWORD_CHANGE_EVENT, getOperatorToken, getSessionToken, setOperatorToken, setSessionToken } from "./api/client";
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

const AuthContext = createContext<AuthState>({
  principal: ANONYMOUS, ready: true, isUser: false, isOperator: false, hasRole: () => false,
  signInOperator: async () => undefined, login: async () => { throw new Error("no provider"); }, register: async () => { throw new Error("no provider"); },
  signOut: async () => undefined, refresh: async () => undefined,
});

export function AuthProvider({ children }: { children: ReactNode }) {
  const [principal, setPrincipal] = useState<Principal>(ANONYMOUS);
  const [ready, setReady] = useState(false);

  const refresh = useCallback(async () => {
    if (!getOperatorToken() && !getSessionToken()) { setPrincipal(ANONYMOUS); setReady(true); return; }
    try {
      const who = await api<WhoAmI>("/auth/whoami");
      setPrincipal(who.authenticated ? toPrincipal(who) : ANONYMOUS);
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) { setOperatorToken(null); setSessionToken(null); }
      setPrincipal(ANONYMOUS);
    } finally {
      setReady(true);
    }
  }, []);

  useEffect(() => { void refresh(); }, [refresh]);

  const signInOperator = useCallback(async (token: string) => {
    setOperatorToken(token.trim());
    try {
      const who = await api<WhoAmI>("/auth/whoami");
      if (!who.authenticated || who.kind !== "operator") throw new ApiError(401, "AUTH_INVALID", "Invalid credentials.", null);
      setPrincipal(toPrincipal(who));
    } catch (e) {
      setOperatorToken(null);
      await refresh();
      throw e;
    }
  }, [refresh]);

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
        await api("/auth/logout", { method: "POST" });
      } catch (e) {
        // 401: the session was already over on the server - finishing locally is truthful.
        if (!(e instanceof ApiError && e.status === 401)) throw e;
      }
    }
    await leave?.();
    setOperatorToken(null);
    setSessionToken(null);
    setPrincipal(ANONYMOUS);
  }, []);

  // A request was rejected with 401 although a credential is stored: re-check with the server.
  const { show } = useToast();
  const { t } = useT();
  useEffect(() => {
    let checking = false;
    const onExpired = async () => {
      if (checking || (!getOperatorToken() && !getSessionToken())) return;
      checking = true;
      try {
        const who = await api<WhoAmI>("/auth/whoami").catch(() => null);
        if (!who?.authenticated) {
          setOperatorToken(null); setSessionToken(null); setPrincipal(ANONYMOUS);
          show(t("toast.sessionExpired"), "info");
        }
      } finally { checking = false; }
    };
    window.addEventListener(AUTH_EXPIRED_EVENT, onExpired);
    return () => window.removeEventListener(AUTH_EXPIRED_EVENT, onExpired);
  }, [show, t]);

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
    principal, ready, isUser: principal.kind === "user", isOperator: principal.kind === "operator",
    hasRole: (minimum) => RANK[principal.role] >= RANK[minimum],
    signInOperator, login, register, signOut, refresh,
  }), [principal, ready, signInOperator, login, register, signOut, refresh]);
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthState {
  return useContext(AuthContext);
}
