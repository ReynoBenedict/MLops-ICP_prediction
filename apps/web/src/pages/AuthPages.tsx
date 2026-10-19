/** Login (users + operators) and self-service registration. Validation mirrors the server rules. */
import { ArrowLeft, KeyRound, LogIn, UserPlus } from "lucide-react";
import { useState, type FormEvent } from "react";
import { Link, Navigate, useSearchParams } from "react-router-dom";
import { ApiError } from "../api/client";
import { useAuth } from "../auth";
import { Wordmark } from "../components/brand";
import { IllPipeline } from "../components/illustrations";
import { LanguageToggle } from "../components/layout";
import { useToast } from "../components/toast";
import { Button, ErrorState } from "../components/ui";
import { useT } from "../i18n";

const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

/** Only same-origin app paths are honoured as a post-login destination. */
export function safeNext(raw: string | null): string | null {
  if (!raw || !raw.startsWith("/") || raw.startsWith("//") || raw.includes("\\")) return null;
  if (["/masuk", "/daftar", "/"].includes(raw.split("?")[0] ?? "")) return null;
  return raw;
}

/**
 * Sign-in/registration frame. The brand panel is decorative context (no headings, so the page's
 * first heading is the form's H1); the form column is the page's <main>.
 */
function AuthFrame({ children, side }: { children: React.ReactNode; side: React.ReactNode }) {
  const { t } = useT();
  return (
    <div className="auth-page">
      <a href="#main" className="skip-link">{t("shell.skip")}</a>
      <aside className="auth-side">
        <Link to="/" style={{ textDecoration: "none" }} aria-label={t("brand.name")}><Wordmark dark size={34} /></Link>
        {side}
        <div className="row-between"><LanguageToggle dark /><Link to="/" className="small link-inline" style={{ color: "#c9d1dd" }}><ArrowLeft size={14} aria-hidden="true" /> {t("shell.landing")}</Link></div>
      </aside>
      <main className="auth-form-wrap" id="main" tabIndex={-1}>
        <div className="auth-form">
          <div className="row-between hide-desktop" style={{ marginBottom: 4 }}>
            <Link to="/" style={{ textDecoration: "none" }} aria-label={t("brand.name")}><Wordmark size={28} /></Link>
            <LanguageToggle />
          </div>
          {children}
        </div>
      </main>
    </div>
  );
}

export function LoginPage() {
  const { t } = useT();
  const { login, signInOperator, principal, ready } = useAuth();
  const [params] = useSearchParams();
  const { show } = useToast();
  // The operator token form is only offered when asked for (/masuk?operator=1, used by /operasi links):
  // customers never see the operator path.
  const operatorOffered = !!params.get("operator");
  const [mode, setMode] = useState<"user" | "operator">(operatorOffered ? "operator" : "user");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [token, setToken] = useState("");
  const [error, setError] = useState<unknown>(null);
  const [fieldError, setFieldError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const next = safeNext(params.get("next"));

  if (ready && principal.kind !== "public") return <Navigate to={next ?? (principal.kind === "operator" ? "/operasi" : "/ringkasan")} replace />;

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null); setFieldError(null);
    if (mode === "user") {
      if (!EMAIL_RE.test(email.trim())) { setFieldError(t("auth.emailInvalid")); return; }
      if (!password) { setFieldError(t("auth.fieldRequired")); return; }
    } else if (!token.trim()) { setFieldError(t("auth.fieldRequired")); return; }
    setBusy(true);
    try {
      // No explicit navigate(): once the principal changes this page renders <Navigate replace>
      // to `next`, so the sign-in page is not left in the history (Back skips it).
      if (mode === "user") { const res = await login(email, password); show(t("toast.welcome", { name: res.user.name }), "good"); }
      else { await signInOperator(token); }
    } catch (err) {
      setError(err instanceof ApiError && err.status === 401 && mode === "operator" ? new ApiError(401, "AUTH_INVALID", t("auth.invalidToken"), err.requestId) : err);
    } finally { setBusy(false); }
  };

  return (
    <AuthFrame side={<div className="stack"><IllPipeline size={240} /><p className="auth-side-title">{t("landing.title")}</p><p>{t("landing.f2.body")}</p></div>}>
      <form onSubmit={submit} noValidate>
        <div className="stack">
          <div>
            <h1>{mode === "user" ? t("auth.loginTitle") : t("auth.operatorTitle")}</h1>
            <p className="muted small mt-2">{mode === "user" ? t("auth.loginLead") : t("auth.operatorLead")}</p>
            {next && <p className="muted small mt-2">{t("auth.nextNote")}</p>}
          </div>
          {operatorOffered && (
            <div className="segmented" role="group" aria-label={t("shell.signin")}>
              <button type="button" aria-pressed={mode === "user"} onClick={() => setMode("user")}><LogIn size={14} aria-hidden="true" />{t("auth.modeUser")}</button>
              <button type="button" aria-pressed={mode === "operator"} onClick={() => setMode("operator")}><KeyRound size={14} aria-hidden="true" />{t("shell.operator")}</button>
            </div>
          )}
          {mode === "user" ? (
            <>
              <label className="field"><span className="field-label">{t("auth.email")}</span>
                <input className="input" type="email" autoComplete="username" value={email} onChange={(e) => setEmail(e.target.value)} required /></label>
              <label className="field"><span className="field-label">{t("auth.password")}</span>
                <input className="input" type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} required /></label>
            </>
          ) : (
            <label className="field"><span className="field-label">{t("auth.operatorToken")}</span>
              <input className="input mono" type="password" autoComplete="off" value={token} onChange={(e) => setToken(e.target.value)} placeholder={t("auth.operatorPlaceholder")} required /></label>
          )}
          {fieldError && <span className="field-error" role="alert">{fieldError}</span>}
          <ErrorState error={error} compact />
          <Button type="submit" variant="primary" size="lg" icon={LogIn} loading={busy}>{t("auth.login")}</Button>
          {mode === "user" && <p className="small muted">{t("auth.noAccount")} <Link to={next ? `/daftar?next=${encodeURIComponent(next)}` : "/daftar"}>{t("shell.register")}</Link></p>}
        </div>
      </form>
    </AuthFrame>
  );
}

export function RegisterPage() {
  const { t } = useT();
  const { register, principal, ready } = useAuth();
  const [params] = useSearchParams();
  const next = safeNext(params.get("next"));
  const { show } = useToast();
  const [form, setForm] = useState({ organization_name: "", name: "", email: "", password: "", accept_terms: false });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [error, setError] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);
  if (ready && principal.kind !== "public") return <Navigate to={next ?? "/ringkasan"} replace />;

  const validate = () => {
    const e: Record<string, string> = {};
    if (form.organization_name.trim().length < 2) e.organization_name = t("auth.fieldRequired");
    if (!form.name.trim()) e.name = t("auth.fieldRequired");
    if (!EMAIL_RE.test(form.email.trim())) e.email = t("auth.emailInvalid");
    if (form.password.length < 10) e.password = t("auth.passwordShort");
    if (!form.accept_terms) e.accept_terms = t("auth.termsRequired");
    setErrors(e);
    return Object.keys(e).length === 0;
  };
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    if (!validate()) return;
    setBusy(true);
    // Success: the principal changes and the guard above redirects (replace) - no timer to leak.
    try { await register(form); show(t("toast.registered"), "good"); } catch (err) { setError(err); setBusy(false); }
  };
  const set = (k: keyof typeof form) => (ev: React.ChangeEvent<HTMLInputElement>) => setForm({ ...form, [k]: k === "accept_terms" ? ev.target.checked : ev.target.value });

  return (
    <AuthFrame side={<div className="stack"><IllPipeline size={240} /><p className="auth-side-title">{t("landing.featuresTitle")}</p><p>{t("landing.plansLead")}</p></div>}>
      <form onSubmit={submit} noValidate>
        <div className="stack">
          <div><h1>{t("auth.registerTitle")}</h1><p className="muted small mt-2">{t("auth.registerLead")}</p></div>
          <label className="field"><span className="field-label">{t("auth.orgName")}</span>
            <input className="input" value={form.organization_name} onChange={set("organization_name")} required maxLength={120} />
            {errors.organization_name && <span className="field-error">{errors.organization_name}</span>}</label>
          <label className="field"><span className="field-label">{t("auth.fullName")}</span>
            <input className="input" value={form.name} onChange={set("name")} required maxLength={120} autoComplete="name" />
            {errors.name && <span className="field-error">{errors.name}</span>}</label>
          <label className="field"><span className="field-label">{t("auth.email")}</span>
            <input className="input" type="email" value={form.email} onChange={set("email")} required autoComplete="email" />
            {errors.email && <span className="field-error">{errors.email}</span>}</label>
          <label className="field"><span className="field-label">{t("auth.password")}</span>
            <input className="input" type="password" value={form.password} onChange={set("password")} required minLength={10} autoComplete="new-password" />
            <span className="field-hint">{t("auth.passwordHint")}</span>
            {errors.password && <span className="field-error">{errors.password}</span>}</label>
          <label className="check"><input type="checkbox" checked={form.accept_terms} onChange={set("accept_terms")} />
            <span>{t("auth.acceptPrefix")} <Link to="/syarat" target="_blank">{t("landing.footer.terms")}</Link> {t("common.and")} <Link to="/privasi" target="_blank">{t("landing.footer.privacy")}</Link>.</span></label>
          {errors.accept_terms && <span className="field-error" role="alert">{errors.accept_terms}</span>}
          <ErrorState error={error} compact />
          <Button type="submit" variant="primary" size="lg" icon={UserPlus} loading={busy}>{t("auth.register")}</Button>
          <p className="small muted"><Link to="/metodologi">{t("nav.methodology")}</Link></p>
          <p className="small muted">{t("auth.hasAccount")} <Link to={next ? `/masuk?next=${encodeURIComponent(next)}` : "/masuk"}>{t("shell.signin")}</Link></p>
        </div>
      </form>
    </AuthFrame>
  );
}
