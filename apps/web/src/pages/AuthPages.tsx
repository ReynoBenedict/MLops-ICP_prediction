/** Login (users + operators) and self-service registration. Validation mirrors the server rules. */
import { ArrowLeft, Check, CircleCheck, KeyRound, LogIn, UserPlus } from "lucide-react";
import { useId, useRef, useState, type FormEvent } from "react";
import { Link, Navigate, useSearchParams } from "react-router-dom";
import { ApiError } from "../api/client";
import { CONFIRMED_BASES, labelFor, loc } from "../api/product-types";
import { useAuth } from "../auth";
import { Wordmark } from "../components/brand";
import { LanguageToggle } from "../components/chrome";
import { reportFormats } from "../components/public/plans";
import { recentMonths, usePlans, usePublicSnapshot } from "../components/public/snapshot";
import { Sparkline } from "../components/public/Sparkline";
import { useToast } from "../components/toast";
import { Button, Delta, ErrorState, errorText, Field, focusFirstInvalid, Skeleton, useMediaQuery } from "../components/ui";
import { dateTime, fmtNum, fmtUsd, period } from "../format";
import { useT } from "../i18n";

const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

/** Only same-origin app paths are honoured as a post-login destination. */
export function safeNext(raw: string | null): string | null {
  if (!raw || !raw.startsWith("/") || raw.startsWith("//") || raw.includes("\\")) return null;
  if (["/masuk", "/daftar", "/"].includes(raw.split("?")[0] ?? "")) return null;
  return raw;
}

/**
 * The latest official ICP on the brand panel (same public snapshot as the landing, read through
 * the shared in-memory cache). Graceful: while loading a quiet skeleton, without data one plain
 * sentence, with the gateway's stored copy the time it was stored. No headings: the form's h1 is
 * the page heading.
 */
function AuthSnapshotCard() {
  const { t, intl, locale } = useT();
  const res = usePublicSnapshot();
  const d = res.data?.data ?? null;
  if (!d) {
    if (res.loading) return <div className="auth-card" role="status" aria-label={t("auth.side.loading")}><Skeleton height={14} width="55%" /><Skeleton height={40} width="70%" /><Skeleton height={14} width="80%" /></div>;
    return <div className="auth-card muted-card"><p>{t("auth.side.unavailable")}</p></div>;
  }
  const ev = d.icp_evidence ?? null;
  const months = recentMonths(d);
  return (
    <div className="auth-card">
      <p className="auth-card-label">{t("auth.side.label")}, {period(d.icp_period, locale, true)}</p>
      <p className="auth-card-value">{d.icp_usd == null ? "-" : <><span className="auth-card-num">{fmtNum(d.icp_usd, intl, 2)}</span> <span className="auth-card-unit">{t("landing.panel.unit")}</span></>}</p>
      {d.mom_change_pct != null && (
        <p className="auth-card-meta"><Delta value={d.mom_change_pct} />
          {d.previous_usd != null && <span>{t("auth.side.prev", { period: period(d.previous_period, locale), value: fmtUsd(d.previous_usd, intl) })}</span>}</p>
      )}
      {months.length > 1 && <Sparkline months={months} caption={t("auth.side.spark", { from: period(months[0]!.period, locale), to: period(months[months.length - 1]!.period, locale) })} />}
      {ev && <p className="auth-card-ev">{CONFIRMED_BASES.has(ev.basis) && <CircleCheck size={14} aria-hidden="true" />}{loc(ev.label, locale)}</p>}
      {res.data?.stale && <p className="auth-card-stale">{res.data.cachedAt ? t("auth.side.stale", { time: dateTime(res.data.cachedAt, intl) }) : t("auth.side.staleNoTime")}</p>}
    </div>
  );
}

/**
 * Behind the brand panel: a navy-toned photograph of an offshore oil platform at dusk (CC0, Wikimedia
 * Commons; generic, no company or place implied; docs/ASSET_INVENTORY.md), under a navy shade that keeps
 * every text on the panel at its contrast. Decorative. On a wide window it is the page's largest paint,
 * so it keeps the normal fetch priority and a small file (the 750 px AVIF is 16 kB); it is requested
 * with the page, not preloaded, because a preload delayed the form's first render on slow connections
 * (docs/DESIGN_SYSTEM.md, Imagery). The panel keeps its navy background when the image is missing.
 */
function AuthPhoto() {
  const sizes = "(min-width: 1101px) 46vw, 1px";
  return (
    <picture className="auth-photo" aria-hidden="true">
      <source type="image/avif" sizes={sizes} srcSet="/img/photo/offshore-platform-dusk-750.avif 750w, /img/photo/offshore-platform-dusk-1200.avif 1200w" />
      <img src="/img/photo/offshore-platform-dusk-1200.webp" srcSet="/img/photo/offshore-platform-dusk-750.webp 750w, /img/photo/offshore-platform-dusk-1200.webp 1200w" sizes={sizes}
        width={1200} height={1600} alt="" aria-hidden="true" decoding="async" />
    </picture>
  );
}

/**
 * Sign-in/registration frame. From 1100 px a brand panel (live ICP card and three capability
 * lines over a toned photograph) sits beside the form; on smaller screens only the form with the
 * wordmark. The brand panel has no headings, so the page's first heading is the form's H1; the form
 * column is the <main>. The live card and the photograph are only mounted while the panel is visible
 * (no request for a hidden card or image). In a forced-colours (Windows contrast) theme the photograph
 * is left out: the theme drops the gradient shade that keeps the panel's text and icons readable on it.
 */
function AuthFrame({ children }: { children: React.ReactNode }) {
  const { t } = useT();
  const wide = useMediaQuery("(min-width: 1101px)");
  const forcedColors = useMediaQuery("(forced-colors: active)");
  return (
    <div className="auth-page">
      <a href="#main" className="skip-link">{t("shell.skip")}</a>
      <aside className="auth-side">
        {wide && !forcedColors && <AuthPhoto />}
        <Link to="/" className="brand-link" aria-label={t("brand.name")}><Wordmark dark size={34} /></Link>
        <div className="auth-side-main">
          {wide && <AuthSnapshotCard />}
          <ul className="auth-caps" aria-label={t("auth.side.caps")}>
            {(["auth.side.cap1", "auth.side.cap2", "auth.side.cap3"] as const).map((k) => <li key={k}><Check size={16} aria-hidden="true" />{t(k)}</li>)}
          </ul>
        </div>
        <div className="row-between"><LanguageToggle dark /><Link to="/" className="small link-inline"><ArrowLeft size={14} aria-hidden="true" /> {t("shell.landing")}</Link></div>
      </aside>
      <main className="auth-form-wrap" id="main" tabIndex={-1}>
        <div className="auth-form">
          <div className="row-between hide-desktop auth-top">
            <Link to="/" className="brand-link" aria-label={t("brand.name")}><Wordmark size={28} /></Link>
            <LanguageToggle />
          </div>
          <Link to="/" className="small link-inline hide-desktop auth-home"><ArrowLeft size={14} aria-hidden="true" /> {t("auth.backHome")}</Link>
          {children}
          <nav className="auth-legal small" aria-label={t("landing.footer.legal")}>
            <Link to="/syarat">{t("landing.footer.terms")}</Link>
            <Link to="/privasi">{t("landing.footer.privacy")}</Link>
            <Link to="/metodologi">{t("landing.nav.data")}</Link>
          </nav>
        </div>
      </main>
    </div>
  );
}

/**
 * What the plan a new organisation starts on includes (from /plans). While the list loads, a
 * skeleton of the same shape holds its place, so the form fields below never move under a visitor
 * who has started typing; nothing when the list is unavailable. A stored gateway copy says so.
 */
function TrialFacts() {
  const { t, intl, locale } = useT();
  const res = usePlans();
  const trial = res.data?.data.find((p) => p.code === "trial") ?? null;
  if (!trial) {
    if (!res.loading || res.data) return null;
    return (
      <div className="trial-facts trial-loading" role="status" aria-label={t("auth.trial.loading")}>
        <Skeleton height={14} width="45%" />
        <ul aria-hidden="true">{[0, 1, 2, 3].map((i) => <li key={i}><Skeleton height={14} width={i % 2 ? "62%" : "74%"} /></li>)}</ul>
      </div>
    );
  }
  const list = reportFormats(trial);
  let formats = list.join(", ");
  try { formats = new Intl.ListFormat(intl, { type: "conjunction" }).format(list); } catch { /* older engines: comma list */ }
  const cachedAt = res.data?.cachedAt ?? null;
  return (
    <div className="trial-facts">
      <p className="trial-title">{t("auth.trial.title", { plan: labelFor(trial, locale) })}</p>
      <ul>
        <li>{t("landing.plan.seats", { n: fmtNum(trial.seats, intl, 0) })}</li>
        <li>{t("landing.plan.horizon", { n: trial.forecast_horizon_max })}</li>
        <li>{trial.history_years ? t("landing.plan.history", { n: trial.history_years }) : t("landing.plan.historyAll")}</li>
        <li>{t("auth.trial.reports", { formats })}</li>
      </ul>
      {res.data?.stale && <p className="trial-stale">{cachedAt ? t("auth.side.stale", { time: dateTime(cachedAt, intl) }) : t("auth.side.staleNoTime")}</p>}
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
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const formRef = useRef<HTMLFormElement>(null);
  const next = safeNext(params.get("next"));

  if (ready && principal.kind !== "public") return <Navigate to={next ?? (principal.kind === "operator" ? "/operasi" : "/ringkasan")} replace />;

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    const found: Record<string, string> = {};
    if (mode === "user") {
      if (!email.trim()) found.email = t("auth.fieldRequired");
      else if (!EMAIL_RE.test(email.trim())) found.email = t("auth.emailInvalid");
      if (!password) found.password = t("auth.fieldRequired");
    } else if (!token.trim()) found.token = t("auth.fieldRequired");
    setErrors(found);
    if (Object.keys(found).length) { requestAnimationFrame(() => focusFirstInvalid(formRef.current)); return; }
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
  const nErrors = Object.keys(errors).length;

  return (
    <AuthFrame>
      <form onSubmit={submit} noValidate ref={formRef}>
        <div className="stack">
          <div>
            <h1>{mode === "user" ? t("auth.loginTitle") : t("auth.operatorTitle")}</h1>
            <p className="muted small mt-2">{mode === "user" ? t("auth.loginLead") : t("auth.operatorLead")}</p>
            {next && <p className="muted small mt-2">{t("auth.nextNote")}</p>}
          </div>
          {operatorOffered && (
            <div className="segmented" role="group" aria-label={t("shell.signin")}>
              <button type="button" aria-pressed={mode === "user"} onClick={() => { setMode("user"); setErrors({}); }}><LogIn size={14} aria-hidden="true" />{t("auth.modeUser")}</button>
              <button type="button" aria-pressed={mode === "operator"} onClick={() => { setMode("operator"); setErrors({}); }}><KeyRound size={14} aria-hidden="true" />{t("shell.operator")}</button>
            </div>
          )}
          {mode === "user" ? (
            <>
              <Field label={t("auth.email")} error={errors.email} required render={(p) => (
                <input {...p} className="input" type="email" autoComplete="username" value={email} onChange={(e) => setEmail(e.target.value)} required />)} />
              <Field label={t("auth.password")} hint={t("auth.forgotHint")} error={errors.password} required render={(p) => (
                <input {...p} className="input" type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} required />)} />
            </>
          ) : (
            <Field label={t("auth.operatorToken")} error={errors.token} required render={(p) => (
              <input {...p} className="input mono" type="password" autoComplete="off" value={token} onChange={(e) => setToken(e.target.value)} placeholder={t("auth.operatorPlaceholder")} required />)} />
          )}
          <div className="sr-only" role="status" aria-live="polite">{nErrors ? t("form.fixFields", { n: nErrors }) : ""}</div>
          <ErrorState error={error} compact signinLink={false} />
          <Button type="submit" variant="primary" size="lg" icon={LogIn} loading={busy}>{t("auth.login")}</Button>
          {mode === "user" && <p className="small muted">{t("auth.noAccount")} <Link to={next ? `/daftar?next=${encodeURIComponent(next)}` : "/daftar"}>{t("shell.register")}</Link></p>}
        </div>
      </form>
    </AuthFrame>
  );
}

export function RegisterPage() {
  const { t, locale } = useT();
  const { register, principal, ready } = useAuth();
  const [params] = useSearchParams();
  const next = safeNext(params.get("next"));
  const { show } = useToast();
  const [form, setForm] = useState({ organization_name: "", name: "", email: "", password: "", accept_terms: false });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [error, setError] = useState<unknown>(null);
  const [busy, setBusy] = useState(false);
  const formRef = useRef<HTMLFormElement>(null);
  const termsErrId = useId();
  if (ready && principal.kind !== "public") return <Navigate to={next ?? "/ringkasan"} replace />;

  const validate = () => {
    const e: Record<string, string> = {};
    if (form.organization_name.trim().length < 2) e.organization_name = t("auth.fieldRequired");
    if (!form.name.trim()) e.name = t("auth.fieldRequired");
    if (!form.email.trim()) e.email = t("auth.fieldRequired");
    else if (!EMAIL_RE.test(form.email.trim())) e.email = t("auth.emailInvalid");
    if (form.password.length < 10) e.password = t("auth.passwordShort");
    if (!form.accept_terms) e.accept_terms = t("auth.termsRequired");
    setErrors(e);
    return Object.keys(e).length === 0;
  };
  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    // Invalid: move focus to the first marked field (its error is read as the field's description).
    if (!validate()) { requestAnimationFrame(() => focusFirstInvalid(formRef.current)); return; }
    setBusy(true);
    // Success: the principal changes and the guard above redirects (replace) - no timer to leak.
    try { await register(form); show(t("toast.registered"), "good"); } catch (err) {
      setBusy(false);
      // Errors about one field are shown on that field (server wording when it is in this language).
      const field = err instanceof ApiError ? (err.code === "EMAIL_TAKEN" ? "email" : err.code === "WEAK_PASSWORD" ? "password" : null) : null;
      if (field) { setErrors({ [field]: errorText(err, t, locale).body }); requestAnimationFrame(() => focusFirstInvalid(formRef.current)); }
      else setError(err);
    }
  };
  const set = (k: keyof typeof form) => (ev: React.ChangeEvent<HTMLInputElement>) => setForm({ ...form, [k]: k === "accept_terms" ? ev.target.checked : ev.target.value });
  const nErrors = Object.keys(errors).length;

  return (
    <AuthFrame>
      <form onSubmit={submit} noValidate ref={formRef}>
        <div className="stack">
          <div><h1>{t("auth.registerTitle")}</h1><p className="muted small mt-2">{t("auth.registerLead")}</p></div>
          <TrialFacts />
          <Field label={t("auth.orgName")} error={errors.organization_name} required render={(p) => (
            <input {...p} className="input" value={form.organization_name} onChange={set("organization_name")} required maxLength={120} autoComplete="organization" />)} />
          <Field label={t("auth.fullName")} error={errors.name} required render={(p) => (
            <input {...p} className="input" value={form.name} onChange={set("name")} required maxLength={120} autoComplete="name" />)} />
          <Field label={t("auth.email")} error={errors.email} required render={(p) => (
            <input {...p} className="input" type="email" value={form.email} onChange={set("email")} required autoComplete="email" />)} />
          <Field label={t("auth.password")} hint={t("auth.passwordHint")} error={errors.password} required render={(p) => (
            <input {...p} className="input" type="password" value={form.password} onChange={set("password")} required minLength={10} autoComplete="new-password" />)} />
          <label className="check"><input type="checkbox" checked={form.accept_terms} onChange={set("accept_terms")}
            aria-invalid={errors.accept_terms ? true : undefined} aria-describedby={errors.accept_terms ? termsErrId : undefined} />
            <span>{t("auth.acceptPrefix")} <Link to="/syarat" target="_blank">{t("landing.footer.terms")}</Link> {t("common.and")} <Link to="/privasi" target="_blank">{t("landing.footer.privacy")}</Link>.</span></label>
          {errors.accept_terms && <span className="field-error" id={termsErrId}>{errors.accept_terms}</span>}
          <div className="sr-only" role="status" aria-live="polite">{nErrors ? t("form.fixFields", { n: nErrors }) : ""}</div>
          <ErrorState error={error} compact titles={{ CONFLICT: t("error.registerConflict") }} />
          <Button type="submit" variant="primary" size="lg" icon={UserPlus} loading={busy}>{t("auth.register")}</Button>
          <p className="small muted">{t("auth.hasAccount")} <Link to={next ? `/masuk?next=${encodeURIComponent(next)}` : "/masuk"}>{t("shell.signin")}</Link></p>
        </div>
      </form>
    </AuthFrame>
  );
}
