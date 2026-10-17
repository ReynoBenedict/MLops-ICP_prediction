/**
 * Toasts: short, non-blocking confirmations of actions the server has already confirmed
 * (signed out, scenario saved, report ready) and failures that do not belong to a form.
 * One polite live region announces them; errors use an assertive region. Each toast closes
 * itself (success 4.5 s, error 8 s) or with its close button; timers are cleared on unmount.
 */
import { CircleAlert, CircleCheck, Info, X } from "lucide-react";
import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useT } from "../i18n";

export type ToastTone = "good" | "bad" | "info";
interface ToastItem { id: number; tone: ToastTone; text: string; leaving: boolean }
interface ToastApi { show: (text: string, tone?: ToastTone) => void }

const ToastContext = createContext<ToastApi>({ show: () => undefined });
const LIFETIME: Record<ToastTone, number> = { good: 4500, info: 4500, bad: 8000 };
const EXIT_MS = 160;

export function ToastProvider({ children }: { children: ReactNode }) {
  const { t: tr } = useT();
  const closeLabel = tr("common.close");
  const [items, setItems] = useState<ToastItem[]>([]);
  const timers = useRef(new Map<number, number>());
  const seq = useRef(0);

  const remove = useCallback((id: number) => {
    setItems((list) => list.map((t) => (t.id === id ? { ...t, leaving: true } : t)));
    const exit = window.setTimeout(() => {
      setItems((list) => list.filter((t) => t.id !== id));
      timers.current.delete(-id);
    }, EXIT_MS);
    timers.current.set(-id, exit);
    const life = timers.current.get(id);
    if (life) { window.clearTimeout(life); timers.current.delete(id); }
  }, []);

  const show = useCallback((text: string, tone: ToastTone = "good") => {
    const id = ++seq.current;
    setItems((list) => [...list.slice(-2), { id, tone, text, leaving: false }]);
    timers.current.set(id, window.setTimeout(() => remove(id), LIFETIME[tone]));
  }, [remove]);

  useEffect(() => {
    const map = timers.current;
    return () => { map.forEach((h) => window.clearTimeout(h)); map.clear(); };
  }, []);

  const api = useMemo(() => ({ show }), [show]);
  const render = (list: ToastItem[]) => list.map((t) => {
    const Icon = t.tone === "good" ? CircleCheck : t.tone === "bad" ? CircleAlert : Info;
    return (
      <div key={t.id} className={`toast ${t.tone}${t.leaving ? " leaving" : ""}`}>
        <Icon size={18} aria-hidden="true" />
        <span className="grow">{t.text}</span>
        <button type="button" className="toast-close" onClick={() => remove(t.id)} aria-label={closeLabel}><X size={15} aria-hidden="true" /></button>
      </div>
    );
  });
  return (
    <ToastContext.Provider value={api}>
      {children}
      <div className="toast-stack">
        <div role="status" aria-live="polite" className="toast-region">{render(items.filter((t) => t.tone !== "bad"))}</div>
        <div role="alert" aria-live="assertive" className="toast-region">{render(items.filter((t) => t.tone === "bad"))}</div>
      </div>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastApi {
  return useContext(ToastContext);
}
