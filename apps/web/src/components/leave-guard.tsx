/**
 * Unsaved-changes guard for forms (scenario editor). While `dirty`:
 * - with a data router (production, `createBrowserRouter`), every in-app navigation including the
 *   browser Back button is blocked and confirmed with ConfirmDialog (react-router `useBlocker`);
 * - with a plain router (tests), clicks on in-app links are intercepted and confirmed instead;
 * - closing or reloading the tab triggers the browser's own "leave site?" prompt.
 */
import { useContext, useEffect, useState } from "react";
import { UNSAFE_DataRouterContext, useBlocker, useNavigate } from "react-router-dom";
import { useT } from "../i18n";
import { ConfirmDialog } from "./ui";

function useBeforeUnload(dirty: boolean) {
  useEffect(() => {
    if (!dirty) return;
    const on = (e: BeforeUnloadEvent) => { e.preventDefault(); e.returnValue = ""; };
    window.addEventListener("beforeunload", on);
    return () => window.removeEventListener("beforeunload", on);
  }, [dirty]);
}

function Dialog({ open, onStay, onLeave }: { open: boolean; onStay: () => void; onLeave: () => void }) {
  const { t } = useT();
  return (
    <ConfirmDialog open={open} title={t("common.unsavedTitle")} confirmLabel={t("common.unsavedLeave")} onCancel={onStay} onConfirm={onLeave}>
      {t("common.unsavedBody")}
    </ConfirmDialog>
  );
}

function DataRouterGuard({ dirty }: { dirty: boolean }) {
  const blocker = useBlocker(({ currentLocation, nextLocation }) => dirty && currentLocation.pathname + currentLocation.search !== nextLocation.pathname + nextLocation.search);
  return <Dialog open={blocker.state === "blocked"} onStay={() => blocker.reset?.()} onLeave={() => blocker.proceed?.()} />;
}

function LinkGuard({ dirty }: { dirty: boolean }) {
  const navigate = useNavigate();
  const [pending, setPending] = useState<string | null>(null);
  useEffect(() => {
    if (!dirty) return;
    const on = (e: MouseEvent) => {
      if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      const a = (e.target as Element | null)?.closest?.("a[href]") as HTMLAnchorElement | null;
      if (!a || a.target === "_blank" || a.hasAttribute("download")) return;
      const url = new URL(a.href, window.location.href);
      if (url.origin !== window.location.origin) return;
      e.preventDefault();
      e.stopPropagation();
      setPending(url.pathname + url.search + url.hash);
    };
    document.addEventListener("click", on, true);
    return () => document.removeEventListener("click", on, true);
  }, [dirty]);
  return <Dialog open={pending !== null} onStay={() => setPending(null)} onLeave={() => { const to = pending; setPending(null); if (to) navigate(to); }} />;
}

export function LeaveGuard({ dirty }: { dirty: boolean }) {
  useBeforeUnload(dirty);
  const dataRouter = useContext(UNSAFE_DataRouterContext);
  return dataRouter ? <DataRouterGuard dirty={dirty} /> : <LinkGuard dirty={dirty} />;
}
