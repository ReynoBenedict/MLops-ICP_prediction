import "@testing-library/jest-dom/vitest";

// The workspace and operator-console strings register when their lazy chunks load in the app. Tests
// render pages without that order, so every dictionary is registered up front; src/i18n/lazy-strings
// .test.ts checks statically that each module loads the strings it uses.
import "../i18n/workspace";
import "../i18n/ops";
// The signed-in shell is a lazy chunk in the app. App imported it statically before the split, so
// tests loaded (and transformed) it before their first render; keep that, so the first render of a
// workspace route does not also pay for transforming the shell inside a test's timeout.
import "../components/layout";

// jsdom does not implement scrolling; the app guards the calls, this keeps test output readable.
window.scrollTo = (() => undefined) as typeof window.scrollTo;
Element.prototype.scrollIntoView = function scrollIntoView() { /* no-op in jsdom */ };

// Multi-step flow tests (sign-in -> sign-out -> sign-in) wait on several async renders; the 1 s default
// made them timing-sensitive when the machine is busy (one intermittent failure observed under load).
import { configure } from "@testing-library/react";
configure({ asyncUtilTimeout: 3000 });

// Shared GETs (api/client sharedGet) are cached per module instance: start every test from a clean cache.
import { beforeEach } from "vitest";
import { clearSharedGets } from "../api/client";
beforeEach(() => clearSharedGets());
