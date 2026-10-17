import "@testing-library/jest-dom/vitest";

// jsdom does not implement scrolling; the app guards the calls, this keeps test output readable.
window.scrollTo = (() => undefined) as typeof window.scrollTo;
Element.prototype.scrollIntoView = function scrollIntoView() { /* no-op in jsdom */ };

// Multi-step flow tests (sign-in -> sign-out -> sign-in) wait on several async renders; the 1 s default
// made them timing-sensitive when the machine is busy (one intermittent failure observed under load).
import { configure } from "@testing-library/react";
configure({ asyncUtilTimeout: 3000 });
