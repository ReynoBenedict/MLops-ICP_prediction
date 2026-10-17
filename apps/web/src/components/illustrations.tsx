/**
 * Original inline SVG illustrations (empty states, onboarding, explanatory). Same palette as the
 * brand: navy #123A5C, teal #008C8C, amber #D99A2B. Purely decorative; always paired with text.
 */
import type { SVGProps } from "react";

type P = SVGProps<SVGSVGElement> & { size?: number };
const base = (size: number | undefined, p: P) => ({ width: size ?? 160, height: (size ?? 160) * 0.75, viewBox: "0 0 160 120", fill: "none", "aria-hidden": true, ...p });

/** Empty chart with a dashed curve: "no data yet". */
export function IllNoData({ size, ...p }: P) {
  return (
    <svg {...base(size, p)}>
      <rect x="14" y="18" width="132" height="84" rx="12" fill="#F3F6FA" stroke="#D6DEE8" />
      <path d="M30 86 H130" stroke="#C5D0DD" strokeWidth="2" strokeLinecap="round" />
      <path d="M30 78 C 50 70, 60 82, 76 66 S 104 48, 130 40" stroke="#123A5C" strokeWidth="2.5" strokeDasharray="5 5" strokeLinecap="round" />
      <circle cx="130" cy="40" r="5" fill="#FFFFFF" stroke="#D99A2B" strokeWidth="2.5" />
      <rect x="40" y="28" width="36" height="8" rx="4" fill="#DCE4EE" />
    </svg>
  );
}

/** Calendar with a question mark: "no forecast published yet". */
export function IllNoForecast({ size, ...p }: P) {
  return (
    <svg {...base(size, p)}>
      <rect x="28" y="22" width="104" height="80" rx="12" fill="#F3F6FA" stroke="#D6DEE8" />
      <rect x="28" y="22" width="104" height="22" rx="12" fill="#123A5C" />
      <rect x="28" y="34" width="104" height="10" fill="#123A5C" />
      <circle cx="52" cy="33" r="3" fill="#F2B64E" /><circle cx="64" cy="33" r="3" fill="#FFFFFF" opacity=".6" />
      {[0, 1, 2, 3].map((i) => <rect key={i} x={44 + i * 22} y="56" width="14" height="10" rx="3" fill="#DCE4EE" />)}
      {[0, 1].map((i) => <rect key={i} x={44 + i * 22} y="74" width="14" height="10" rx="3" fill="#DCE4EE" />)}
      <circle cx="110" cy="78" r="14" fill="#FFFFFF" stroke="#008C8C" strokeWidth="2" />
      <path d="M105 74 a5 5 0 1 1 7 4.6 c-1.6.7-2 1.6-2 3" stroke="#008C8C" strokeWidth="2" strokeLinecap="round" />
      <circle cx="110" cy="86" r="1.4" fill="#008C8C" />
    </svg>
  );
}

/** Two diverging paths from one point: "no scenarios yet". */
export function IllScenarios({ size, ...p }: P) {
  return (
    <svg {...base(size, p)}>
      <rect x="14" y="18" width="132" height="84" rx="12" fill="#F3F6FA" stroke="#D6DEE8" />
      <path d="M30 70 H78" stroke="#123A5C" strokeWidth="3" strokeLinecap="round" />
      <path d="M78 70 C 96 70, 104 44, 130 36" stroke="#D99A2B" strokeWidth="3" strokeLinecap="round" />
      <path d="M78 70 C 96 70, 104 92, 130 96" stroke="#008C8C" strokeWidth="3" strokeLinecap="round" strokeDasharray="6 5" />
      <circle cx="78" cy="70" r="5" fill="#123A5C" />
      <rect x="124" y="30" width="12" height="12" rx="2" fill="#D99A2B" />
      <path d="M124 102 l6 -11 l6 11z" fill="#008C8C" />
    </svg>
  );
}

/** Bell with a quiet dot: "no alerts". */
export function IllAlerts({ size, ...p }: P) {
  return (
    <svg {...base(size, p)}>
      <circle cx="80" cy="60" r="44" fill="#F3F6FA" stroke="#D6DEE8" />
      <path d="M62 72 V 58 a18 18 0 0 1 36 0 v 14 l 6 8 H 56 z" fill="#FFFFFF" stroke="#123A5C" strokeWidth="2.5" strokeLinejoin="round" />
      <path d="M74 86 a6 6 0 0 0 12 0" stroke="#123A5C" strokeWidth="2.5" />
      <circle cx="98" cy="42" r="6" fill="#D99A2B" />
    </svg>
  );
}

/** Lock with an upward arrow: "upgrade / sign-in needed". */
export function IllUpgrade({ size, ...p }: P) {
  return (
    <svg {...base(size, p)}>
      <rect x="14" y="18" width="132" height="84" rx="12" fill="#F3F6FA" stroke="#D6DEE8" />
      <rect x="54" y="56" width="52" height="36" rx="8" fill="#FFFFFF" stroke="#123A5C" strokeWidth="2.5" />
      <path d="M64 56 V 48 a16 16 0 0 1 32 0 v 8" stroke="#123A5C" strokeWidth="2.5" fill="none" />
      <circle cx="80" cy="74" r="4" fill="#008C8C" />
      <path d="M118 44 V 28 M 112 34 l 6 -6 l 6 6" stroke="#D99A2B" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/** Document with a checkmark: "no reports yet". */
export function IllReports({ size, ...p }: P) {
  return (
    <svg {...base(size, p)}>
      <path d="M52 20 h 40 l 20 20 v 60 a6 6 0 0 1 -6 6 H 52 a6 6 0 0 1 -6 -6 V 26 a6 6 0 0 1 6 -6z" fill="#FFFFFF" stroke="#D6DEE8" />
      <path d="M92 20 v 20 h 20" fill="#F3F6FA" stroke="#D6DEE8" />
      <rect x="56" y="52" width="40" height="5" rx="2.5" fill="#DCE4EE" /><rect x="56" y="64" width="48" height="5" rx="2.5" fill="#DCE4EE" />
      <rect x="56" y="76" width="30" height="5" rx="2.5" fill="#DCE4EE" />
      <rect x="56" y="88" width="20" height="8" rx="2" fill="#123A5C" /><rect x="80" y="88" width="12" height="8" rx="2" fill="#D99A2B" />
      <circle cx="112" cy="92" r="12" fill="#008C8C" /><path d="M106 92 l 4 4 l 8 -8" stroke="#FFFFFF" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/** Three-step pipeline: sources -> verification -> forecast (onboarding / methodology). */
export function IllPipeline({ size, ...p }: P) {
  return (
    <svg {...base(size, p)}>
      <rect x="10" y="40" width="38" height="40" rx="8" fill="#FFFFFF" stroke="#123A5C" strokeWidth="2" />
      <rect x="61" y="40" width="38" height="40" rx="8" fill="#FFFFFF" stroke="#008C8C" strokeWidth="2" />
      <rect x="112" y="40" width="38" height="40" rx="8" fill="#FFFFFF" stroke="#D99A2B" strokeWidth="2" />
      <path d="M48 60 H 61 M 99 60 H 112" stroke="#8A93A3" strokeWidth="2" strokeDasharray="3 3" />
      <rect x="18" y="52" width="22" height="4" rx="2" fill="#123A5C" /><rect x="18" y="60" width="16" height="4" rx="2" fill="#123A5C" opacity=".6" /><rect x="18" y="68" width="19" height="4" rx="2" fill="#123A5C" opacity=".4" />
      <path d="M72 60 l 6 6 l 12 -12" stroke="#008C8C" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M118 72 C 124 66, 128 70, 134 58 S 142 50, 146 48" stroke="#D99A2B" strokeWidth="2.5" strokeLinecap="round" />
    </svg>
  );
}

/** Explanatory: world benchmark prices -> ministerial decree -> official ICP on the platform. */
export function IllIcpProcess({ size, ...p }: P) {
  return (
    <svg {...base(size, p)}>
      {/* world prices: three small bars */}
      <rect x="10" y="30" width="40" height="60" rx="8" fill="#F3F6FA" stroke="#D6DEE8" />
      <rect x="18" y="62" width="6" height="20" rx="2" fill="#123A5C" /><rect x="27" y="52" width="6" height="30" rx="2" fill="#008C8C" /><rect x="36" y="44" width="6" height="38" rx="2" fill="#D99A2B" />
      <path d="M16 40 h28" stroke="#8A93A3" strokeWidth="2" strokeLinecap="round" />
      {/* decree: document with a seal */}
      <path d="M62 24 h 26 l 10 10 v 56 a4 4 0 0 1 -4 4 H 62 a4 4 0 0 1 -4 -4 V 28 a4 4 0 0 1 4 -4z" fill="#FFFFFF" stroke="#123A5C" strokeWidth="2" />
      <path d="M88 24 v 10 h 10" fill="#F3F6FA" stroke="#123A5C" strokeWidth="2" />
      <rect x="66" y="46" width="22" height="4" rx="2" fill="#DCE4EE" /><rect x="66" y="56" width="26" height="4" rx="2" fill="#DCE4EE" /><rect x="66" y="66" width="18" height="4" rx="2" fill="#DCE4EE" />
      <circle cx="86" cy="80" r="7" fill="#D99A2B" /><path d="M83 80 l 2 2 l 4 -4" stroke="#FFFFFF" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      {/* platform: screen with the official value */}
      <rect x="110" y="34" width="42" height="48" rx="8" fill="#FFFFFF" stroke="#008C8C" strokeWidth="2" />
      <rect x="116" y="42" width="30" height="6" rx="3" fill="#123A5C" />
      <path d="M116 70 C 122 66, 126 68, 131 60 S 141 54, 146 52" stroke="#008C8C" strokeWidth="2.5" strokeLinecap="round" />
      <circle cx="146" cy="52" r="3" fill="#FFFFFF" stroke="#D99A2B" strokeWidth="2" />
      <path d="M124 90 h 14" stroke="#008C8C" strokeWidth="2" strokeLinecap="round" />
      {/* arrows */}
      <path d="M50 60 H 58 M 98 60 H 110" stroke="#8A93A3" strokeWidth="2" strokeDasharray="3 3" />
      <path d="M55 57 l 3 3 l -3 3 M 107 57 l 3 3 l -3 3" stroke="#8A93A3" strokeWidth="2" strokeLinecap="round" fill="none" />
    </svg>
  );
}

/** Small source-type marks (16 px) used next to dataset names: decree, market price, exchange rate. */
export function MarkDecree({ size = 16, ...p }: P) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" aria-hidden="true" {...p}>
      <path d="M4 2 h 6 l 3 3 v 9 H 4z" fill="#FFFFFF" stroke="#123A5C" strokeWidth="1.4" strokeLinejoin="round" />
      <path d="M10 2 v 3 h 3" stroke="#123A5C" strokeWidth="1.4" strokeLinejoin="round" />
      <circle cx="9.5" cy="10.5" r="2.2" fill="#D99A2B" />
    </svg>
  );
}
export function MarkBarrel({ size = 16, ...p }: P) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" aria-hidden="true" {...p}>
      <rect x="3.5" y="2" width="9" height="12" rx="2.5" fill="#FFFFFF" stroke="#123A5C" strokeWidth="1.4" />
      <path d="M3.5 6 h 9 M 3.5 10 h 9" stroke="#008C8C" strokeWidth="1.4" />
    </svg>
  );
}
export function MarkFx({ size = 16, ...p }: P) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none" aria-hidden="true" {...p}>
      <path d="M2 6 h 9 M 8 3 l 3 3 l -3 3" stroke="#123A5C" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M14 10 h -9 M 8 7 l -3 3 l 3 3" stroke="#D99A2B" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/** Explanatory: an elapsed month (observed benchmarks) versus a future month (assumed). */
export function IllTimeline({ size, ...p }: P) {
  return (
    <svg {...base(size, p)}>
      <path d="M12 70 H 148" stroke="#C5D0DD" strokeWidth="2" />
      <path d="M12 70 H 92" stroke="#123A5C" strokeWidth="3" />
      {[20, 44, 68].map((x) => <circle key={x} cx={x} cy="70" r="5" fill="#123A5C" />)}
      <circle cx="92" cy="70" r="6" fill="#FFFFFF" stroke="#008C8C" strokeWidth="3" />
      {[116, 140].map((x) => <circle key={x} cx={x} cy="70" r="5" fill="#FFFFFF" stroke="#D99A2B" strokeWidth="2.5" strokeDasharray="2 2" />)}
      <rect x="4" y="30" width="56" height="16" rx="8" fill="#DCE4EE" /><rect x="76" y="30" width="32" height="16" rx="8" fill="#CDEAEA" /><rect x="112" y="30" width="44" height="16" rx="8" fill="#F8E7C4" />
      <path d="M92 46 v 16 M 128 46 v 16" stroke="#8A93A3" strokeDasharray="2 2" />
    </svg>
  );
}
