/** Logo symbol and wordmark as inline SVG (same geometry as public/brand/logo-symbol.svg). */
export function LogoSymbol({ size = 32, tone = "navy" }: { size?: number; tone?: "navy" | "teal" | "white" }) {
  const bg = tone === "navy" ? "#123A5C" : tone === "teal" ? "#008C8C" : "#FFFFFF";
  const bars = tone === "white" ? "#123A5C" : "#FFFFFF";
  const stroke = tone === "white" ? "#D99A2B" : "#F2B64E";
  return (
    <svg width={size} height={size} viewBox="0 0 64 64" aria-hidden="true" focusable="false">
      <rect width="64" height="64" rx="14" fill={bg} />
      <rect x="13" y="38" width="8" height="13" rx="2.5" fill={bars} opacity="0.92" />
      <rect x="27" y="29" width="8" height="22" rx="2.5" fill={bars} opacity="0.92" />
      <rect x="41" y="20" width="8" height="31" rx="2.5" fill={bars} opacity="0.92" />
      <path d="M11 33 L26 23 L36 27 L52 12" fill="none" stroke={stroke} strokeWidth="3.5" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M44 11 L53 11 L53 20" fill="none" stroke={stroke} strokeWidth="3.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

export function Wordmark({ dark = false, size = 32, tagline }: { dark?: boolean; size?: number; tagline?: string }) {
  return (
    <span className="wordmark" style={{ display: "inline-flex", alignItems: "center", gap: 10 }}>
      <LogoSymbol size={size} tone={dark ? "teal" : "navy"} />
      <span style={{ display: "flex", flexDirection: "column", lineHeight: 1.1 }}>
        <span style={{ fontWeight: 700, fontSize: size * 0.5, letterSpacing: "-0.01em", color: dark ? "#fff" : "#123A5C" }}>ICP Intelligence</span>
        {tagline && <span style={{ fontSize: Math.max(11, size * 0.34), color: dark ? "#A9B6C8" : "#5D6779", fontWeight: 500 }}>{tagline}</span>}
      </span>
    </span>
  );
}
