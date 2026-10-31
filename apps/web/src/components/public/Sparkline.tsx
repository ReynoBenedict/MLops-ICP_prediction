/**
 * A small line of official ICP over the public history (sign-in and registration brand panel).
 * Plain SVG; decorative next to the stated values, so hidden from assistive technology, while the
 * caption names what it shows and its period. Missing months lift the pen (never interpolated);
 * the newest point is marked.
 */
import type { PublicMonth } from "./snapshot";
import { linePath } from "./svg-path";

const W = 240, H = 44, PAD = 4;

export function Sparkline({ months, caption }: { months: PublicMonth[]; caption: string }) {
  const values = months.map((m) => m.icp);
  const known = values.filter((v): v is number => v !== null);
  if (known.length < 2) return null;
  const lo = Math.min(...known), hi = Math.max(...known);
  const x = (i: number) => PAD + (i / Math.max(1, values.length - 1)) * (W - 2 * PAD);
  const y = (v: number) => PAD + (1 - (v - lo) / (hi - lo || 1)) * (H - 2 * PAD);
  const last = values.reduce<number>((acc, v, i) => (v !== null ? i : acc), -1);
  return (
    <figure className="ptrend">
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" height={H} preserveAspectRatio="none" aria-hidden="true" focusable="false">
        <path d={linePath(values, x, y)} className="ptrend-line" vectorEffect="non-scaling-stroke" />
      </svg>
      {last >= 0 && <span className="ptrend-dot" aria-hidden="true" style={{ left: `${(x(last) / W) * 100}%`, top: y(values[last]!) }} />}
      <figcaption>{caption}</figcaption>
    </figure>
  );
}
