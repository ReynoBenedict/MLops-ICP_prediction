/**
 * SVG path through the non-null values of a monthly series (public charts: the landing chart and the
 * sign-in sparkline). A null value lifts the pen: a missing month is a break, never interpolated.
 */
export function linePath(values: Array<number | null>, x: (i: number) => number, y: (v: number) => number): string {
  let d = "";
  let pen = false;
  values.forEach((v, i) => {
    if (v === null) { pen = false; return; }
    d += `${pen ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`;
    pen = true;
  });
  return d;
}
