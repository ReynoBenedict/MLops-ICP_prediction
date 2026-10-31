/**
 * Product tour (landing #produk): WAI-ARIA tabs, one per workspace area. Each tab names the question
 * the area answers, two or three concrete capabilities and a real screenshot of the application
 * (captured from the local test organisation, never showing forecast values; see product-shots.ts).
 * The selection stays in component state: it changes a preview on a marketing page, not what the
 * page is about, so Back keeps its usual meaning.
 */
import { Check, ImageOff } from "lucide-react";
import { useState } from "react";
import { period } from "../../format";
import { useT, type Key } from "../../i18n";
import { tabPanelProps, Tabs } from "../ui";
import { PRODUCT_SHOTS } from "./product-shots";

const TABS = ["history", "insight", "scenario", "reports"] as const;
type Tab = (typeof TABS)[number];
/** On phones the screenshot is shown at a readable scale, cropped to the half that tells the story. */
const FOCUS: Record<Tab, "start" | "end"> = { history: "start", insight: "end", scenario: "start", reports: "start" };

export function ProductTour() {
  const { t, locale } = useT();
  const [tab, setTab] = useState<Tab>("history");
  const [failed, setFailed] = useState<Record<string, boolean>>({});
  const shot = PRODUCT_SHOTS.shots[tab];
  // Each language has its own captures; a failed image falls back to the "not available" frame.
  const src = locale === "en" ? shot.srcEn : shot.src;
  const k = (suffix: string) => `landing.tour.${tab}.${suffix}` as Key;
  return (
    <div className="tour">
      <Tabs idBase="tour" label={t("landing.tour.tabs")} value={tab} onChange={setTab}
        tabs={TABS.map((v) => ({ value: v, label: t(`landing.tour.${v}.tab` as Key) }))} />
      <div {...tabPanelProps("tour", tab)} className="tour-panel">
        <div className="tour-copy">
          <h3 className="tour-q">{t(k("q"))}</h3>
          <ul className="tour-caps">
            {(["c1", "c2", "c3"] as const).map((c) => <li key={c}><Check size={16} aria-hidden="true" />{t(k(c))}</li>)}
          </ul>
        </div>
        <figure className="tour-shot">
          <div className="tour-frame">
            <div className="tour-bar" aria-hidden="true"><span className="tour-bar-title">{t("brand.name")} <span className="tour-bar-sep">/</span> {t(k("tab"))}</span></div>
            {failed[src]
              ? <div className="tour-missing" style={{ aspectRatio: `${shot.width} / ${shot.height}` }}><ImageOff size={20} aria-hidden="true" />{t("landing.tour.missing")}</div>
              : (
                <div className={`tour-crop focus-${FOCUS[tab]}`}>
                  <img key={`${tab}-${locale}`} src={src} width={shot.width} height={shot.height} loading="lazy" decoding="async" alt={t(k("alt"))}
                    onError={() => setFailed((f) => ({ ...f, [src]: true }))} />
                </div>
              )}
          </div>
          <figcaption>{t("landing.tour.caption", { month: period(PRODUCT_SHOTS.dataThrough, locale, true) })}</figcaption>
        </figure>
      </div>
    </div>
  );
}
