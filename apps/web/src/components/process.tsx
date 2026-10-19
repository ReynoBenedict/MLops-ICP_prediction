/**
 * Three-step explainer of how an official ICP comes about (illustration + text). Kept in its own
 * module so public pages (Metodologi & Data) can use it without loading the chart bundle.
 */
import { useT } from "../i18n";
import { IllIcpProcess } from "./illustrations";

export function ProcessExplainer() {
  const { t } = useT();
  return (
    <div className="process">
      <div className="process-ill"><IllIcpProcess size={220} /></div>
      <ol className="process-steps">
        <li>{t("forecast.process1")}</li>
        <li>{t("forecast.process2")}</li>
        <li>{t("forecast.process3")}</li>
      </ol>
    </div>
  );
}
