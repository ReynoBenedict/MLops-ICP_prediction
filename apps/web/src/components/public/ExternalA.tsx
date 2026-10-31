/** A link that leaves the site: new tab, no opener or referrer, an icon, and a spoken "opens in a new tab". */
import { ExternalLink } from "lucide-react";
import type { ReactNode } from "react";
import { useT } from "../../i18n";

export function ExternalA({ href, className, children }: { href: string; className?: string; children: ReactNode }) {
  const { t } = useT();
  return (
    <a href={href} target="_blank" rel="noopener noreferrer" className={className}>
      {children}<ExternalLink size={13} aria-hidden="true" className="ext-icon" /><span className="sr-only"> {t("landing.panel.newTab")}</span>
    </a>
  );
}
