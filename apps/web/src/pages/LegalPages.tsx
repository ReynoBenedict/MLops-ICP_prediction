/**
 * Terms of use and privacy policy. Plain Indonesian descriptions of what the service actually does
 * with data; no invented legal entity ("Operator layanan ICP Intelligence"). Reading width is
 * capped at about 70 characters, with a visible "last updated" line and legal links in the footer.
 */
import { ArrowLeft } from "lucide-react";
import { Link, NavLink } from "react-router-dom";
import { Wordmark } from "../components/brand";
import { LanguageToggle } from "../components/layout";
import { useT } from "../i18n";

function Frame({ title, intro, children }: { title: string; intro: string; children: React.ReactNode }) {
  const { t } = useT();
  return (
    <div className="landing legal-page">
      <nav className="landing-nav" aria-label={t("landing.footer.terms")}>
        <Link to="/" style={{ textDecoration: "none" }}><Wordmark size={30} /></Link>
        <div className="links">
          <NavLink to="/syarat" className={({ isActive }) => (isActive ? "active" : undefined)}>{t("landing.footer.terms")}</NavLink>
          <NavLink to="/privasi" className={({ isActive }) => (isActive ? "active" : undefined)}>{t("landing.footer.privacy")}</NavLink>
        </div>
        <div className="row"><LanguageToggle /><Link className="btn sm" to="/"><ArrowLeft size={14} aria-hidden="true" />{t("shell.landing")}</Link></div>
      </nav>
      <article className="prose">
        <header className="legal-head">
          <h1>{title}</h1>
          <p className="lead">{intro}</p>
          <p className="legal-meta"><span>{t("legal.updatedLabel")}</span><time dateTime="2026-10-16">{t("legal.updatedDate")}</time></p>
        </header>
        {children}
        <p className="legal-contact">{t("legal.contact")}</p>
      </article>
      <footer className="legal-foot">
        <div className="inner">
          <span>{t("landing.footer.operator")} {t("landing.footer.noAdvice")}</span>
          <nav className="row" aria-label={t("landing.footer.terms")}>
            <Link to="/">{t("shell.landing")}</Link>
            <Link to="/syarat">{t("landing.footer.terms")}</Link>
            <Link to="/privasi">{t("landing.footer.privacy")}</Link>
            <Link to="/masuk">{t("shell.signin")}</Link>
          </nav>
        </div>
      </footer>
    </div>
  );
}

export function TermsPage() {
  const { t, locale } = useT();
  return (
    <Frame title={t("legal.termsTitle")} intro={t("legal.intro.terms")}>
      {locale === "id" ? (
        <>
          <h2>1. Layanan</h2>
          <p>ICP Intelligence adalah platform informasi dan analitik yang menampilkan Harga Minyak Mentah Indonesia (ICP) resmi yang diterbitkan Kementerian ESDM, harga acuan minyak dunia, kurs referensi Bank Indonesia, serta proyeksi, skenario, insight dan laporan yang dihitung dari data tersebut. Layanan dioperasikan oleh Operator layanan ICP Intelligence dan tidak berafiliasi dengan Kementerian ESDM, Bank Indonesia, EIA, maupun Bank Dunia.</p>
          <h2>2. Bukan nasihat</h2>
          <p>Proyeksi dan estimasi adalah keluaran metode statistik yang dikalibrasi pada data historis. Keluaran tersebut bukan nasihat investasi, perdagangan, atau keputusan anggaran, dan bukan pengganti penetapan resmi. Pengguna bertanggung jawab atas keputusan yang diambil berdasarkan informasi di platform.</p>
          <h2>3. Akun dan organisasi</h2>
          <p>Pendaftaran membuat organisasi pada paket Uji coba dengan pendaftar sebagai pemilik. Pemilik dan admin organisasi dapat mengundang anggota; setiap anggota bertanggung jawab menjaga kerahasiaan kata sandi dan kunci API. Operator layanan dapat menangguhkan organisasi yang menyalahgunakan layanan (misalnya pengambilan data otomatis di luar batas paket).</p>
          <h2>4. Paket dan pembayaran</h2>
          <p>Hak akses setiap paket (horizon proyeksi, skenario tersimpan, laporan, pemberitahuan, kursi, akses API) ditegakkan oleh server. Perubahan paket dilakukan oleh operator layanan setelah kesepakatan tertulis; belum ada pembayaran mandiri di dalam aplikasi dan tidak ada data pembayaran yang dikumpulkan.</p>
          <h2>5. Data dan lisensi sumber</h2>
          <p>Nilai ICP bersumber dari publikasi Kementerian ESDM; harga WTI dan Brent dari U.S. EIA (domain publik); harga Dubai dari World Bank Pink Sheet (CC BY 4.0); kurs dari Bank Indonesia (JISDOR). Pengguna dapat mengunduh dan menggunakan data agregat bulanan untuk keperluan internal organisasinya dengan menyebutkan sumber; penjualan kembali data mentah tidak diizinkan.</p>
          <h2>6. Ketersediaan</h2>
          <p>Layanan disediakan apa adanya. Pembaruan data bergantung pada ketersediaan sumber eksternal; status kesegaran data selalu ditampilkan. Operator berupaya menjaga ketersediaan namun tidak menjamin layanan bebas gangguan.</p>
          <h2>7. Perubahan</h2>
          <p>Syarat ini dapat diperbarui; tanggal pembaruan tercantum di atas. Penggunaan berkelanjutan setelah perubahan berarti persetujuan atas syarat yang diperbarui.</p>
        </>
      ) : (
        <>
          <h2>1. The service</h2>
          <p>ICP Intelligence is an information and analytics platform that shows the official Indonesian Crude Price (ICP) published by the Ministry of Energy and Mineral Resources, world crude benchmarks, Bank Indonesia's reference exchange rate, and the projections, scenarios, insights and reports computed from that data. It is operated by the ICP Intelligence service operator and is not affiliated with the Ministry, Bank Indonesia, the EIA or the World Bank.</p>
          <h2>2. Not advice</h2>
          <p>Projections and estimates are outputs of statistical methods calibrated on historical data. They are not investment, trading or budgeting advice and not a substitute for the official determination. Users are responsible for decisions taken on the basis of information in the platform.</p>
          <h2>3. Accounts and organisations</h2>
          <p>Registration creates an organisation on the Trial plan with the registrant as owner. Owners and admins may invite members; each member must keep passwords and API keys confidential. The operator may suspend organisations that abuse the service (for example automated extraction beyond plan limits).</p>
          <h2>4. Plans and payment</h2>
          <p>Each plan's entitlements (forecast horizon, saved scenarios, reports, alerts, seats, API access) are enforced server-side. Plan changes are made by the operator after written agreement; there is no self-service payment in the app and no payment data is collected.</p>
          <h2>5. Data and source licences</h2>
          <p>ICP values come from Ministry publications; WTI and Brent from the U.S. EIA (public domain); Dubai from the World Bank Pink Sheet (CC BY 4.0); exchange rates from Bank Indonesia (JISDOR). Users may download and use monthly aggregate data internally with attribution; resale of raw data is not permitted.</p>
          <h2>6. Availability</h2>
          <p>The service is provided as is. Data refreshes depend on external sources; data freshness is always displayed. The operator aims to keep the service available but does not guarantee uninterrupted operation.</p>
          <h2>7. Changes</h2>
          <p>These terms may be updated; the date above shows the latest revision. Continued use after a change constitutes acceptance.</p>
        </>
      )}
    </Frame>
  );
}

export function PrivacyPage() {
  const { t, locale } = useT();
  return (
    <Frame title={t("legal.privacyTitle")} intro={t("legal.intro.privacy")}>
      {locale === "id" ? (
        <>
          <h2>Data yang disimpan</h2>
          <ul>
            <li><strong>Akun:</strong> alamat email, nama, peran, organisasi, waktu pendaftaran dan masuk terakhir.</li>
            <li><strong>Kata sandi:</strong> disimpan hanya sebagai hash scrypt dengan salt acak; tidak pernah dicatat atau dikirim kembali.</li>
            <li><strong>Sesi dan kunci API:</strong> token acak; hanya hash SHA-256-nya yang disimpan. Sesi berlaku 30 hari dan dapat dicabut dari halaman Akun. Agen pengguna (browser) sesi disimpan untuk membantu mengenali sesi.</li>
            <li><strong>Aktivitas:</strong> log audit tindakan sensitif (masuk, undangan anggota, perubahan paket, pembuatan kunci API, pembuatan laporan) dengan pelaku dan waktu, tanpa alamat IP dan tanpa token. Penghitungan penggunaan (jumlah proyeksi, laporan, permintaan API) per organisasi untuk penegakan paket.</li>
            <li><strong>Konten Anda:</strong> skenario tersimpan, aturan pemberitahuan, laporan yang dibuat dan proyeksi yang Anda jalankan, terikat pada organisasi Anda dan tidak terlihat oleh organisasi lain.</li>
          </ul>
          <h2>Data yang tidak dikumpulkan</h2>
          <p>Tidak ada data pembayaran, tidak ada pelacak iklan, tidak ada analitik pihak ketiga di aplikasi. Preferensi bahasa disimpan di browser Anda sendiri.</p>
          <h2>Penggunaan</h2>
          <p>Data digunakan hanya untuk menyediakan layanan, menegakkan hak akses paket, keamanan (deteksi penyalahgunaan), dan dukungan. Operator platform dapat melihat daftar organisasi, paket dan statusnya untuk administrasi.</p>
          <h2>Retensi</h2>
          <p>Data akun disimpan selama organisasi aktif. Log audit disimpan untuk keperluan keamanan. Permintaan penghapusan akun dapat diajukan kepada operator layanan dan akan diproses dengan menonaktifkan akun serta menganonimkan log yang terkait.</p>
          <h2>Pihak ketiga</h2>
          <p>Platform mengambil data dari Kementerian ESDM, U.S. EIA, World Bank dan Bank Indonesia; tidak ada data pengguna yang dikirim ke sumber-sumber tersebut.</p>
        </>
      ) : (
        <>
          <h2>Data we store</h2>
          <ul>
            <li><strong>Account:</strong> email address, name, role, organisation, registration and last sign-in times.</li>
            <li><strong>Passwords:</strong> stored only as salted scrypt hashes; never logged or returned.</li>
            <li><strong>Sessions and API keys:</strong> random tokens; only their SHA-256 hash is stored. Sessions last 30 days and can be revoked from the Account page. The session's browser user agent is stored to help recognise it.</li>
            <li><strong>Activity:</strong> an audit log of sensitive actions (sign-in, invitations, plan changes, API key creation, report generation) with actor and time, without IP addresses and without tokens. Usage counts (forecasts, reports, API requests) per organisation for plan enforcement.</li>
            <li><strong>Your content:</strong> saved scenarios, alert rules, generated reports and forecasts you ran, scoped to your organisation and invisible to others.</li>
          </ul>
          <h2>Data we do not collect</h2>
          <p>No payment data, no advertising trackers, no third-party analytics in the app. Your language preference is kept in your own browser.</p>
          <h2>Use</h2>
          <p>Data is used only to provide the service, enforce plan entitlements, security (abuse detection) and support. Platform operators can see the list of organisations, plans and statuses for administration.</p>
          <h2>Retention</h2>
          <p>Account data is kept while the organisation is active. Audit logs are kept for security. Account deletion requests can be sent to the operator and are handled by disabling the account and anonymising related logs.</p>
          <h2>Third parties</h2>
          <p>The platform fetches data from the Ministry of Energy, the U.S. EIA, the World Bank and Bank Indonesia; no user data is sent to those sources.</p>
        </>
      )}
    </Frame>
  );
}
