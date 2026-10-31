/**
 * Terms of Use and Privacy Policy text (draft of 19 October 2026, pending owner and legal review).
 *
 * Every statement about data handling was checked against the code: accounts and sessions
 * (backend/icp_api/services/accounts.py, infra/cloudflare/d1/migrations/0004_accounts_and_product.sql),
 * rate limiting (backend/icp_api/security.py, apps/gateway/src/index.ts), request logs
 * (backend/icp_api/middleware.py), audit log (0001_initial.sql), browser storage (src/api/client.ts,
 * src/i18n/index.tsx). Business and legal facts that the code cannot establish (legal entity,
 * address, contact, governing law, retention periods, legal basis) are NOT invented: they appear as
 * visible placeholders `[[key]]`, listed in PLACEHOLDERS and in ux5/legal-owner-inputs.md.
 *
 * Markup inside texts: `[[key]]` = owner placeholder, `**bold**`. No other formatting.
 */

export interface L { id: string; en: string }
export type Block = { p: L } | { ul: L[] };
export interface Section { id: string; title: L; blocks: Block[] }
export interface LegalDoc { intro: L; draftNote: L; sections: Section[] }

/** Decisions only the owner (with legal counsel) can make. Rendered as marked placeholders. */
export const PLACEHOLDERS: Record<string, L> = {
  entity: { id: "nama badan hukum penyelenggara", en: "legal name of the operating entity" },
  address: { id: "alamat terdaftar penyelenggara", en: "registered address of the operator" },
  contact: { id: "saluran kontak resmi (email atau formulir) untuk pertanyaan hukum dan privasi", en: "official contact channel (email or form) for legal and privacy questions" },
  law: { id: "hukum yang berlaku dan forum penyelesaian sengketa", en: "governing law and dispute forum" },
  liability: { id: "batas tanggung jawab yang disepakati", en: "agreed limitation of liability" },
  notice: { id: "jangka waktu pemberitahuan sebelum perubahan ketentuan berlaku", en: "notice period before changed terms take effect" },
  basis: { id: "dasar hukum pemrosesan data pribadi untuk setiap tujuan", en: "legal basis for processing personal data for each purpose" },
  retentionAccount: { id: "masa simpan data akun setelah organisasi berhenti", en: "retention period for account data after an organisation leaves" },
  retentionAudit: { id: "masa simpan log audit", en: "retention period for the audit log" },
  retentionContent: { id: "masa simpan laporan, proyeksi dan konten organisasi", en: "retention period for reports, forecasts and organisation content" },
  hostingBackend: { id: "lokasi dan penyedia hosting server analitik produksi", en: "location and provider of the production analytics server" },
  transfer: { id: "pernyataan transfer data lintas negara dan dasarnya", en: "cross-border transfer statement and its basis" },
  age: { id: "batas usia minimum pengguna", en: "minimum user age" },
  response: { id: "jangka waktu tanggapan atas permintaan hak subjek data", en: "response time for data subject requests" },
};

const p = (id: string, en: string): Block => ({ p: { id, en } });
const ul = (...items: Array<[string, string]>): Block => ({ ul: items.map(([id, en]) => ({ id, en })) });

export const TERMS: LegalDoc = {
  intro: {
    id: "Ketentuan ini mengatur penggunaan ICP Intelligence oleh organisasi dan anggotanya. Baca bersama Kebijakan Privasi.",
    en: "These terms govern the use of ICP Intelligence by organisations and their members. Read them together with the Privacy Policy.",
  },
  draftNote: {
    id: "Dokumen ini adalah versi draf yang belum ditinjau penasihat hukum. Bagian bertanda [Diisi pemilik] belum ditetapkan dan tidak berlaku sampai diisi.",
    en: "This document is a draft that has not been reviewed by legal counsel. Parts marked [Owner to complete] are not yet decided and have no effect until completed.",
  },
  sections: [
    { id: "layanan", title: { id: "Tentang layanan", en: "About the service" }, blocks: [
      p("ICP Intelligence adalah platform informasi dan analitik yang menampilkan Harga Minyak Mentah Indonesia (ICP) resmi yang ditetapkan Kementerian Energi dan Sumber Daya Mineral (ESDM), harga acuan minyak dunia (Brent, Dubai, WTI), kurs referensi JISDOR Bank Indonesia, serta proyeksi, skenario, insight otomatis, laporan dan pemberitahuan yang dihitung dari data tersebut.",
        "ICP Intelligence is an information and analytics platform that shows the official Indonesian Crude Price (ICP) set by the Ministry of Energy and Mineral Resources (ESDM), world crude benchmarks (Brent, Dubai, WTI), Bank Indonesia's JISDOR reference rate, and the forecasts, scenarios, automatic insights, reports and notifications computed from that data."),
      p("Layanan diselenggarakan oleh [[entity]], [[address]] (\"Penyelenggara\"). ICP Intelligence tidak berafiliasi dengan Kementerian ESDM, Bank Indonesia, U.S. Energy Information Administration (EIA) maupun Bank Dunia, dan tidak mewakili lembaga tersebut.",
        "The service is operated by [[entity]], [[address]] (the \"Operator\"). ICP Intelligence is not affiliated with and does not represent the Ministry of Energy and Mineral Resources, Bank Indonesia, the U.S. Energy Information Administration (EIA) or the World Bank."),
    ] },
    { id: "akun", title: { id: "Akun, organisasi dan peran", en: "Accounts, organisations and roles" }, blocks: [
      p("Pendaftaran membuat sebuah organisasi pada paket Uji coba, dengan pendaftar sebagai Pemilik. Layanan ditujukan bagi organisasi dan orang yang bertindak atas nama organisasi; pengguna harus berusia minimal [[age]].",
        "Registration creates an organisation on the Trial plan with the registrant as Owner. The service is intended for organisations and people acting on their behalf; users must be at least [[age]]."),
      ul(
        ["**Pemilik dan Admin** mengundang anggota, menetapkan peran, menonaktifkan anggota dan mengelola kunci API.", "**Owners and Admins** invite members, assign roles, disable members and manage API keys."],
        ["**Analis** menjalankan proyeksi, membuat skenario, laporan dan aturan pemberitahuan.", "**Analysts** run forecasts and create scenarios, reports and alert rules."],
        ["**Pembaca** hanya dapat melihat data dan hasil.", "**Viewers** can only view data and results."],
      ),
      p("Setiap pengguna wajib menjaga kerahasiaan kata sandi, token sesi dan kunci API, serta segera memberi tahu admin organisasinya bila menduga ada penyalahgunaan. Pemilik bertanggung jawab atas penggunaan layanan oleh anggota dan kunci API organisasinya.",
        "Every user must keep passwords, session tokens and API keys confidential and promptly tell their organisation admin about suspected misuse. The Owner is responsible for the use of the service by the organisation's members and API keys."),
    ] },
    { id: "penggunaan", title: { id: "Penggunaan yang diizinkan dan dilarang", en: "Acceptable use" }, blocks: [
      p("Anda boleh memakai layanan untuk keperluan internal organisasi Anda. Anda tidak boleh:", "You may use the service for your organisation's internal purposes. You must not:"),
      ul(
        ["mengakali batas paket, batas laju permintaan, kontrol akses atau langkah keamanan lain;", "circumvent plan limits, request rate limits, access controls or other security measures;"],
        ["mengambil data secara otomatis di luar batas paket Anda, atau menjual kembali data mentah;", "extract data automatically beyond your plan limits, or resell raw data;"],
        ["membagikan kunci API atau akun kepada pihak di luar organisasi Anda;", "share API keys or accounts with anyone outside your organisation;"],
        ["menyajikan estimasi atau proyeksi dari layanan ini sebagai angka resmi pemerintah;", "present estimates or forecasts from this service as official government figures;"],
        ["menguji, memindai atau membebani sistem tanpa izin tertulis Penyelenggara.", "probe, scan or load-test the systems without the Operator's written permission."],
      ),
    ] },
    { id: "paket", title: { id: "Paket, uji coba dan pembayaran", en: "Plans, trial and payment" }, blocks: [
      p("Hak akses setiap paket (misalnya horizon proyeksi, kedalaman riwayat, skenario tersimpan, format laporan, aturan pemberitahuan, kursi dan akses API) ditampilkan di halaman Akun dan ditegakkan oleh server.",
        "Each plan's entitlements (for example forecast horizon, history depth, saved scenarios, report formats, alert rules, seats and API access) are shown on the Account page and enforced by the server."),
      p("Tidak ada pembayaran di dalam aplikasi dan tidak ada data pembayaran yang dikumpulkan. Pemilik atau Admin dapat mengajukan perubahan paket; permintaan ditinjau secara manual oleh Penyelenggara, dan harga serta syarat komersial disepakati secara terpisah di luar aplikasi.",
        "There is no in-app payment and no payment data is collected. An Owner or Admin may request a plan change; requests are reviewed manually by the Operator, and prices and commercial terms are agreed separately outside the app."),
    ] },
    { id: "data", title: { id: "Data sumber dan lisensinya", en: "Source data and licences" }, blocks: [
      ul(
        ["**ICP resmi:** keputusan menteri dan pengumuman Kementerian ESDM (Ditjen Migas).", "**Official ICP:** ministerial decrees and announcements of the Ministry of Energy and Mineral Resources (DG Oil and Gas)."],
        ["**Brent dan WTI:** U.S. Energy Information Administration, domain publik.", "**Brent and WTI:** U.S. Energy Information Administration, public domain."],
        ["**Dubai:** World Bank Commodity Price Data (Pink Sheet), lisensi CC BY 4.0.", "**Dubai:** World Bank Commodity Price Data (Pink Sheet), CC BY 4.0 licence."],
        ["**Kurs USD/IDR:** kurs referensi JISDOR Bank Indonesia.", "**USD/IDR rate:** Bank Indonesia JISDOR reference rate."],
      ),
      p("Bila Anda mendistribusikan ulang data, Anda wajib menyebutkan sumbernya dan mematuhi ketentuan sumber tersebut. Status verifikasi setiap bulan ICP ditampilkan apa adanya di Riwayat harga; bulan tanpa bukti tercatat ditandai demikian.",
        "If you redistribute data you must credit the source and follow that source's terms. The verification status of every ICP month is shown as recorded in Price history; months without recorded evidence are marked as such."),
    ] },
    { id: "bukan-nasihat", title: { id: "Bukan nasihat; sifat proyeksi", en: "Not advice; nature of forecasts" }, blocks: [
      p("Estimasi dan proyeksi adalah keluaran metode statistik yang diestimasi dari data historis. Kesalahan historisnya diukur pada periode uji dan ditampilkan bersama pembandingnya; rentang proyeksi bersifat empiris dan tidak dijamin terkalibrasi, dan proyeksi bulan mendatang bersyarat pada asumsi harga acuan. Keluaran tersebut bukan nasihat investasi, perdagangan, perpajakan atau anggaran, dan bukan pengganti penetapan resmi ICP.",
        "Estimates and forecasts are outputs of statistical methods estimated from historical data. Their historical error is measured over a test period and shown with its comparison baseline; forecast ranges are empirical and not guaranteed to be calibrated, and forecasts for future months are conditional on benchmark assumptions. They are not investment, trading, tax or budgeting advice and do not replace the official ICP determination."),
      p("Anda bertanggung jawab atas keputusan yang diambil berdasarkan informasi di layanan ini.", "You are responsible for decisions taken on the basis of information in this service."),
    ] },
    { id: "ketersediaan", title: { id: "Ketersediaan dan perubahan layanan", en: "Availability and changes to the service" }, blocks: [
      p("Layanan disediakan sebagaimana adanya dan sebagaimana tersedia. Pembaruan data bergantung pada sumber eksternal; status kesegaran data selalu ditampilkan. Penyelenggara berupaya menjaga ketersediaan tetapi tidak menjamin layanan bebas gangguan atau kesalahan. Fitur dapat ditambah, diubah atau dihentikan; perubahan yang memengaruhi paket Anda diumumkan di dalam aplikasi.",
        "The service is provided as is and as available. Data refreshes depend on external sources; data freshness is always shown. The Operator aims to keep the service available but does not guarantee uninterrupted or error-free operation. Features may be added, changed or withdrawn; changes affecting your plan are announced in the app."),
    ] },
    { id: "penangguhan", title: { id: "Penangguhan dan pengakhiran", en: "Suspension and termination" }, blocks: [
      p("Penyelenggara dapat menangguhkan organisasi atau anggota yang melanggar ketentuan ini atau membahayakan keamanan layanan. Penangguhan dicatat dalam log audit beserta alasannya. Organisasi dapat berhenti memakai layanan kapan saja; penghapusan (anonimisasi) data diajukan di halaman Akun dan dilaksanakan operator sebagaimana dijelaskan dalam Kebijakan Privasi.",
        "The Operator may suspend an organisation or member that breaches these terms or endangers the security of the service. Suspensions are recorded in the audit log with a reason. An organisation may stop using the service at any time; deletion (anonymisation) of data is requested on the Account page and carried out by the operator as described in the Privacy Policy."),
    ] },
    { id: "hki", title: { id: "Hak kekayaan intelektual", en: "Intellectual property" }, blocks: [
      p("Perangkat lunak, desain, ilustrasi dan teks layanan adalah milik Penyelenggara atau pemberi lisensinya. Data sumber tetap tunduk pada lisensi sumbernya. Konten yang Anda buat (nama skenario, asumsi, catatan, aturan) tetap milik organisasi Anda; Anda memberi Penyelenggara izin terbatas untuk menyimpan dan memprosesnya hanya untuk menyediakan layanan.",
        "The software, design, illustrations and text of the service belong to the Operator or its licensors. Source data remains subject to its source licence. Content you create (scenario names, assumptions, notes, rules) remains your organisation's; you grant the Operator a limited permission to store and process it only to provide the service."),
    ] },
    { id: "tanggung-jawab", title: { id: "Batas tanggung jawab", en: "Limitation of liability" }, blocks: [
      p("Sejauh diizinkan hukum yang berlaku, Penyelenggara tidak bertanggung jawab atas kerugian tidak langsung yang timbul dari penggunaan proyeksi, estimasi atau data pihak ketiga. Batas tanggung jawab: [[liability]].",
        "To the extent permitted by applicable law, the Operator is not liable for indirect losses arising from the use of forecasts, estimates or third-party data. Limitation of liability: [[liability]]."),
      p("Hukum yang berlaku dan penyelesaian sengketa: [[law]].", "Governing law and disputes: [[law]]."),
    ] },
    { id: "perubahan", title: { id: "Perubahan ketentuan", en: "Changes to these terms" }, blocks: [
      p("Ketentuan ini dapat diperbarui. Tanggal versi tercantum di bagian atas halaman, dan perubahan penting diumumkan di dalam aplikasi [[notice]] sebelum berlaku.",
        "These terms may be updated. The version date is shown at the top of the page, and material changes are announced in the app [[notice]] before they take effect."),
    ] },
    { id: "kontak", title: { id: "Kontak", en: "Contact" }, blocks: [
      p("Pertanyaan tentang ketentuan ini: [[contact]]. Sampai saluran tersebut ditetapkan, pemilik atau admin organisasi Anda dapat menghubungi Penyelenggara melalui jalur yang disepakati saat aktivasi.",
        "Questions about these terms: [[contact]]. Until that channel is set, your organisation owner or admin can reach the Operator through the channel agreed at activation."),
    ] },
  ],
};

export const PRIVACY: LegalDoc = {
  intro: {
    id: "Data apa yang disimpan ICP Intelligence, untuk apa, siapa yang dapat melihatnya, dan apa yang tidak dikumpulkan.",
    en: "What data ICP Intelligence stores, what for, who can see it, and what is not collected.",
  },
  draftNote: TERMS.draftNote,
  sections: [
    { id: "penyelenggara", title: { id: "Penyelenggara dan ruang lingkup", en: "Operator and scope" }, blocks: [
      p("Kebijakan ini berlaku untuk aplikasi web ICP Intelligence dan API-nya. Pengendali data adalah [[entity]], [[address]]. Organisasi pelanggan menentukan siapa yang diundang ke ruang kerjanya.",
        "This policy covers the ICP Intelligence web app and its API. The data controller is [[entity]], [[address]]. Customer organisations decide who is invited to their workspace."),
    ] },
    { id: "data-disimpan", title: { id: "Data yang kami simpan", en: "Data we store" }, blocks: [
      ul(
        ["**Akun:** alamat email, nama, peran, organisasi, waktu pendaftaran dan masuk terakhir, tanda bila kata sandi sementara harus diganti beserta waktu kedaluwarsanya, serta kolom waktu verifikasi email (belum dipakai selama pengiriman email nonaktif).", "**Account:** email address, name, role, organisation, registration and last sign-in time, a flag when a temporary password must be replaced together with its expiry time, and an email-verification time field (unused while email delivery is disabled)."],
        ["**Kata sandi:** hanya hash scrypt dengan salt acak 16 byte; kata sandi tidak pernah dicatat atau dapat dibaca kembali.", "**Passwords:** only a scrypt hash with a random 16-byte salt; passwords are never logged and cannot be read back."],
        ["**Sesi:** token acak yang disimpan sebagai hash SHA-256, waktu dibuat, terakhir dipakai, kedaluwarsa (30 hari sejak masuk, atau lebih awal setelah 7 hari tidak dipakai menurut pengaturan bawaan) dan dicabut, serta agen pengguna browser (maksimal 200 karakter) agar Anda dapat mengenali sesi.", "**Sessions:** a random token stored as a SHA-256 hash, created, last-used, expiry (30 days after sign-in, or earlier after 7 days without use under the default setting) and revocation times, and the browser user agent (at most 200 characters) so you can recognise the session."],
        ["**Kunci API:** nama, 12 karakter awal untuk pengenalan, hash kunci, pembuat dan waktu terakhir dipakai. Kunci lengkap hanya ditampilkan sekali.", "**API keys:** name, the first 12 characters for recognition, a hash of the key, creator and last-used time. The full key is shown only once."],
        ["**Penggunaan:** jumlah tindakan (misalnya proyeksi dijalankan, laporan dibuat, permintaan API) per organisasi, anggota atau kunci, dengan waktu dan ID permintaan, untuk menegakkan batas paket, termasuk kuota permintaan API bulanan.", "**Usage:** counts of actions (for example forecasts run, reports generated, API requests) per organisation, member or key, with time and request id, to enforce plan limits, including the monthly API request quota."],
        ["**Log audit:** tindakan sensitif dan percobaan yang ditolak (masuk, undangan, perubahan peran, paket atau status, kunci API, aktivasi data) dengan pelaku, tindakan, sasaran, hasil, waktu dan ID permintaan. Log audit tidak memuat kata sandi, token atau alamat IP.", "**Audit log:** sensitive actions and denied attempts (sign-in, invitations, role, plan or status changes, API keys, data activation) with actor, action, target, outcome, time and request id. The audit log contains no passwords, tokens or IP addresses."],
        ["**Konten organisasi:** skenario tersimpan, aturan dan kotak masuk pemberitahuan, proyeksi yang dijalankan beserta asumsinya, permintaan perubahan paket dan pengajuan penghapusan beserta catatan atau alasannya, serta laporan yang dibuat. Berkas laporan (PDF, Excel, CSV) disimpan di penyimpanan objek server agar dapat diunduh kembali.", "**Organisation content:** saved scenarios, alert rules and the notification inbox, forecasts run with their assumptions, plan change requests and deletion requests with their notes or reasons, and generated reports. Report files (PDF, Excel, CSV) are kept in server object storage so they can be downloaded again."],
      ),
    ] },
    { id: "teknis", title: { id: "Data teknis dan alamat IP", en: "Technical data and IP addresses" }, blocks: [
      p("Log permintaan aplikasi mencatat metode, jalur, status, durasi dan ID permintaan, tanpa alamat IP dan tanpa token. Untuk membatasi laju permintaan, alamat IP klien dipakai sementara di memori server (jendela sekitar satu menit) dan oleh gateway Cloudflare; untuk permintaan yang sudah masuk, gateway memakai hash kredensial, bukan token itu sendiri. Untuk melindungi akun dari tebakan kata sandi, server mencatat percobaan masuk yang gagal di memori selama 15 menit dengan kunci berupa hash SHA-256 dari alamat e-mail yang dinormalkan, digabung dengan alamat IP klien; masuk yang berhasil hanya menghapus catatan untuk alamat e-mail dan alamat IP itu (catatan lain berakhir dengan sendirinya setelah 15 menit), dan semuanya hilang saat server dimulai ulang. Alamat IP tidak disimpan di basis data aplikasi.",
        "Application request logs record method, path, status, duration and request id, without IP addresses and without tokens. To limit request rates, the client IP address is used temporarily in server memory (a window of about one minute) and by the Cloudflare gateway; for signed-in requests the gateway uses a hash of the credential, not the token itself. To protect accounts against password guessing, the server keeps failed sign-in attempts in memory for 15 minutes, keyed by a SHA-256 hash of the normalised e-mail address combined with the client IP address; a successful sign-in clears only the record for that e-mail address and IP address (other records expire on their own after 15 minutes), and all of it is lost when the server restarts. IP addresses are not stored in the application database."),
    ] },
    { id: "browser", title: { id: "Data di browser Anda", en: "Data in your browser" }, blocks: [
      p("Aplikasi tidak memasang cookie. Ia menyimpan sedikit data di penyimpanan browser Anda sendiri:", "The app sets no cookies. It keeps a little data in your own browser storage:"),
      ul(
        ["**localStorage \"icp.sessionToken\":** token sesi Anda, sampai Anda keluar atau sesi berakhir (30 hari, atau lebih awal bila tidak dipakai). Siapa pun yang memakai browser yang sama dapat memakainya, jadi keluarlah di perangkat bersama.", "**localStorage \"icp.sessionToken\":** your session token, until you sign out or the session ends (30 days, or earlier when unused). Anyone using the same browser can use it, so sign out on shared devices."],
        ["**localStorage \"icp.locale\":** pilihan bahasa.", "**localStorage \"icp.locale\":** your language choice."],
        ["**localStorage \"icp.onboarding.dismissed.<id organisasi>:<nama anggota>\":** tanda bahwa kartu \"Langkah pertama\" sudah ditutup.", "**localStorage \"icp.onboarding.dismissed.<organisation id>:<member name>\":** a flag that the \"First steps\" card was dismissed."],
        ["**sessionStorage \"icp.operatorToken\":** hanya untuk operator platform, terhapus saat tab ditutup.", "**sessionStorage \"icp.operatorToken\":** platform operators only, cleared when the tab is closed."],
        ["**sessionStorage \"icp.compare.*\":** hasil perbandingan skenario terakhir per tab, agar tombol Kembali tidak menjalankan ulang perbandingan; dihapus saat keluar atau saat tab ditutup.", "**sessionStorage \"icp.compare.*\":** the last scenario comparison per tab, so Back does not re-run it; removed on sign-out or when the tab is closed."],
      ),
    ] },
    { id: "tidak-dikumpulkan", title: { id: "Yang tidak kami kumpulkan", en: "What we do not collect" }, blocks: [
      p("Tidak ada data pembayaran, pelacak iklan, analitik pihak ketiga, atau sumber daya dari pihak ketiga yang dimuat oleh aplikasi. Layanan tidak mengirim email (fungsi email ada di perangkat lunak tetapi dinonaktifkan); undangan dan kata sandi sementara diserahkan oleh admin organisasi Anda. Modul pembayaran juga ada tetapi dinonaktifkan dan tidak memproses data apa pun.",
        "No payment data, advertising trackers, third-party analytics, or third-party resources are loaded by the app. The service sends no email (email functions exist in the software but are disabled); invitations and temporary passwords are handed over by your organisation admin. A payment module also exists but is disabled and processes no data."),
    ] },
    { id: "tujuan", title: { id: "Tujuan dan dasar pemrosesan", en: "Purposes and legal basis" }, blocks: [
      p("Data dipakai hanya untuk menyediakan layanan, menegakkan hak akses paket, menjaga keamanan (deteksi penyalahgunaan dan jejak audit) dan memberikan dukungan. Data tidak dijual dan tidak dipakai untuk iklan. Dasar hukum: [[basis]].",
        "Data is used only to provide the service, enforce plan entitlements, keep it secure (abuse detection and audit trail) and give support. Data is not sold and not used for advertising. Legal basis: [[basis]]."),
    ] },
    { id: "akses", title: { id: "Siapa yang dapat melihat data", en: "Who can see the data" }, blocks: [
      ul(
        ["Anggota organisasi Anda melihat konten organisasi sesuai perannya; organisasi lain tidak pernah dapat melihatnya (pemisahan ini diuji otomatis).", "Members of your organisation see its content according to their role; other organisations can never see it (this separation is tested automatically)."],
        ["Admin dan Pemilik melihat daftar anggota, peran, status dan waktu masuk terakhir.", "Admins and Owners see the member list, roles, status and last sign-in time."],
        ["Operator platform melihat daftar organisasi, paket, status, permintaan perubahan paket dan log audit untuk administrasi dan keamanan.", "Platform operators see organisations, plans, status, plan change requests and the audit log for administration and security."],
      ),
    ] },
    { id: "pemroses", title: { id: "Penyedia infrastruktur dan lokasi data", en: "Infrastructure providers and data location" }, blocks: [
      p("Aplikasi web dan gateway API berjalan di Cloudflare Workers. Pada lingkungan uji (staging), metadata aplikasi disimpan di Cloudflare D1 dan berkas di Cloudflare KV. Server analitik produksi: [[hostingBackend]]. Transfer data lintas negara: [[transfer]].",
        "The web app and API gateway run on Cloudflare Workers. In the staging environment, application metadata is stored in Cloudflare D1 and files in Cloudflare KV. Production analytics server: [[hostingBackend]]. Cross-border transfers: [[transfer]]."),
      p("Data pasar diambil dari Kementerian ESDM, U.S. EIA, Bank Dunia dan Bank Indonesia; tidak ada data pengguna yang dikirim ke sumber tersebut.", "Market data is fetched from the Ministry of Energy and Mineral Resources, the U.S. EIA, the World Bank and Bank Indonesia; no user data is sent to those sources."),
    ] },
    { id: "retensi", title: { id: "Masa simpan", en: "Retention" }, blocks: [
      ul(
        ["Sesi berakhir 30 hari setelah masuk, setelah 7 hari tidak dipakai (pengaturan bawaan), atau saat Anda keluar atau mengakhirinya di halaman Akun; mengganti kata sandi mencabut semua sesi Anda termasuk sesi yang sedang dipakai (Anda masuk kembali dengan kata sandi baru), dan menonaktifkan anggota mencabut semua sesinya. Kata sandi sementara berlaku 72 jam (pengaturan bawaan).", "Sessions end 30 days after sign-in, after 7 days without use (default setting), or when you sign out or end them on the Account page; changing a password revokes all of your sessions including the one in use (you sign in again with the new password), and disabling a member revokes all of their sessions. Temporary passwords are valid for 72 hours (default setting)."],
        ["Data akun: [[retentionAccount]].", "Account data: [[retentionAccount]]."],
        ["Log audit: [[retentionAudit]]. Log audit tidak pernah dihapus otomatis; operator hanya dapat memangkas entri yang lebih tua dari masa simpan yang ditetapkan, secara manual.", "Audit log: [[retentionAudit]]. The audit log is never deleted automatically; the operator can only prune entries older than the configured retention period, manually."],
        ["Ekspor data organisasi dibuat saat diminta dan tidak disimpan di server. Ekspor tidak berisi hash kata sandi, token atau kunci; daftar sesi hanya memuat sesi pemilik yang mengunduh, dan identitas serta catatan internal operator platform disamarkan.", "Organisation data exports are generated on request and not kept on the server. They contain no password, token or key hashes; the session list holds only the downloading owner's own sessions, and platform-operator identities and internal notes are redacted."],
        ["Laporan, proyeksi dan konten organisasi: [[retentionContent]]. Saat ini konten tersebut tidak dihapus otomatis.", "Reports, forecasts and organisation content: [[retentionContent]]. This content is currently not deleted automatically."],
      ),
    ] },
    { id: "keamanan", title: { id: "Keamanan", en: "Security" }, blocks: [
      p("Kata sandi, token sesi dan kunci API hanya disimpan sebagai hash; data setiap organisasi dipisahkan di server; permintaan dibatasi lajunya; tindakan sensitif diaudit. Tidak ada sistem yang sepenuhnya aman; laporkan dugaan masalah keamanan melalui [[contact]].",
        "Passwords, session tokens and API keys are stored only as hashes; each organisation's data is separated on the server; requests are rate limited; sensitive actions are audited. No system is completely secure; report suspected security issues through [[contact]]."),
    ] },
    { id: "hak", title: { id: "Hak Anda", en: "Your rights" }, blocks: [
      p("Anda dapat melihat dan mengubah nama di halaman Akun serta melihat dan mengakhiri sesi aktif. Pemilik organisasi dapat mengunduh salinan data organisasi (berkas ZIP) di halaman Akun, bagian Data & privasi. Setiap anggota dapat mengajukan penghapusan akunnya sendiri, dan pemilik dapat mengajukan penghapusan seluruh organisasi, di bagian yang sama; pengajuan dapat ditarik oleh pengajunya selama belum diproses. Operator platform memproses pengajuan secara manual: nama dan e-mail dianonimkan, akun dinonaktifkan dan sesi serta kunci API dicabut; log audit, catatan pemakaian, catatan laporan dan proyeksi (tanpa data pribadi selain ID pelaku) serta data resmi publik tetap disimpan. Untuk koreksi data atau pertanyaan lain, hubungi [[contact]]; permintaan ditanggapi dalam [[response]]. Admin organisasi juga dapat menonaktifkan akun anggota.",
        "You can view and change your name on the Account page and see and end your active sessions. The organisation owner can download a copy of the organisation's data (a ZIP file) on the Account page, Data & privacy section. Every member can request deletion of their own account, and the owner can request deletion of the whole organisation, in the same section; a request can be withdrawn by the person who made it until it is processed. The platform operator processes requests manually: name and e-mail are anonymised, the account is disabled and sessions and API keys are revoked; the audit log, usage records, report and forecast records (with no personal data beyond actor ids) and public official data are kept. For corrections or other questions, contact [[contact]]; requests are answered within [[response]]. Organisation admins can also disable a member's account."),
    ] },
    { id: "perubahan-privasi", title: { id: "Perubahan kebijakan", en: "Changes to this policy" }, blocks: [
      p("Kebijakan ini dapat diperbarui; tanggal versi tercantum di bagian atas, dan perubahan penting diumumkan di dalam aplikasi [[notice]] sebelum berlaku.",
        "This policy may be updated; the version date is shown at the top, and material changes are announced in the app [[notice]] before they take effect."),
    ] },
    { id: "kontak-privasi", title: { id: "Kontak", en: "Contact" }, blocks: [
      p("Pertanyaan atau permintaan terkait privasi: [[contact]].", "Privacy questions or requests: [[contact]]."),
    ] },
  ],
};

/** Every placeholder key used in a document (for tests and the owner-input list). */
export function placeholdersIn(doc: LegalDoc): string[] {
  const keys = new Set<string>();
  const scan = (l: L) => { for (const s of [l.id, l.en]) for (const m of s.matchAll(/\[\[(\w+)\]\]/g)) keys.add(m[1]!); };
  scan(doc.intro);
  for (const sec of doc.sections) for (const b of sec.blocks) ("p" in b ? [b.p] : b.ul).forEach(scan);
  return [...keys];
}
