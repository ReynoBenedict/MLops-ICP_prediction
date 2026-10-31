/** legal strings: [Indonesian, English]. Merged into DICT by dictionary.ts. Long legal prose lives in pages/legal-content.ts. */
import type { Pair } from "./pair";

export const LEGAL = {
  "legal.termsTitle": ["Syarat Penggunaan", "Terms of Use"],
  "legal.privacyTitle": ["Kebijakan Privasi", "Privacy Policy"],
  "legal.updatedLabel": ["Versi", "Version"],
  "legal.updatedDate": ["Draf 19 Oktober 2026", "Draft of 19 October 2026"],
  "legal.contents": ["Daftar isi", "Contents"],
  "legal.placeholder": ["Diisi pemilik", "Owner to complete"],
  "legal.placeholderHint": ["Bagian ini belum ditetapkan oleh pemilik layanan.", "This part has not yet been decided by the service owner."],
  "legal.backToTop": ["Kembali ke daftar isi", "Back to contents"],
  "legal.related.terms": ["Baca juga Kebijakan Privasi", "See also the Privacy Policy"],
  "legal.related.privacy": ["Baca juga Syarat Penggunaan", "See also the Terms of Use"],
} as const satisfies Record<string, Pair>;
