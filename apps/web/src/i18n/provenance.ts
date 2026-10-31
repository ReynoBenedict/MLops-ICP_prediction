/**
 * Value provenance strings: [Indonesian, English]. Merged into the eager DICT by dictionary.ts because
 * the public site shows them too (KindChip on the landing and the methodology page, evidence badges in
 * the landing's market panel), while most other workspace strings load lazily (i18n/workspace.ts).
 * - kind.*: value kinds (official, estimate awaiting publication, forecast, legacy record);
 * - src.*: where a forecast input comes from (observed, your scenario, last value held);
 * - verify.*: evidence states of a month (fallback labels; the server labels win when available).
 */
import type { Pair } from "./pair";

export const PROVENANCE = {
  "kind.estimate_awaiting_publication": ["Estimasi (menunggu publikasi)", "Estimate (awaiting publication)"],
  "kind.estimate.short": ["bulan sudah berakhir, ICP resmi belum terbit", "month ended, official ICP not yet published"],
  "kind.forecast.short": ["bulan belum berakhir, harga acuan diasumsikan", "month not ended, benchmarks assumed"],
  "kind.future_forecast": ["Proyeksi", "Forecast"],
  "kind.legacy_record": ["Rekam lama", "Legacy record"],
  "kind.estimate.help": ["Bulan ini sudah berakhir tetapi ICP resminya belum ditetapkan ESDM. Harga acuan bulan tersebut sudah teramati, sehingga ini adalah perkiraan nilai resmi yang akan terbit.", "The month has ended but ESDM has not published its ICP yet. The month's benchmark prices are observed, so this estimates the official value that will be published."],
  "kind.forecast.help": ["Bulan ini belum berakhir. Harga acuannya belum diketahui dan berasal dari skenario Anda atau nilai terakhir yang dipertahankan.", "The month has not ended. Its benchmark prices are unknown and come from your scenario or the last known value."],
  "kind.official_published": ["Sudah resmi", "Now official"],
  "src.observed": ["Teramati", "Observed"],
  "src.scenario": ["Skenario Anda", "Your scenario"],
  "src.assumed_hold_last": ["Nilai terakhir dipertahankan", "Held at last value"],
  "src.unavailable": ["Tidak tersedia", "Unavailable"],
  // --- verification vocabulary (fallback labels; the server labels win when available) ---
  "verify.two_documents": ["Dua dokumen ESDM sepakat", "Two ESDM documents agree"],
  "verify.two_documents.help": ["Dua dokumen resmi ESDM yang berbeda (keputusan menteri dan pengumuman) menyebut nilai yang sama.", "Two different official ESDM documents (decree and announcement) state the same value."],
  "verify.decree_text": ["Teks keputusan menteri", "Ministerial decree text"],
  "verify.decree_text.help": ["Nilai dibaca dari teks keputusan menteri.", "The value was read from the text of the ministerial decree."],
  "verify.document_plus_legacy": ["Satu dokumen ESDM + arsip lama", "One ESDM document + legacy archive"],
  "verify.document_plus_legacy.help": ["Satu dokumen ESDM ditambah catatan dari arsip lama; belum ada dokumen resmi kedua.", "One ESDM document plus a legacy archive record; no second official document yet."],
  "verify.single_document": ["Satu dokumen ESDM", "One ESDM document"],
  "verify.single_document.help": ["Hanya satu dokumen resmi tercatat untuk bulan ini.", "Only one official document is recorded for this month."],
  "verify.ocr_single": ["Dibaca otomatis (OCR), satu dokumen", "Machine-read (OCR), one document"],
  "verify.ocr_single.help": ["Nilai dibaca otomatis dari dokumen pindaian dan menunggu pemeriksaan kedua.", "The value was read automatically from a scanned document and awaits a second check."],
  "verify.conflict": ["Sumber tidak sepakat", "Sources disagree"],
  "verify.conflict.help": ["Dokumen yang tercatat menyebut nilai atau nomor keputusan berbeda.", "The recorded documents state different values or decree numbers."],
  "verify.no_evidence": ["Bukti belum tercatat", "No evidence recorded"],
  "verify.no_evidence.help": ["Belum ada dokumen sumber yang tercatat untuk bulan ini di platform. Nilainya berasal dari riwayat resmi yang dimuat, tetapi buktinya belum dilampirkan.", "No source document is recorded for this month in the platform yet. The value comes from the loaded official history, but its evidence is not attached."],
  "verify.provider_published": ["Dipublikasikan penyedia data", "Published by the data provider"],
  "verify.provider_published.help": ["Harga acuan dan kurs dipublikasikan oleh penyedianya (EIA, Bank Dunia, Bank Indonesia); tidak ada pemeriksaan dua dokumen.", "Benchmarks and FX are published by their provider (EIA, World Bank, Bank Indonesia); there is no two-document check."],
  "verify.column": ["Bukti sumber", "Source evidence"],
  "verify.summaryTitle": ["Bukti per bulan dalam rentang paket Anda", "Evidence per month in your plan range"],
  "verify.dissentMonths": ["{n} bulan memiliki bacaan sumber lain yang berbeda; status bulan tersebut tidak berubah. Lihat catatan di tabel.", "{n} {n|month has|months have} a differing reading from another source; their status is unchanged. See the note in the table."],
} as const satisfies Record<string, Pair>;
