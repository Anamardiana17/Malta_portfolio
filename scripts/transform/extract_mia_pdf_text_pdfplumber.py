from pathlib import Path
import pdfplumber

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")

INPUTS = [
    BASE / "data_raw/mia_reports/MIA_Link_Aug_2024.pdf",
    BASE / "data_raw/mia_reports/traffic_results_oct_2017.pdf",
]

OUT_DIR = BASE / "data_processed/mia_text"
OUT_DIR.mkdir(parents=True, exist_ok=True)

for pdf_fp in INPUTS:
    if not pdf_fp.exists():
        print("missing:", pdf_fp)
        continue

    parts = []
    with pdfplumber.open(str(pdf_fp)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            try:
                txt = page.extract_text() or ""
            except Exception as e:
                txt = f"\n[ERROR extracting page {i}: {e}]\n"
            parts.append(f"\n\n===== PAGE {i} =====\n\n{txt}")

        out_fp = OUT_DIR / f"{pdf_fp.stem}.txt"
        out_fp.write_text("".join(parts), encoding="utf-8")
        print("saved:", out_fp)
        print("pages:", len(pdf.pages))
