import re
import pandas as pd
from pathlib import Path

BASE = Path("/Users/ambakinanti/Desktop/Malta_portfolio")
IN_FP = BASE / "data_processed/spa_research/malta_spa_outlet_master.csv"
OUT_FP = BASE / "data_processed/spa_research/malta_spa_outlet_master_clean.csv"

df = pd.read_csv(IN_FP).copy()

for c in ["outlet_name", "location_text", "source_name", "source_url"]:
    if c not in df.columns:
        df[c] = None

df["outlet_name"] = df["outlet_name"].fillna("").astype(str).str.strip()
df["location_text"] = df["location_text"].fillna("").astype(str).str.strip()

df["outlet_name_norm"] = (
    df["outlet_name"]
    .str.lower()
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

bad_exact = {
    "", "404 page", "sorry", "we’re sorry.", "we're sorry.", "tripadvisor.com",
    "address", "award-winning", "explore", "find out more", "head office",
    "myoka ltd.", "new", "spa in malta", "leading spa operator in malta",
    "experience an", "@carismaspamalta", "wellness@carismaspa.com",
    "chat with us or fill the form below", "including public holidays",
    "real people, real reviews", "personalise your experience",
    "best spa treatments near me in malta",
    "discover the art of wellness, rooted in malta",
    "35+",
}

bad_patterns = [
    r"^\d+/\d+$",
    r"^\+?\d[\d\s]+$",
    r"^t:\s*",
    r"^tel:\s*",
    r"^e:\s*",
    r"@",
    r"^\d{2}:\d{2}\s*-\s*\d{2}:\d{2}$",
    r"^mon\b",
    r"^sat\b",
    r"^sun\b",
    r"^click to see our .* menu$",
    r"^dji_\d+",
    r"^over 4000\+",
]

good_keywords = [
    "spa", "wellness", "massage", "hilton", "marriott", "doubletree",
    "radisson", "hyatt", "novotel", "intercontinental", "hotel",
    "resort", "essensi", "carisma", "myoka", "phoenicia", "hugo",
    "ramla", "riviera", "salini", "golden sands", "st. paul"
]

mask_bad = df["outlet_name_norm"].isin(bad_exact)
for pat in bad_patterns:
    mask_bad = mask_bad | df["outlet_name_norm"].str.contains(pat, case=False, regex=True)

mask_good_keyword = False
for kw in good_keywords:
    mask_good_keyword = mask_good_keyword | df["outlet_name_norm"].str.contains(re.escape(kw), case=False, regex=True)

word_count = df["outlet_name"].str.split().str.len().fillna(0)
mask_not_sentence = ~df["outlet_name"].str.contains(
    r"[.!?]|welcome|enjoy|discover|offers|where|with views|surrounded by|steps from",
    case=False,
    regex=True
)
mask_reasonable_len = df["outlet_name"].str.len().between(4, 80)

mask_keep = (~mask_bad) & mask_reasonable_len & (mask_good_keyword | ((word_count <= 8) & mask_not_sentence))

df2 = df.loc[mask_keep].copy()

replace_map = {
    "carisma spa malta": "Carisma Spa Malta",
    "essensi spa": "Essensi Spa",
    "doubletree by hilton": "DoubleTree by Hilton Malta",
    "doubletree by hilton malta": "DoubleTree by Hilton Malta",
    "hilton malta": "Hilton Malta",
    "hilton malta st. julian’s": "Hilton Malta",
    "hilton malta st. julian's": "Hilton Malta",
    "malta marriott resort & spa": "Malta Marriott Resort & Spa",
    "radisson blu resort & spa, golden sands": "Radisson Blu Resort & Spa, Malta Golden Sands",
    "radisson blu resort & spa, malta golden sands": "Radisson Blu Resort & Spa, Malta Golden Sands",
    "intercontinental hotel - st. julian's": "InterContinental Malta",
    "carisma spa & wellness": "Carisma Spa & Wellness",
    "66 st. paul's & spa": "66 Saint Paul's & Spa",
    "66 saint paul’s & spa - by appointment only": "66 Saint Paul's & Spa",
    "66 st. pauls street, valletta, malta": "66 Saint Paul's & Spa",
    "ax odycy - qawra": "AX Odycy Spa",
    "hugo's h hotel - st. julian's": "Hugo's H Hotel Spa",
    "hyatt regency - st. julian's": "Hyatt Regency Malta Spa",
    "novotel malta sliema": "Novotel Malta Sliema Spa",
    "ramla bay resort - mellieħa": "Ramla Bay Resort Spa",
    "riviera spa resort - mellieħa": "Riviera Spa Resort",
    "grand hotel excelsior - valletta": "Grand Hotel Excelsior Spa",
    "salini resort, salina - by appointment only": "Salini Resort Spa",
}

df2["outlet_name_std"] = df2["outlet_name_norm"].map(replace_map).fillna(df2["outlet_name"])

df2["rating_num"] = pd.to_numeric(df2.get("rating"), errors="coerce")
df2["review_count_num"] = pd.to_numeric(df2.get("review_count"), errors="coerce")

df2["priority"] = (
    df2["rating_num"].fillna(0) * 20
    + df2["review_count_num"].fillna(0).clip(upper=10000) ** 0.5
    + df2["source_name"].fillna("").str.contains("tripadvisor|fresha", case=False, regex=True).astype(int) * 2
)

df2["outlet_name_std_norm"] = (
    df2["outlet_name_std"]
    .fillna("")
    .astype(str)
    .str.lower()
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

df2 = df2.sort_values(["outlet_name_std_norm", "priority"], ascending=[True, False])
df2 = df2.drop_duplicates(subset=["outlet_name_std_norm"], keep="first").copy()

final_cols = [
    "outlet_name_std", "outlet_name", "location_text", "rating", "review_count",
    "source_name", "source_url"
]
for c in final_cols:
    if c not in df2.columns:
        df2[c] = None

df2 = df2[final_cols].rename(columns={"outlet_name_std": "outlet_name_final"})
df2 = df2.sort_values("outlet_name_final").reset_index(drop=True)

df2.to_csv(OUT_FP, index=False)

print("saved:", OUT_FP)
print("shape before:", df.shape)
print("shape after :", df2.shape)
print("unique outlet_name_final:", df2["outlet_name_final"].nunique())
print(df2.to_string(index=False))
