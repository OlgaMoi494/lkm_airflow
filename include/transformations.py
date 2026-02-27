import re

import pandas as pd


def replace_nulls_logic(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna("-")


def sort_by_date_logic(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["at"] = pd.to_datetime(df["at"], errors="coerce")
    df = df.sort_values("at", ascending=True)
    return df.reset_index(drop=True)


def clean_text(text: str) -> str:
    if pd.isna(text) or text == "-":
        return "-"
    pattern = r"[^\w\s\.,!?;:\-\'\"()+=]"
    cleaned = re.sub(pattern, "", str(text))
    if not cleaned.strip():
        return "-"
    return cleaned


def clean_content_logic(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "content" in df.columns:
        df["content"] = df["content"].apply(clean_text)
    return df
