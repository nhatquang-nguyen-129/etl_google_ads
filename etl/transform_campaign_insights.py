import logging
import pandas as pd

def transform_campaign_insights(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform campaign insights of Google Ads
    ---------
    Workflow:
        1. Validate required columns
        2. Normalize date dimensions (date / year / month)
        3. Parse campaign naming convention
        4. Enrich platform dimension
        5. Return transformed DataFrame
    ---------
    Returns:
        1. DataFrame:
            Campaign insights fact table ready for analytics & BigQuery ingestion
    """

    msg = (
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of Google Ads campaign insights..."
    )
    print(msg)
    logging.info(msg)

    if df.empty:
        msg = "⚠️ [TRANSFORM] Empty campaign insights then transformation will be skipped."
        print(msg)
        logging.warning(msg)
        return df

    # ---------- Required columns ----------
    required_cols = {
        "customer_id",
        "campaign_id",
        "campaign_name",
        "date",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            "❌ [TRANSFORM] Failed to transform Google Ads campaign insights due to missing columns "
            f"{missing} then transformation will be suspended."
        )

    df = df.copy()

    # ---------- Normalize date ----------
    df["date"] = pd.to_datetime(
        df["date"],
        errors="coerce",
        utc=True
    ).dt.floor("D")

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.strftime("%Y-%m")

    # ---------- Platform ----------
    df["platform"] = "Google"

    # ---------- Campaign naming convention ----------
    df = df.assign(
        objective=df["campaign_name"].fillna("").str.split("_").str[0].fillna("unknown"),
        budget_group=df["campaign_name"].fillna("").str.split("_").str[1].fillna("unknown"),
        region=df["campaign_name"].fillna("").str.split("_").str[2].fillna("unknown"),
        category_level_1=df["campaign_name"].fillna("").str.split("_").str[3].fillna("unknown"),

        track_group=df["campaign_name"].fillna("").str.split("_").str[6].fillna("unknown"),
        pillar_group=df["campaign_name"].fillna("").str.split("_").str[7].fillna("unknown"),
        content_group=df["campaign_name"].fillna("").str.split("_").str[8].fillna("unknown"),
    )

    msg = (
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of Google Ads campaign insights."
    )
    print(msg)
    logging.info(msg)

    return df
