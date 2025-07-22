import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Load environment variables from .env file
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

# Set up SQLAlchemy connection to PostgreSQL using environment variables
engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

# Define valid values for region fields
VALID_REGIONS = {"AMER", "EMEA", "APAC", "DASH", "JAPAN", "GLOBAL"}

# Create an in-memory list to store all DQ issues
dq_issues = []

# ──────────────────────────────────────────────────────────────────────────────
# Utility functions
# ──────────────────────────────────────────────────────────────────────────────

def normalize_columns(df):
    """Normalize column names to snake_case and remove special characters"""
    return df.rename(columns=lambda c: re.sub(r"[^\w]+", "_", c.strip().lower()).strip("_"))

def clean_currency(series):
    """Remove $, commas, and replace dashes/blanks with NaN, then coerce to float"""
    return pd.to_numeric(
        series.astype(str).str.replace(r"[\$,]", "", regex=True).replace({"-": None, "": None}),
        errors="coerce"
    )

def clean_dates(df, date_cols):
    """Convert date columns to consistent YYYY-MM-DD format"""
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
    return df

def flag_issue(df, idx, issue_msg, column=None, value=None, dataset=None):
    """Track DQ issues in both df['dq_flag'] and in the dq_issues list"""
    if "dq_flag" not in df.columns:
        df["dq_flag"] = ""
    df.at[idx, "dq_flag"] += issue_msg + "; "
    dq_issues.append({
        "dataset": dataset or getattr(df, "name", "unknown"),
        "row_index": idx,
        "column": column,
        "issue": issue_msg,
        "value": str(value if value is not None else (df.at[idx, column] if column else None))
    })

def finalize_dq_flags(df):
    """Add a 'dq_check' column to indicate pass/fail based on dq_flag presence"""
    df["dq_check"] = df["dq_flag"].apply(lambda x: "Failed" if x.strip() else "Pass")
    return df

# ──────────────────────────────────────────────────────────────────────────────
# Dataset-specific ETL logic
# ──────────────────────────────────────────────────────────────────────────────

def process_bookings(path):
    df = normalize_columns(pd.read_csv(path))
    df.name = "bookings"
    df["dq_flag"] = ""

    # Rename ambiguous 'type' column to 'opp_type' (first one)
    cols = df.columns.to_list()
    for i, col in enumerate(cols):
        if col == "type":
            cols[i] = "opp_type"
            break
    for i, col in enumerate(cols):
        if col == "type_1" and i != 0:
            cols[i] = "type"
            break
    df.columns = cols

    print(df.columns)

    # Standardizing values in ACV column,a nd cleaning up date formats
    df["net_new_acv_usd"] = clean_currency(df["net_new_acv_usd"])
    df = clean_dates(df, ["close_date", "start_date", "created_date", "qualified_date", "week_start"])

    # Impute invalid Reporting Regions from CRM Region
    mask = ~df["reporting_region"].isin(VALID_REGIONS)
    df.loc[mask, "reporting_region"] = df.loc[mask, "crm_region"]
    for idx in df[mask].index:
        flag_issue(df, idx, "Reporting Region was missing or invalid and was set to CRM Region", "reporting_region")

    # Ensure opp_name starts with CHURN if ACV is negative
    churn_mask = df["net_new_acv_usd"] < 0
    opp_name_mismatch = churn_mask & ~df["opp_name"].str.startswith("CHURN", na=False)
    df.loc[opp_name_mismatch, "opp_name"] = "CHURN - " + df.loc[opp_name_mismatch, "opp_name"]
    for idx in df[opp_name_mismatch].index:
        flag_issue(df, idx, "Opp name prefixed with 'CHURN' to reflect negative ACV", "opp_name")

    # Add outcome column: 'Churn' if ACV < 0, else deal_type
    df["outcome"] = df.apply(
        lambda row: "Churn" if row["net_new_acv_usd"] < 0 else row["opp_type"],
        axis=1
    )

    # Fix contradiction: Net New + Churn + Negative ACV → update deal_type to 'Renewal'
    contradiction_mask = (
        df["net_new_acv_usd"] < 0
        & df["opp_type"].str.lower().str.contains("net new", na=False)
    )
    df.loc[contradiction_mask, "opp_type"] = "Renewal"
    for idx in df[contradiction_mask].index:
        flag_issue(df, idx, "Deal type changed from 'Net New' to 'Renewal' due to churn status", "deal_type")

    # Non-churn opps shouldn't have negative ACV
    mask = (df["net_new_acv_usd"] < 0) & (~df["opp_type"].str.lower().str.contains("churn", na=False))
    for idx in df[mask].index:
        flag_issue(df, idx, "ACV is negative, but opportunity is not marked as churn", "net_new_acv_usd")

    return finalize_dq_flags(df)

def process_forecast(path):
    df = normalize_columns(pd.read_csv(path))
    df.name = "forecast"
    df["dq_flag"] = ""

    df = df[df["product"].str.lower() != "total"]
    df["forecast_value"] = clean_currency(df["forecast_value"])

    if "crm_region" in df.columns:
        mask = ~df["region"].isin(VALID_REGIONS)
        df.loc[mask, "region"] = df.loc[mask, "crm_region"]
        for idx in df[mask].index:
            flag_issue(df, idx, "Region was invalid and imputed from CRM Region", "region")

    return finalize_dq_flags(df)

def process_pipeline(path):
    df = normalize_columns(pd.read_csv(path))
    df.name = "pipeline"
    df["dq_flag"] = ""

    df["net_new_acv_usd"] = clean_currency(df["net_new_acv_usd"])
    df = clean_dates(df, ["close_date", "start_date", "created_date", "qualified_date", "week_start"])

    mask = (df["net_new_acv_usd"] <= 0) | (df["net_new_acv_usd"].isna())
    for idx in df[mask].index:
        flag_issue(df, idx, "Pipeline ACV is zero or negative, which is invalid", "net_new_acv_usd")

    return finalize_dq_flags(df)

def process_targets(path):
    df = normalize_columns(pd.read_csv(path))
    df.name = "targets"
    df["dq_flag"] = ""

    df = df[df["type"].str.lower() != "total"]
    df["net_new_acv_target"] = clean_currency(df["net_new_acv_target"])

    mask = (df["net_new_acv_target"] <= 0) | (df["net_new_acv_target"].isna())
    for idx in df[mask].index:
        flag_issue(df, idx, "Target ACV is missing or not positive", "net_new_acv_target")

    return finalize_dq_flags(df)

# ──────────────────────────────────────────────────────────────────────────────
# Execute ETL
# ──────────────────────────────────────────────────────────────────────────────
bookings = process_bookings("seeds/bookings.csv")
forecast = process_forecast("seeds/forecast.csv")
pipeline = process_pipeline("seeds/pipeline.csv")
targets  = process_targets("seeds/targets.csv")

with engine.begin() as conn:
    conn.execute(text("DROP SCHEMA IF EXISTS dbx CASCADE"))
    conn.execute(text("CREATE SCHEMA dbx"))

bookings.to_sql("bookings", engine, schema="dbx", if_exists="append", index=False, method="multi")
forecast.to_sql("forecast", engine, schema="dbx", if_exists="append", index=False, method="multi")
pipeline.to_sql("pipeline", engine, schema="dbx", if_exists="append", index=False, method="multi")
targets.to_sql("targets", engine, schema="dbx", if_exists="append", index=False, method="multi")

pd.DataFrame(dq_issues).to_sql("dq_issues_log", engine, schema="dbx", if_exists="append", index=False, method="multi")

print("✅ Data cleaned, validated, and loaded into Postgres.")