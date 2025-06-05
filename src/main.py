import pandas as pd
import json
import sys
import re

# Import modules from your project structure
from data_ingestion import reader
from data_cleansing import date_parser
from data_transformation import drug_mention_finder
from utils.utils import (
    is_atccode_multi_level,
    is_nct_number,
    clean_skipped_hex_sequences,
)

# Path definitions
rawdata_file_path = "./data/raw/"
cleaned_data_file_path = "./data/cleaned/"
processed_file_path = "./data/output/"
output_json = processed_file_path + "drug_journal_mentions_graph.json"


def main_pipeline():
    """
    Main function for the structured data pipeline.
    """
    print("Starting data pipeline...")

    # 1. Load Data using the reader module
    try:
        drugs_df, pubmed_df, clinical_trials_df = reader.load_data()
    except Exception as e:
        print(f"Data loading error: {e}")
        sys.exit(1)

    print("Data loaded successfully via reader module.")

    # 2. Clean text fields
    text_columns_to_clean = {
        "drugs_df": ["drug"],
        "pubmed_df": ["title", "journal"],
        "clinical_trials_df": ["scientific_title", "journal"],
    }

    print("Applying UTF-8 hex correction to text fields...")

    drugs_df["drug"] = drugs_df["drug"].astype(str).apply(clean_skipped_hex_sequences)
    pubmed_df["title"] = (
        pubmed_df["title"].astype(str).apply(clean_skipped_hex_sequences)
    )
    pubmed_df["journal"] = (
        pubmed_df["journal"].astype(str).apply(clean_skipped_hex_sequences)
    )
    clinical_trials_df["scientific_title"] = (
        clinical_trials_df["scientific_title"]
        .astype(str)
        .apply(clean_skipped_hex_sequences)
    )
    clinical_trials_df["journal"] = (
        clinical_trials_df["journal"].astype(str).apply(clean_skipped_hex_sequences)
    )

    print("UTF-8 hex correction applied.")
    # -------------------------------------------------------------------------

    # 2.a. Validate and filter drug ATC codes
    initial_drug_count = len(drugs_df)
    drugs_df = drugs_df[
        drugs_df["atccode"].apply(lambda x: is_atccode_multi_level(str(x)))
    ].copy()
    print(f"ATC codes checked: {initial_drug_count - len(drugs_df)} drugs removed.")

    # 2.b. Validate and filter clinical trial NCT numbers
    initial_trial_count = len(clinical_trials_df)
    clinical_trials_df = clinical_trials_df[
        clinical_trials_df["id"].apply(lambda x: is_nct_number(str(x)))
    ].copy()
    print(
        f"Validated NCT numbers: {initial_trial_count - len(clinical_trials_df)} trials removed."
    )

    # --- Date standardization
    print("Applying date standardization to PubMed and Clinical Trials data...")
    pubmed_df["date"] = pubmed_df["date"].apply(date_parser.standardize_date)
    clinical_trials_df["date"] = clinical_trials_df["date"].apply(
        date_parser.standardize_date
    )

    # 2.c. Save cleaned dataframes to CSV
    # When a value in a dataFrame column contains a comma (or a double quote, or a newline character)
    # , to_csv() will automatically enclose that entire field in double quotes in the output CSV file.
    print(f"Saving cleaned dataframes to '{cleaned_data_file_path}' :")
    drugs_df.to_csv(cleaned_data_file_path + "drugs_cleaned.csv", index=False)
    pubmed_df.to_csv(cleaned_data_file_path + "pubmed_cleaned.csv", index=False)
    clinical_trials_df.to_csv(
        cleaned_data_file_path + "clinical_trials_cleaned.csv", index=False
    )
    # ---------------------------------------------------------------------------

    # 3. Process Publications for Drug Mentions

    # Prepare drug list
    drugs_list_upper = [
        (drug_name, drug_name.upper())
        for drug_name in drugs_df["drug"].unique()
        if pd.notna(drug_name)
    ]

    print(
        f"Prepared list of {len(drugs_list_upper)} unique valid drug names for mention finding."
    )

    all_mentions = []

    # drug_mention_finder.find_drug_mentions should use date_parser.standardize_date internally
    print("Processing combined PubMed data for drug mentions...")
    all_mentions.extend(
        drug_mention_finder.find_drug_mentions(pubmed_df, drugs_list_upper, "pubmed")
    )

    print("Processing Clinical Trials CSV data for drug mentions...")
    all_mentions.extend(
        drug_mention_finder.find_drug_mentions(
            clinical_trials_df, drugs_list_upper, "clinical_trial"
        )
    )

    all_mentions = [
        m for m in all_mentions if m.get("date") is not None
    ]  # Ensure date is not None
    print(f"Total drug mentions found: {len(all_mentions)}")

    # 5. Save Output
    if not all_mentions:
        print(
            "Warning: No drug mentions were found in any publications. An empty graph will be produced."
        )

    try:
        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(all_mentions, f, indent=2, ensure_ascii=False)
        print(f"\nData pipeline completed successfully!")
        print(f"Output saved to '{output_json}'")
    except Exception as e:
        print(f"An error occurred while saving the output JSON file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main_pipeline()
