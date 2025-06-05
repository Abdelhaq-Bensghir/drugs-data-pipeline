import pandas as pd
import json
import re

rawdata_file_path = "./data/raw/"
drugs_csv_path = rawdata_file_path + "drugs.csv"
pubmed_csv_path = rawdata_file_path + "pubmed.csv"
pubmed_json_path = rawdata_file_path + "pubmed.json"
clinical_trials_csv_path = rawdata_file_path + "clinical_trials.csv"


def load_data():
    """
    Loads raw data into pandas DataFrames.
    Merges and de-duplicates PubMed data from CSV and JSON sources.
    Handles potential errors and exceptions.
    Malformed JSON is handled by attempting to clean trailing commas (the raw json contains a non-needed comma).

    Returns:
        tuple: A tuple containing three pandas DataFrames:
               (drugs_df, pubmed_df, clinical_trials_df)

    Raises:
        DataLoadError: If any crucial file is not found or a critical loading error occurs.
    """

    try:
        print(f"Loading drugs data from {drugs_csv_path}...")
        drugs_df = pd.read_csv(drugs_csv_path)

        print(f"Loading clinical trials data from {clinical_trials_csv_path}...")
        clinical_trials_df = pd.read_csv(clinical_trials_csv_path)

        # Load PubMed CSV
        print(f"Loading PubMed CSV data from {pubmed_csv_path}...")
        pubmed_csv_df = pd.read_csv(pubmed_csv_path)
        # Standardize 'id' column to string for reliable merging and de-duplication
        if "id" in pubmed_csv_df.columns:
            pubmed_csv_df["id"] = pubmed_csv_df["id"].astype(str)
        else:
            print(
                f"'id' column not found in {pubmed_csv_path}. De-duplication might be affected."
            )

        # Load PubMed JSON
        print(f"Loading PubMed JSON data from {pubmed_json_path}...")
        pubmed_json_df = pd.DataFrame()  # Initialize empty DataFrame
        try:
            with open(pubmed_json_path, "r", encoding="utf-8") as f:
                content = f.read()
            # Attempt to fix common JSON issues like trailing commas
            cleaned_content = re.sub(r",\s*([\]}])", r"\1", content)
            pubmed_json_data = json.loads(cleaned_content)
            pubmed_json_df = pd.DataFrame(pubmed_json_data)
            print(f"Successfully loaded and parsed {pubmed_json_path}.")
            # Standardize 'id' column to string
            if "id" in pubmed_json_df.columns:
                pubmed_json_df["id"] = pubmed_json_df["id"].astype(str)
            else:
                print(
                    f"'id' column not found in {pubmed_json_path}. De-duplication might be affected."
                )
        except json.JSONDecodeError as e:
            error_msg = f"JSONDecodeError when loading {pubmed_json_path}: {e}. Pipeline cannot proceed."
            print(error_msg)
            raise DataLoadError(error_msg) from e
        except Exception as clean_e:  # Catch other errors during JSON processing
            error_msg = f"Failed to load or parse {pubmed_json_path}. Error: {clean_e}. Pipeline cannot proceed."
            print(error_msg)
            raise DataLoadError(error_msg) from clean_e

        # Ensure both pubmed dataframes have the required columns before concat (handled by schema generally in production pipelines)
        # For simplicity, we assume 'id', 'title', 'date', 'journal' exist
        # A more robust solution might involve schema validation or explicit column selection/renaming

        # Merge PubMed data
        print("Merging PubMed CSV and JSON data...")
        if not pubmed_csv_df.empty or not pubmed_json_df.empty:
            pubmed_df = pd.concat([pubmed_csv_df, pubmed_json_df], ignore_index=True)

            # De-duplicate based on 'id'. Rows with empty string IDs might be treated as one group by drop_duplicates.
            # If 'id' column might be missing after load (despite warnings), handle here:
            if "id" in pubmed_df.columns:
                initial_pubmed_count = len(pubmed_df)
                # Consider how to handle IDs that are empty strings or NaN if they are not truly unique identifiers
                # For example, filter them out before de-duplication if they represent invalid entries:
                # pubmed_df = pubmed_df[pubmed_df['id'].str.strip() != '']
                pubmed_df = pubmed_df.drop_duplicates(subset=["id"], keep="first")
                print(
                    f"De-duplicated PubMed data: {initial_pubmed_count - len(pubmed_df)} duplicates removed. Current PubMed articles: {len(pubmed_df)}"
                )
            else:
                print(
                    "'id' column not present in merged PubMed data. Skipping de-duplication."
                )
        elif pubmed_csv_df.empty and pubmed_json_df.empty:
            print(
                "Both PubMed CSV and JSON sources are empty. Resulting PubMed DataFrame will be empty."
            )
            pubmed_df = pd.DataFrame()  # Ensure pubmed_df exists even if empty
        else:  # One is empty, the other is not
            pubmed_df = pubmed_csv_df if not pubmed_csv_df.empty else pubmed_json_df
            print("One PubMed source was empty; using the non-empty source.")

        print("Data loading and initial preparation complete.")
        return drugs_df, pubmed_df, clinical_trials_df

    except FileNotFoundError as e:
        error_msg = f"Error: An input file was not found. Details: {e}"
        print(error_msg)
        raise DataLoadError(error_msg) from e
    except Exception as e:  # Catch-all for other unexpected errors during loading
        error_msg = f"Unexpected error during data loading: {e}"
        print(error_msg)
        raise DataLoadError(error_msg) from e
