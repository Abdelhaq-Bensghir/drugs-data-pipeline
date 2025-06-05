import pandas as pd


def find_drug_mentions(publications_df, drugs_list_upper, source_type):
    """
    Identifies mentions of drugs within the titles of publications in a DataFrame.
    It constructs a list of dictionaries, each representing a drug mention event.
    The date column in publications_df is expected to be already standardized.

    Args:
        publications_df (pd.DataFrame): DataFrame containing publication data (e.g., PubMed, Clinical Trials).
                                        Expected to have 'date' column already standardized to 'YYYY-MM-DD'.
        drugs_list_upper (list): A list of tuples, where each tuple is (original_drug_name, uppercase_drug_name).
        source_type (str): A string indicating the source of the publication (e.g., "pubmed", "clinical_trial").

    Returns:
        list: A list of dictionaries, each describing a drug mention.
    """
    mentions = []

    # Determine the correct title column name based on the source type
    title_column = "title"
    if source_type == "clinical_trial":
        title_column = "scientific_title"

    # Check if required columns exist before proceeding
    required_cols = [title_column, "journal", "id", "date"]
    for col in required_cols:
        if col not in publications_df.columns:
            print(
                f"Warning: Column '{col}' not found in {source_type} data. Skipping processing for this source."
            )
            return []  # Return empty list if a critical column is missing

    # Ensure necessary columns exist and handle missing values
    publications_df[title_column] = publications_df[title_column].fillna("")
    publications_df["journal"] = publications_df["journal"].fillna("")
    publications_df["id"] = publications_df["id"].fillna("")

    # Iterate over each row in the publications DataFrame
    for _, row in publications_df.iterrows():
        title = str(row[title_column]).upper()
        journal = str(row["journal"])
        date = row["date"]
        pub_id = str(row["id"])
        original_title = str(row[title_column])

        # Skip processing if the title is empty or the date is None
        if not title or date is None:
            continue

        # Check for each drug if it's mentioned in the current publication's title
        for drug_name_original, drug_name_upper in drugs_list_upper:
            if drug_name_upper in title:
                mentions.append(
                    {
                        "drug": drug_name_original,
                        "journal": journal,
                        "date": date,
                        "source_type": source_type,
                        "publication_id": pub_id,
                        "publication_title": original_title,
                    }
                )
    return mentions
