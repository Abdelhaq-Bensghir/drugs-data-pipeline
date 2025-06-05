import json
import os
from collections import defaultdict

# Variables
current_script_directory = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_script_directory, "..", ".."))
graph_json_path = os.path.join(
    project_root, "data", "output", "drug_journal_mentions_graph.json"
)


def load_graph_data(file_path=graph_json_path):
    """
    Load data from the JSON graph output of the pipeline.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            print(f"Warning: The file '{file_path}' is empty. No data to analyze.")
        return data
    except FileNotFoundError:
        print(
            f"Error: The file '{file_path}' was not found. Please ensure the main pipeline has been run."
        )
        return None
    except json.JSONDecodeError as e:
        print(
            f"Error: Could not decode JSON from '{file_path}'. The file might be corrupted or empty. Details: {e}"
        )
        return None


def find_journal_with_most_different_drugs(graph_data):
    """
    Extracts the name of the journal that mentions the most different drugs.

    Args:
        graph_data (list): The graph data loaded from drug_journal_mentions_graph.json.

    Returns:
        tuple: (Name of the journal, Number of different drugs) or (None, 0) if no data.
    """
    if not graph_data:
        return None, 0

    journal_drugs = defaultdict(set)  # Key: journal, Value: set of unique drugs

    for mention in graph_data:
        journal = mention.get("journal")
        drug = mention.get("drug")
        if journal and drug:
            journal_drugs[journal].add(drug)

    if not journal_drugs:
        return None, 0

    most_mentions_journal = None
    max_different_drugs = 0

    for journal, drugs_set in journal_drugs.items():
        num_different_drugs = len(drugs_set)
        if num_different_drugs > max_different_drugs:
            max_different_drugs = num_different_drugs
            most_mentions_journal = journal

    return most_mentions_journal, max_different_drugs


def find_related_drugs_by_pubmed_journals(graph_data, target_drug):
    """
    For a given drug, finds the set of drugs mentioned by the same journals
    referenced by scientific publications (PubMed) but not clinical trials (Clinical Trials).

    Args:
        graph_data (list): The loaded graph data.
        target_drug (str): The name of the target drug.

    Returns:
        set: A set of related drug names.
    """
    if not graph_data or not target_drug:
        return set()

    target_drug_upper = target_drug.upper()

    # Step 1: Identify all journals that mention each drug and by what source
    drug_journal_sources = defaultdict(
        lambda: defaultdict(set)
    )  # drug -> journal -> {source_type}

    for mention in graph_data:
        drug = mention.get("drug")
        journal = mention.get("journal")
        source_type = mention.get("source_type")
        if drug and journal and source_type:
            drug_journal_sources[drug.upper()][journal].add(source_type)

    # Step 2: Find journals that mentioned the target drug exclusively via PubMed
    target_pubmed_only_journals = set()
    if target_drug_upper in drug_journal_sources:
        for journal, sources in drug_journal_sources[target_drug_upper].items():
            if "pubmed" in sources and "clinical_trial" not in sources:
                target_pubmed_only_journals.add(journal)

    if not target_pubmed_only_journals:
        # print(f"Target drug '{target_drug}' has no exclusive PubMed mentions to establish links.")
        return set()

    # Step 3: Find other drugs mentioned by these same journals (exclusively PubMed)
    related_drugs = set()

    for other_drug_upper, journal_map in drug_journal_sources.items():
        if (
            other_drug_upper == target_drug_upper
        ):  # we don't include the target drug itself
            continue

        # Check if this other drug is mentioned in one of the target drug's "PubMed-only" journals
        # AND if its own mentions in these journals are also exclusively PubMed
        shared_pubmed_only_journals_with_other_drug = set()
        for journal_name in target_pubmed_only_journals:
            if (
                journal_name in journal_map
            ):  # The other drug is mentioned in this journal
                sources_for_other_drug = journal_map[journal_name]
                if (
                    "pubmed" in sources_for_other_drug
                    and "clinical_trial" not in sources_for_other_drug
                ):
                    shared_pubmed_only_journals_with_other_drug.add(journal_name)

        if shared_pubmed_only_journals_with_other_drug:
            # If the other drug shares at least one PubMed-only journal with the target drug,
            # and these mentions are also PubMed-only for the other drug.
            # For now, returning the capitalized version of the uppercase drug name.
            related_drugs.add(other_drug_upper.capitalize())

    return related_drugs


if __name__ == "__main__":

    # Load the graph data
    print(f"Loading graph data from: {graph_json_path}")
    data = load_graph_data()

    if data is not None:

        # First ad-hoc analysis: Find journal with most different drugs
        print("\n--- Analysis 1: Journal with most different drug mentions ---")
        journal, count = find_journal_with_most_different_drugs(data)
        if journal:
            print(
                f"The journal mentioning the most different drugs is: '{journal}' ({count} drugs)."
            )
        else:
            print("No journal mentions found or data was empty.")

        # Second ad-hoc analysis: Find related drugs for a target drug
        print(
            "\n--- Analysis 2: Find related drugs for BETAMETHASONE, having a shared PubMed-only journals ---"
        )

        # For demonstration, we use a target_drug.
        example_target_drug = "BETAMETHASONE"
        print(f"Finding related drugs for: '{example_target_drug}'")

        related = find_related_drugs_by_pubmed_journals(data, example_target_drug)

        if related:
            print(
                f"Drugs related to '{example_target_drug}' via shared PubMed-only journals: {', '.join(sorted(list(related))) or 'None'}"
            )
        else:
            print(
                f"No drugs found related to '{example_target_drug}' under the specified criteria, or target drug not found with PubMed-only mentions."
            )
    else:
        print("Could not load graph data. Ad-hoc analysis will not be performed.")
