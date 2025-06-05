import re


def is_atccode_multi_level(code_string):
    """
    Checks if a given string is a valid ATC code based on its structure
    at any hierarchical level (1st to 5th). based on WHO data https://atcddd.fhi.no/atc/structure_and_principles/

    Valid ATC code patterns:
    - 1st Level: L (e.g., A)
    - 2nd Level: LNN (e.g., A10)
    - 3rd Level: LNNL (e.g., A10B)
    - 4th Level: LNNLL (e.g., A10BA)
    - 5th Level: LNNLLNN (e.g., A10BA02)

    Args:
        code_string (str): The string to validate.

    Returns:
        bool: True if the string is a valid ATC code at any level,
              False otherwise.
    """
    if not isinstance(code_string, str):
        return False  # Input must be a string

    # Combined pattern using OR (|)
    # This ensures the entire string matches one of the patterns exactly.
    # ^ : start of string
    # [A-Z] : an uppercase letter
    # \d{2} : exactly two digits
    # $ : end of string
    combined_pattern = r"^([A-Z]|[A-Z]\d{2}|[A-Z]\d{2}[A-Z]|[A-Z]\d{2}[A-Z]{2}|[A-Z]\d{2}[A-Z]{2}\d{2})$"

    if re.fullmatch(combined_pattern, code_string):
        return True
    else:
        return False


def is_nct_number(trial_id_string):
    """
    Checks if a given string is a valid NCT (ClinicalTrials.gov) number.
    A valid NCT number starts with "NCT" (uppercase) followed by 8 digits.
    Example: NCT12345678

    Args:
        trial_id_string (str): The string to validate.

    Returns:
        bool: True if the string is a valid NCT number, False otherwise.
    """
    if not isinstance(trial_id_string, str):
        return False  # Input must be a string

    # Pattern: Starts with "NCT" (uppercase), followed by exactly 8 digits.
    nct_pattern = r"^NCT\d{8}$"

    if re.fullmatch(nct_pattern, trial_id_string):
        return True
    else:
        return False


def clean_skipped_hex_sequences(field):
    """
    Removes ALL patterns like '\\xYY' (a literal backslash, 'x', and two hexadecimal digits 'Y')
    from a given string. The goal is to simply eliminate these sequences.
    A better option would be to decode them, but for time concerns we will do simple.

    Args:
        field: The input string from which to remove the patterns.

    Returns:
        The cleaned string with all '\\xYY' patterns removed.
    """

    #   '\\'   : Matches a literal backslash.
    #   'x'    : Matches the literal character 'x'.
    #   '[0-9a-fA-F]{2}' : Matches any two hexadecimal digits.
    pattern = r"\\x[0-9a-fA-F]{2}"

    # replace all occurrences of the defined pattern with an empty string.
    cleaned_field = re.sub(pattern, "", field)
    return cleaned_field
