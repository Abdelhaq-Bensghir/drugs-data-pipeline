import pytest
from src.utils.utils import is_atccode_multi_level


# Valid test cases for each level
@pytest.mark.parametrize(
    "valid_code, level_description",
    [
        ("A", "1st level"),
        ("R", "1st level"),
        ("A10", "2nd level"),
        ("N02", "2nd level"),
        ("A10B", "3rd level"),
        ("C09A", "3rd level"),
        ("A04AD", "4th level"),
        ("A10BA", "4th level"),
        ("S01ED", "4th level"),
        ("A10BA02", "5th level"),
        ("N02BE51", "5th level"),
    ],
)
def test_valid_atc_codes(valid_code, level_description):
    """Tests valid ATC codes at different levels."""
    assert (
        is_atccode_multi_level(valid_code) is True
    ), f"Failed for valid code ({level_description}): {valid_code}"


# Invalid test cases
@pytest.mark.parametrize(
    "invalid_code, reason",
    [
        ("A10BA02X", "Too long"),
        ("A10BA2", "6 characters, does not match any pattern"),
        ("a10ba02", "Lowercase not accepted by current pattern"),
        ("6302001", "Starts with a digit"),
        ("A1B", "Invalid LNL pattern"),
        ("A10BCDE", "Too many characters/wrong format"),
        ("", "Empty string"),
        (" ", "Space only"),
        ("A0", "Incomplete LNN"),
        ("A00A0", "General wrong format"),
        ("A!", "Special character"),
    ],
)
def test_invalid_atc_codes(invalid_code, reason):
    """Tests invalid ATC codes."""
    assert (
        is_atccode_multi_level(invalid_code) is False
    ), f"Failed for invalid code ({reason}): {invalid_code}"


def test_non_string_input():
    """Tests inputs that are not strings."""
    assert is_atccode_multi_level(None) is False, "Failed for None input"
    assert is_atccode_multi_level(12345) is False, "Failed for numeric input"
    assert is_atccode_multi_level([]) is False, "Failed for list input"
    assert is_atccode_multi_level({}) is False, "Failed for dictionary input"
