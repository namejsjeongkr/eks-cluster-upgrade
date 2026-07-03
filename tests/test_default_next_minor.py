"""Test the default-target-version helper.

The old default target was str(float(version) + 0.01), which is broken at every
X.9 -> X.10 boundary (e.g. "1.29" -> "1.3", "1.9" -> "1.91"). The replacement
must increment the minor as an integer.
"""

import pytest

from eksupgrade.models.eks import _default_next_minor


@pytest.mark.parametrize(
    "current,expected",
    [
        ("1.34", "1.35"),  # happy path
        ("1.29", "1.30"),  # float bug produced "1.3"
        ("1.9", "1.10"),  # float bug produced "1.91"
        ("1.30", "1.31"),
        ("1.39", "1.40"),  # float bug produced "1.4"
    ],
)
def test_default_next_minor(current, expected):
    assert _default_next_minor(current) == expected
