"""Churn label construction.

The precise churn definition (lookback/observation windows, what counts as a lapse, etc.)
is TBD and will be provided later. Keep the definition here so it is single-sourced.
"""

from __future__ import annotations

from ed_subs.config import Config


def build_churn_label(config: Config):
    """Construct the churn label for modeling.

    TODO: implement once the churn definition is finalized.
    """
    raise NotImplementedError("Churn definition TBD — to be provided later.")
