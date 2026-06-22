"""Churn / subscribe prediction model.

Trains on the gold feature table + churn label and logs runs to MLflow. Model family and
features are TBD; this is a scaffold to be filled in during Phase 3.
"""

from __future__ import annotations

from ed_subs.config import Config


def train(config: Config):
    """Train the churn/subscribe model and log to MLflow.

    TODO: implement once the gold feature table and churn label are available.
    """
    raise NotImplementedError("Model structure TBD — to be provided later.")
