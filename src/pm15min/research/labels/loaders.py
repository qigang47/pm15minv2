from __future__ import annotations

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.queries.loaders import load_oracle_prices_table, load_truth_table


def load_label_inputs(cfg: DataConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    return load_truth_table(cfg), load_oracle_prices_table(cfg)
