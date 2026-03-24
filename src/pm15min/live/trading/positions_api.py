from __future__ import annotations

from collections.abc import Callable

import requests

from .contracts import DataApiConfig, PositionRecord
from .normalize import normalize_position_row


def list_positions_from_data_api(
    data_api_config: DataApiConfig,
    *,
    session_factory: Callable[[], requests.Session] = requests.Session,
) -> list[PositionRecord]:
    if not data_api_config.is_configured:
        raise ValueError("missing_polymarket_user_address")
    url = f"{data_api_config.base_url.rstrip('/')}/positions"
    session = session_factory()
    page_size = 500
    offset = 0
    out: list[PositionRecord] = []
    while True:
        resp = session.get(
            url,
            params={
                "user": str(data_api_config.user_address),
                "limit": page_size,
                "offset": offset,
            },
            timeout=15,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not isinstance(rows, list):
            raise TypeError("positions API returned non-list payload")
        if not rows:
            break
        out.extend(normalize_position_row(row) for row in rows if isinstance(row, dict))
        if len(rows) < page_size:
            break
        offset += page_size
    return out
