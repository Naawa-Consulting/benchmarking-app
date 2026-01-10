from typing import Any

import pyreadstat


def read_sav(path: str) -> tuple[Any, Any]:
    """
    Read an SPSS .sav file and return (dataframe, metadata).

    TODO:
    - Validate schema and required columns
    - Normalize column names
    - Persist raw data to the warehouse
    """
    return pyreadstat.read_sav(path)
