from pathlib import Path

import numpy as np
import pandas as pd
from numpy import NaN


class VacancyDF:
    def __init__(self, path: Path, sep: str, encoding: str) -> None:
        self.path = path
        self.sep = sep
        self.encoding = encoding
        self._df = pd.read_csv(self.path, sep=self.sep, encoding=self.encoding)

    @property
    def dataframe(self) -> pd.DataFrame:
        return self._df

    def drop_columns(self, *args, **kwargs) -> None:
        self._df.drop(list(args), **kwargs)

    def replace(self, column: str, *args, **kwargs) -> None:
        self._df[column].replace(*args, **kwargs)

    def __repr__(self) -> str:
        return self._df.to_string()
