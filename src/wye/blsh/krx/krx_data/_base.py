import time
import numpy as np
import pandas as pd
from pykrx.website import krx


class Base:
    def set_trd_dd(self, date, nearest):
        self.trd_dd = (
            krx.get_nearest_business_day_in_a_week(date, True) if nearest else date
        )

    def adjust_df(self, df, cols):
        fetched_at = time.strftime("%Y-%m-%d %H:%M:%S")
        keys = list(cols.keys())
        if df is not None and not df.columns.empty:
            df = df[keys].copy()
            num_cols = [key for key in keys if np.issubdtype(cols[key], np.number)]
            int_cols = [key for key in keys if np.issubdtype(cols[key], np.integer)]
            df.loc[:, num_cols] = df.loc[:, num_cols].replace(",", "", regex=True)
            # df.loc[:, num_cols] = df.loc[:, num_cols].replace(r"\-$", "0", regex=True)
            df[int_cols] = (
                df[int_cols].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
            )
            df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
            df = df.astype(cols)
        else:
            df = pd.DataFrame(columns=cols)
        df.columns = df.columns.str.lower()
        df["fetched_at"] = fetched_at
        return df
