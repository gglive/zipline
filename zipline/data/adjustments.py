# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from errno import ENOENT
from os import remove
from os.path import exists

import sqlite3

import bcolz
import pandas as pd
from numpy import (
    floating,
    integer,
    issubdtype,
)

from six import iteritems


SQLITE_ADJUSTMENT_COLUMNS = frozenset(['effective_date', 'ratio', 'sid'])
SQLITE_ADJUSTMENT_COLUMN_DTYPES = {
    'effective_date': integer,
    'ratio': floating,
    'sid': integer,
}
SQLITE_ADJUSTMENT_TABLENAMES = frozenset(['splits', 'dividends', 'mergers'])


SQLITE_DIVIDEND_PAYOUT_COLUMN_DTYPES = {
    'ex_date': integer,
    'declared_date': integer,

}


class SQLiteAdjustmentWriter(object):
    """
    Writer for data to be read by SQLiteAdjustmentWriter

    Parameters
    ----------
    conn_or_path : str or sqlite3.Connection
        A handle to the target sqlite database.
    overwrite : bool, optional, default=False
        If True and conn_or_path is a string, remove any existing files at the
        given path before connecting.

    See Also
    --------
    SQLiteAdjustmentReader
    """

    def __init__(self, conn_or_path, overwrite=False):
        if isinstance(conn_or_path, sqlite3.Connection):
            self.conn = conn_or_path
        elif isinstance(conn_or_path, str):
            if overwrite and exists(conn_or_path):
                try:
                    remove(conn_or_path)
                except OSError as e:
                    if e.errno != ENOENT:
                        raise
            self.conn = sqlite3.connect(conn_or_path)
        else:
            raise TypeError("Unknown connection type %s" % type(conn_or_path))

    def write_frame(self, tablename, frame):
        if frozenset(frame.columns) != SQLITE_ADJUSTMENT_COLUMNS:
            raise ValueError(
                "Unexpected frame columns:\n"
                "Expected Columns: %s\n"
                "Received Columns: %s" % (
                    SQLITE_ADJUSTMENT_COLUMNS,
                    frame.columns.tolist(),
                )
            )
        elif tablename not in SQLITE_ADJUSTMENT_TABLENAMES:
            raise ValueError(
                "Adjustment table %s not in %s" % (
                    tablename, SQLITE_ADJUSTMENT_TABLENAMES
                )
            )

        expected_dtypes = SQLITE_ADJUSTMENT_COLUMN_DTYPES
        actual_dtypes = frame.dtypes
        for colname, expected in iteritems(expected_dtypes):
            actual = actual_dtypes[colname]
            if not issubdtype(actual, expected):
                raise TypeError(
                    "Expected data of type {expected} for column '{colname}', "
                    "but got {actual}.".format(
                        expected=expected,
                        colname=colname,
                        actual=actual,
                    )
                )
        return frame.to_sql(tablename, self.conn)

    def calc_dividend_ratios(self, dividends):

        daily_bar_table = bcolz.ctable(
            rootdir=self.daily_bar_table,
            mode='r')
        trading_days = pd.DatetimeIndex(
            daily_bar_table.attrs['calendar'], tz='UTC'
        )

        # Remove rows with no gross_amount
        dividends_df = dividends_df[pd.notnull(dividends_df.gross_amount)]

        dividends_df['ex_date_dt'] = pd.to_datetime(
            dividends_df['ex_date_nano'], utc=True)

        # strftime("%s") not working here for some reason
        epoch = pd.Timestamp(0, tz='UTC')

        closes = daily_bar_table['close'][:]
        sids = daily_bar_table['id'][:]

        ratios = np.zeros(len(dividends_df))
        effective_dates = np.zeros(len(dividends_df), dtype=np.uint32)

        first_row = {int(k): v for k, v
                     in daily_bar_table.attrs['first_row'].iteritems()}
        calendar_offset = {
            int(k): v for k, v
            in daily_bar_table.attrs['calendar_offset'].iteritems()}

        del dividends_df['declared_date_nano']
        del dividends_df['pay_date_nano']
        del dividends_df['net_amount']
        del dividends_df['ex_date_nano']

        dividends_df = dividends_df.reset_index(drop=True)
        for i, row in dividends_df.iterrows():
            sid = row['sid']
            day_loc = trading_days.searchsorted(row['ex_date_dt'])
            # Find the first non-empty close for the asset **before** the ex date.
            found_close = False
            while True:
                # Always go back at least one day.
                day_loc -= 1
                ix = first_row[sid] + day_loc - calendar_offset[sid]
                prev_close = closes[ix]
                if sids[ix] != sid:
                    break
                elif prev_close != 0:
                    found_close = True
                    break

            if found_close:
                ratios[i] = 1.0 - row['gross_amount'] / (prev_close * 0.001)
                prev_day = trading_days[day_loc]
                prev_day_s = int((prev_day - epoch).total_seconds())
                effective_dates[i] = prev_day_s
            else:
                # All 0 zero data before ex_date
                # This occurs with at least sid 25157
                logger.warn("Couldn't compute ratio for dividend %s" % dict(row))
                continue

        dividends_df['ratio'] = ratios
        dividends_df['effective_date'] = effective_dates
        output_df = dividends_df[dividends_df.effective_date != 0]
        del output_df['gross_amount']
        del output_df['ex_date_dt']

        ratios_path = paths.build.adjustments.dividend_ratios
        if os.path.exists(ratios_path):
            # bcolz checks for path existence before writing.
            os.remove(ratios_path)
        s_table = bcolz.ctable.fromdataframe(output_df)
        s_table.tohdf5(ratios_path, '/dividend_ratios')

        daily_bar_table = bcolz.ctable(rootdir=self.daily_bars_path)

    def write_dividend_data(self, dividends):

        # First write the dividend payouts.


        # Second from the dividend payouts, calculate ratios.

        dividend_ratios = self.calc_dividend_ratios(dividends)


        self.write_frame('dividends', dividend_ratios)

    def write(self, splits, mergers, dividends):
        """
        Writes data to a SQLite file to be read by SQLiteAdjustmentReader.

        Parameters
        ----------
        splits : pandas.DataFrame
            Dataframe containing split data.
        mergers : pandas.DataFrame
            DataFrame containing merger data.
        dividends : pandas.DataFrame
            DataFrame containing dividend data.

        Notes
        -----
        DataFrame input (`splits`, `mergers`, and `dividends`) should all have
        the following columns:

        effective_date : int
            The date, represented as seconds since Unix epoch, on which the
            adjustment should be applied.
        ratio : float
            A value to apply to all data earlier than the effective date.
        sid : int
            The asset id associated with this adjustment.

        The ratio column is interpreted as follows:
        - For all adjustment types, multiply price fields ('open', 'high',
          'low', and 'close') by the ratio.
        - For **splits only**, **divide** volume by the adjustment ratio.

        Dividend ratios should be calculated as
        1.0 - (dividend_value / "close on day prior to dividend ex_date").

        Returns
        -------
        None

        See Also
        --------
        SQLiteAdjustmentReader : Consumer for the data written by this class
        """
        self.write_frame('splits', splits)
        self.write_frame('mergers', mergers)
        self.write_dividend_data(dividends)
        self.conn.execute(
            "CREATE INDEX splits_sids "
            "ON splits(sid)"
        )
        self.conn.execute(
            "CREATE INDEX splits_effective_date "
            "ON splits(effective_date)"
        )
        self.conn.execute(
            "CREATE INDEX mergers_sids "
            "ON mergers(sid)"
        )
        self.conn.execute(
            "CREATE INDEX mergers_effective_date "
            "ON mergers(effective_date)"
        )
        self.conn.execute(
            "CREATE INDEX dividends_sid "
            "ON dividends(sid)"
        )
        self.conn.execute(
            "CREATE INDEX dividends_effective_date "
            "ON dividends(effective_date)"
        )

    def close(self):
        self.conn.close()
