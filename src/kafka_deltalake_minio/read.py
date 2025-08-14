from deltalake import DeltaTable

import polars as pl
from kafka_deltalake_minio.settings import DELTA_TABLE, STORAGE_OPTIONS


def read():
    try:
        dt = DeltaTable(DELTA_TABLE, storage_options=STORAGE_OPTIONS)
        print(f"Version: {dt.version()}")
        print(f"Files: {dt.file_uris()}")
        print(f"History: {dt.history()}")
        df = pl.read_delta(source=DELTA_TABLE, storage_options=STORAGE_OPTIONS)
        # df = pl.scan_delta(source=DELTA_TABLE, storage_options=STORAGE_OPTIONS)
        print(df)
        for row in df.rows(named=True):
            print(row)
    except Exception as _:
        print(f'{DELTA_TABLE} not found...')

# read()
# write()
read()