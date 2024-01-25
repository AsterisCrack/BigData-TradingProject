import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def dfToParquet(df, fileName):
    """
    Convert Pandas DataFrame to Parquet file.
    """
    table = pa.Table.from_pandas(df)
    pq.write_table(table, fileName)