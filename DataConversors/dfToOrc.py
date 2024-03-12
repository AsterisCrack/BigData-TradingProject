import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc

def dfToOrc(df, fileName):
    """
    Convert Pandas DataFrame to ORC file.
    """
    table = pa.Table.from_pandas(df, preserve_index=False)
    """with pa.OSFile(fileName, 'wb') as sink:
        orc.write_table(table, sink)"""
    orc.write_table(table, fileName)