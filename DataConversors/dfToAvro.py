#Basate en ese código para crear un conversor de DataFrames de pandas a archivos Avro
import pandas as pd
from fastavro import writer, parse_schema
from io import BytesIO

def generate_avro_schema(df):
    '''
    Generate Avro schema dynamically from Pandas DataFrame structure.
    '''
    fields = []
    for column_name, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            field_type = 'int'
        elif pd.api.types.is_float_dtype(dtype):
            field_type = 'float'
        elif pd.api.types.is_string_dtype(dtype):
            field_type = 'string'
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = 'boolean'
        #For date types
        elif pd.api.types.is_datetime64_dtype(dtype):
            field_type = {'type': 'long', 'logicalType': 'timestamp-millis'}
        else:
            # For other data types, you may need to handle them accordingly
            raise ValueError(f"Unsupported data type for column '{column_name}. Data type: '{dtype}")
        
        fields.append({'name': column_name, 'type': field_type})

    return {'type': 'record', 'name': 'MyDataFrame', 'fields': fields}


def dfToAvro(df, fileName):
    '''
    Convert Pandas DataFrame to Avro file.
    '''
    avro_schema = generate_avro_schema(df)
    avro_records = df.to_dict(orient='records')

    buffer = BytesIO()
    writer(buffer, parse_schema(avro_schema), avro_records)
    buffer.seek(0)

    with open(fileName, 'wb') as f:
        f.write(buffer.read())
        