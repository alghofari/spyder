import pandas as pd


def datatype_casting(table_schema, dataframe):
    for schema in table_schema:
        field_name = schema['name']
        field_type = schema['type']
        if field_type == "TIMESTAMP":
            dataframe[field_name] = pd.to_datetime(dataframe[field_name])
        elif field_type == "DATE":
            dataframe[field_name] = pd.to_datetime(dataframe[field_name]).dt.date
        elif field_type == "FLOAT":
            dataframe[field_name] = pd.to_numeric(dataframe[field_name])
        elif field_type == "INTEGER":
            dataframe[field_name] = dataframe[field_name].astype("Int64")
        elif field_type == "BOOLEAN":
            dataframe[field_name] = dataframe[field_name].astype("bool")
        elif field_type == "STRING":
            dataframe[field_name] = dataframe[field_name].astype(str)

    return dataframe
