import json
import pandas as pd

def transform_dtype(schema_path, dataframe, gcs_logger):
    with open(schema_path) as schema_file:
        schema_json = json.load(schema_file)

    if not dataframe.empty:
        try:
            for key in schema_json:
                name = key["name"]
                dtype = key["type"]

                # --- For time and date purposes
                if dtype == "TIME" or dtype == "DATE":
                    dataframe[name] = dataframe[name].astype("str")
                    dataframe[name] = dataframe[name].apply(
                        lambda x: pd.to_datetime(x, errors="coerce", utc=True))

                    if dtype == "DATE":
                        dataframe[name] = dataframe[name].dt.date
                        dataframe[name] = dataframe[name].replace({pd.NaT: None})
                        
                    elif dtype == "TIME":
                        dataframe[name] = dataframe[name].dt.time
                
                elif dtype == "TIMESTAMP":
                    dataframe[name] = dataframe[name].astype("str")
                    dataframe[name] = dataframe[name].apply(
                        lambda x: pd.to_datetime(x, errors="coerce", utc=True))

                elif dtype == "INTEGER":
                    dataframe[name] = dataframe[name].astype("Int64")
                elif dtype == "FLOAT":
                    dataframe[name] = dataframe[name].astype("float")
                elif dtype == "STRING":
                    dataframe[name] = dataframe[name].apply(
                        lambda x: str(x) if x is not None and x == x else x)
                elif dtype == "BOOLEAN":
                    dataframe[name] = dataframe[name].astype("boolean")
                else:
                    gcs_logger.log(
                        f"There is column with no mapping, column name: {name} with {dtype} type, check the data")
        except:
            raise ValueError(f"There is an error when tranforming data into specific type")
    
        return dataframe

def check_add_missing_column(dataframe, columns_to_check, list_original_cols, gcs_logger):
    # Check the missing columns
    checking_exist = all(x in list_original_cols for x in columns_to_check)

    if checking_exist == False:
        for col in columns_to_check:
            gcs_logger.log(
                f"This {col}, it's not existed, will be add the column into None Value")
            dataframe[col] = None # Add column if not exists

    return dataframe