from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, trim, when, lit, \
        concat_ws, sha2, first, udf, coalesce, regexp_replace
from pyspark.sql.types import StringType
import json
from uuid import uuid4

__DEBUG_FLAG = True

def load_config(config_file: str) -> dict:
    with open(config_file) as f:
        config = json.load(f)
        
    assert 'pipeline_stages' in config, "pipeline_stages not found in config file."
    return config

def read_snappy_parquet(file_path: str) -> DataFrame:
    """
    Reads a Snappy Parquet file using Spark and returns a DataFrame.

    Args:
        file_path (str): The path to the Snappy Parquet file.

    Returns:
        DataFrame: The Spark DataFrame.
    """
    spark = SparkSession.builder.appName("MainApp").getOrCreate()
    try:
        df = spark.read.parquet(file_path)
        return df
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return None  # Or raise the exception, depending on your error handling policy

def clean_data(df: DataFrame) -> DataFrame:
    """
    Cleans the input DataFrame by standardizing company names and country codes,
    and handling missing values.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The cleaned DataFrame.
    """
    # Standardize company names (lowercase, trim & delete commas and points)
    df = df.withColumn("cleaned_company_name", 
                       regexp_replace(trim(lower(col("company_name"))), "[^a-zA-Z0-9\\s]", ""))

    # Standardize country codes (uppercase and trim)
    df = df.withColumn("cleaned_country_code", trim(col("main_country_code")))

    # Delete all new lines from address
    
    to_be_cleared_columns = ['main_street', 'locations',
                             'long_description', 'company_legal_names',
                             'short_description', 'main_address_raw_text']
    for column in to_be_cleared_columns:
        df = df.withColumn(column, when(col(column).contains("\n"), lit("")).otherwise(col(column)))
    
    # df.sort('company_name').toPandas().to_csv("input_cleared.csv")
    return df

def create_combined_address(df: DataFrame) -> DataFrame:
    """
    Creates a combined address field from street, city, and country.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with the combined address column.
    """
    df = df.withColumn(
        "combined_address",
        concat_ws(", ", lower(trim(col("main_street"))), lower(trim(col("main_city"))), lower(trim(col("main_country"))))
    )
    return df

def generate_entity_id(df: DataFrame, columns: list, id: str = "") -> DataFrame:
    """
    Generates a unique entity ID based on the specified columns.
    If a column value is null, it uses uuid4 to generate a unique value for that column,
    then encodes with sha2. In this way, the entity ID is unique for each entity.

    Args:
        df (DataFrame): The input DataFrame.
        columns (list): A list of column names to use for generating the entity ID.

    Returns:
        DataFrame: The DataFrame with the entity ID column.
    """

    def generate_uuid_udf():
        return str(uuid4())

    generate_uuid = udf(generate_uuid_udf, StringType()).asNondeterministic()

    # Create a list of columns, replacing nulls with UUIDs
    modified_cols = [
        coalesce(col(c), generate_uuid()).alias(c) for c in columns
    ]

    # Concatenate the modified columns
    concatenated_string = concat_ws("", *modified_cols)

    # Generate the SHA-256 hash
    df = df.withColumn(f"entity_id_{id}", sha2(concatenated_string, 256))
    return df

def group_similar_enitites(df: DataFrame, id: str = "") -> DataFrame:
    """
    Groups similar entities based on the entity ID.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with the entity ID column.
    """
    a_df = df.groupBy(f"entity_id_{id}").count()
    # a_df.show(10)
    return a_df

# Debug function
def print_grouped_data(df: DataFrame, grouped_df: DataFrame, id: str = "") -> None:
    grouped_df = grouped_df.filter(col("count") > 1)
    
    entities = list(map(lambda x: x[f"entity_id_{id}"], 
                        grouped_df.select(f"entity_id_{id}").collect()))
    
    a = df.filter(col(f"entity_id_{id}").isin(entities))
    a.sort(f"entity_id_{id}").toPandas().to_csv("output.csv")


def group_unique_data(df: DataFrame, id = "") -> DataFrame:
    """
    Groups unique entities based on the entity ID.
    If the entity ID is the same, it takes the first value of the column.
    TODO: Heuristic to be improved and choose the most descriptive values.
    
    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The output DataFrame with merged data.
    """
    all_columns = df.columns.copy()
    all_columns.remove(f"entity_id_{id}")
    df = df.groupBy(f"entity_id_{id}").agg(
        *[first(x, ignorenulls=True).alias(x) for x in all_columns]
    )
    return df


# Debug Function
def search_all_outputs_with_name(df: DataFrame, name: str) -> DataFrame:
    """
    Searches all inputs with the specified company name.

    Args:
        df (DataFrame): The input DataFrame.
        name (str): The name to search for.

    Returns:
        DataFrame: The DataFrame with the search results.
    """
    return df.filter(col("company_name") == (name))


def pipeline(df: DataFrame, config: dict) -> DataFrame:
    """
    Runs the data processing pipeline on the input DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        config (dict): The configuration dictionary.

    Returns:
        DataFrame: The output DataFrame.
    """
    # Clean the data
    df = clean_data(df)

    # Create combined address
    df = create_combined_address(df)

    # Generate entity IDs
    for pipeline_stage_id, stage in config['pipeline_stages'].items():
        df = generate_entity_id(df, stage, pipeline_stage_id)

        # DEBUG: See how many entities are resulted
        if __DEBUG_FLAG:
            grouped_df = group_similar_enitites(df, pipeline_stage_id)
            print("Created ", grouped_df.count(), " groups of duplicates out of ", df.count(), " records.")

        # Group unique data
        df = group_unique_data(df, pipeline_stage_id)

    return df

def clear_pipeline_intermediates(df: DataFrame, config: dict):
    config_fields = ['cleaned_company_name', 
                     'cleaned_country_code', 
                     'combined_address']
    config_fields.extend(
        [f'entity_id_{id}' for id in config['pipeline_stages'].keys()])
    print(config_fields)
    for col_name in config_fields:
        df = df.drop(col_name)
    return df

if __name__ == "__main__":
    file_path = "veridion_entity_resolution_challenge.snappy.parquet"
    
    # Initialize SparkSession here
    spark = SparkSession.builder.appName("MainApp").getOrCreate()
    
    df = read_snappy_parquet(file_path)
    
    if df is not None:
        config = load_config("config.json")

        df = pipeline(df, config)
        
        # Clear intermediate columns
        df = clear_pipeline_intermediates(df, config)
        
        df = df.sort('company_name')
        df.toPandas().to_csv("output_companies.csv")
        # Write to snappy parquet
        df.write.parquet("output_companies.snappy.parquet", mode="overwrite", compression="snappy")

    else:
        print("Error reading Parquet file.")
    spark.stop()