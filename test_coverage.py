import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, trim, when, lit, concat_ws, sha2, first, udf, coalesce
from pyspark.sql.types import StringType, BooleanType

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from main import create_combined_address, clean_data
import json
from copy import deepcopy
# Test the coverage of the results from output_companies.csv

class TestCoverage(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestCoverage, self).__init__(*args, **kwargs)
        self.spark: SparkSession = SparkSession.builder.appName("TestCoverage").getOrCreate()
        # self.df_init = self.spark.read.parquet("veridion_entity_resolution_challenge.snappy.parquet")
        self.df_init : DataFrame = self.spark.read.csv("input_cleared.csv", header=True)
        self.df_final : DataFrame = self.spark.read.csv("output_companies.csv", header=True)
        
        
        self.df_init = create_combined_address(clean_data(self.df_init))
        self.df_final = create_combined_address(clean_data(self.df_final))
        
    def test_all_entities_are_represented(self):

        # Test that all companies in the input data are represented in the output data
        # We will test that all entities are represented by one relevant column in the output
        relevant_columns = []
        with open("config.json") as f:
            config = json.load(f)
            all_columns_with_duplicates = list(dict(config["pipeline_stages"]).values())
            [relevant_columns.extend(x) for x in all_columns_with_duplicates]
            relevant_columns = set(relevant_columns)
            
        print(relevant_columns)
        
        # Used to count how relevant columns are represented in the input data
        how_much_coverage = {col: 0 for col in relevant_columns}
        total_non_null_columns_elem = {col: 0 for col in relevant_columns}
        
        df_init_copy = (self.df_init).select(*(col(x).alias(x + "_init") for x in self.df_init.columns))
        init_copy_cols = df_init_copy.columns
        # print(init_copy_cols)
        for column in relevant_columns:
            # Filter out the elements that have a corespondent in the initial df for the given column
            df_init_copy = df_init_copy.join(self.df_final, [df_init_copy[column + "_init"].isNotNull(),
                                                              df_init_copy[column + "_init"] == self.df_final[column]],
                                                            "left_anti")
            df_init_copy = df_init_copy.select(*init_copy_cols)
            # print(df_init_copy.select(column + '_init').distinct().collect())
            # Count how many elements are in the initial df for the given column
            local_copy = self.df_init[self.df_init[column].isNotNull()]\
                    .select(*(col(x).alias(x + "_init") for x in self.df_init.columns))
            
            total_non_null_columns_elem[column] = local_copy.filter(col(column + '_init').isNotNull()).count()
            
            column_elements_in_final = [x[column] for x in self.df_final.select(column).distinct().collect()]
            local_copy = local_copy.filter(col(column + '_init').isNotNull())\
                    .filter(col(column + '_init').isin(column_elements_in_final))
            
            how_much_coverage[column] += local_copy.count()

        # We expect that all the elements in the final df are in any form in the initial df
        self.assertEqual(df_init_copy.count(), 0)
        print({col: f"{how_much_coverage[col]} / {total_non_null_columns_elem[col]}" for col in relevant_columns})
        
    def duplicates_on_col(self, col_name: str):
        
        def non_void_string(s: str):
            return s is not None and s != ""
        
        udf_non_void_string = udf(non_void_string, BooleanType())
        df_duplicated_addressed = self.df_final\
            .filter(udf_non_void_string(col(col_name)))\
            .groupBy(col_name).count().filter(col("count") > 1)
            
        list_col_vals = list(map(lambda x: x[col_name], df_duplicated_addressed.select(col_name).collect()))
        df_duplicated_addressed = self.df_final.filter(col(col_name).isin(list_col_vals))\
            .select(*self.df_final.columns)
        return df_duplicated_addressed
    
    def test_duplicates_address(self):
        new_df = self.duplicates_on_col("combined_address")
        print("Result on addresses: ", new_df.count())
        new_df.toPandas().to_csv("duplicates_on_combined_address.csv", index=False)
        
    def test_duplicates_company_names(self):
        new_df = self.duplicates_on_col("company_commercial_names")
        print("Result on commercial_names: ", new_df.count())
        new_df.toPandas().to_csv("duplicates_on_company_commercial_names.csv", index=False)
        
if __name__ == '__main__':
    unittest.main()