# Entity Resolution Task 
## Horia Mercan

In this Markdown file I will explain the solution for the Entity Resolution Task.

By analysing the initial dataset, we can outline some key attributes that any entity has and can be used for distinguish or merge two entities, as
1. **company_name**
2. **main_country_code**
3. **main_street**
4. **main_city**
5. **main_country**
6. **website_domain, facebook_url, instagram_url,...**

Two entities that share the same values among the attributes aforementioned are extremely likely to be the same. However, this might not be the case for most of the entities because they might share only few of the attributes. 

That being considered, I propose a solution with multiple stages of merging among the entities. I created a pipeline that merges entities based on different combinations of key attributes that should be considered. All the combinations are described in the **config.json**  file. Any pipeline stage is described there as:
```json
    stage_id : [
        ... list of common attributes that ensure equivalency of two different entities ...
    ]
```

**In essence, the solution works by:**

1. **Preparing the data:** Cleaning and standardizing key attributes.
2. **Generating identifiers:** Creating unique entity IDs based on combinations of relevant attributes, using UUIDs to handle missing values and hashing for consistency.
3. **Iterative refinement:** Applying multiple stages of entity ID generation, potentially using different sets of attributes in each stage, to improve the accuracy of matching.
4. **Consolidation:** Grouping records with the same entity ID and merging their data, taking the first non-null value for each attribute.
5. **Outputting the result:** Saving the final, resolved dataset to a CSV file.


## Solution technical details

I used the Spark framework in Python for analysing the data. Some key points for this decision were:
* It's fast and can be easily used with .snappy.parquet and .csv formats
* Spark DataFrames can be used with user defined functions
* Can easily group data

Initially, I make sure that all the data are cleaned (remove redundant spaces from attributes, transform everything to lower case and delete possible typos of commas, points, etc) in the function **clean_data(DataFrame) -> Dataframe**. Also, I want to create new fields in my dataset, such as **cleaned_company_name**, **cleaned_country_code** and **combined_address**. Those will be used to detect similar entities. 

Then, for each step in the pipeline, I take certain columns, concatenate them and create sha2 encodings. By doing this, I assure uniqueness of the encoding, 2 entities with same attribute values will have the same encoding. Also, I was thinking of using this for the ability to calculate it twice and be able to scale it if I would get online entities for this pipeline. 

I group data with same sha2 encoding and make the assumption that they are the same. This would ensure me correctness (by the attributes that I choose in the config.json file). I respect as much as possible of all the data available when merging (if an entity has non-null website_domain and another has non-null linkedin_url, in the end the resulting entity would have both of them => creation of better data).