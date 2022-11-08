# Introduction
While Google BigQuery's `load` function provides an schema auto-detection ability, it only takes into account a small subset of an entire dataset.([Read more](https://cloud.google.com/bigquery/docs/schema-detect)) As a result, the schema that they generated could sometimes fail, especially when the data which they derived the schema from is not representative of the nature of the rest of the dataset. e.g. If a column has mainly **integer**, and only few instances of **float**, the auto-detection function might only picked out the integers at random, generating a column type of **integer** for said column. When you attempt to load your raw data into the table created based on this schema, error ensues. This library is developed to solve this problem. It takes into account every single record and generate a schema that is able to handle all values in the column. For this case, the library will generate a column with type **float** which can handle both **integer** and **float**.

# Requirements
Python 3.9 or higher. Older versions of Python might still work, but please adjust the dependencies accordingly.

# How to Use
_Note: The library currently only handles Python dictionaries as input. Do convert the raw files (JSON or CSV) into Python dictionaries first._

## Using SchemaGenerator
Use SchemaGenerator if you could not have all your data in one list and prefer to iterate them over a number of batches.

```
from bq_schema_generator.schema_generator import SchemaGenerator

# Batches of records
batches = [
    [{...}, {...}, {...}], # batch 1
    [{...}, {...}, {...}], # batch 2
    [{...}, {...}, {...}], # batch 3
    [{...}, {...}, {...}], # batch N
]

# Initialize a SchemaGenerator object
schema_generator = SchemaGenerator()

# Iterate over the records over a number of batches
for batch in batches:
    for record in batch:
        schema_generator.update_schema_columns(record)

# Get generated schema
bq_schema = schema_generator.get_bq_schema()

```

## Using batch_to_bq_schema
Use `batch_to_bq_schema` if you only have one list of python dictionaries.

```
from bq_schema_generator import batch_to_bq_schema

# One batch of records
batch = [{...}, {...}, {...}]

# Get generated schema
bq_schema = batch_to_bq_schema(batch)
```
