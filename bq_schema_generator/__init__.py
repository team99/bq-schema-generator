
from typing import List, Dict
from bq_schema_generator.schema_generator import SchemaGenerator


def batch_to_bq_schema(batch: List[List[Dict]]) -> List[Dict]:
    """ Takes a batch of records retrieved from an API call and converts it to a list of dictionaries which can be
    consumed by the Big Query API as a schema"""
    schema_generator = SchemaGenerator()
    schema_generator.update_schema_columns(batch)

    return schema_generator.get_bq_schema()
