
from typing import List, Dict, Union
from .utils import get_dict_value
import logging

type_ = {
    dict: "RECORD",
    bool: "BOOLEAN",
    float: "FLOAT",
    int: "FLOAT",
    str: "STRING",
    list: None,
    type(None): "NULL"
}

# left side is loose, right side is precise/correct
# i.e. right side can overwrite left side
type_hierarchy = {
    "BOOLEAN": ["STRING", "RECORD"],
    "FLOAT": ["STRING", "RECORD"],
    "STRING": ["RECORD"],
    "RECORD": []
}


class SchemaGenerator(object):
    """
    Improved version that handles nested lists of structs.
    Also, internally uses a precision based hierarchy to determine
    which datatype to use (instead of using the last known value).
    As with previous version, omits empty lists or structs.
    Legacy issue: all numbers are cast as float.
    """

    def __init__(self,
                 default_column_types: Dict = None):
        self.schema_columns_dict = {}
        self.bq_consumable_schema = []
        self.default_column_types = default_column_types if default_column_types is not None else {}

    def _get_type(self, x: Union[dict, bool, float, int, str, list, type(None)]) -> Union[str, None]:
        if isinstance(x, list):
            if len(x) > 0:
                return type_[type(x[0])]
            else:
                return None
        else:
            return type_[type(x)]

    def _get_mode(self, x: Union[dict, bool, float, int, str, list, type(None)]) -> Union[str, None]:
        if isinstance(x, list):
            if len(x) > 0:
                return "REPEATED"
            else:
                return None
        else:
            return "NULLABLE"

    def _get_mode_type(self, x: Union[dict, bool, float, int, str, list, type(None)]) -> Dict:
        return {"mode": self._get_mode(x), "type": self._get_type(x)}

    def _get_element_schema(self, elem_key: str, elem_value: Union[dict, bool, float, int, str, list, type(None)],
                            parent_elem_name: str) -> dict:
        """recursively updates a dictionary by checking an element's value for its mode and type,
        as well as updating the element's key name by appending it to its parents' with a separator'
        """
        # unknown type, ignore field
        if elem_value is None or elem_value == [] or elem_value == {}:
            return

        # generate the element's name by joining it to its parents', separated by '.' character
        elem_name = parent_elem_name+'.'+elem_key if parent_elem_name != '' else elem_key

        # generate the element_schema dict by running get_mode_type on the elem_value,
        # and adding the elem_key as 'NAME'
        element_schema = self._get_mode_type(elem_value)
        element_schema["name"] = elem_key

        # if is dict/list, do recursion
        if element_schema["type"] == "RECORD":
            if element_schema["mode"] == "NULLABLE":  # is dict
                for element_key, element_value in elem_value.items():
                    self._get_element_schema(element_key, element_value, elem_name)
            else:   # is list
                for element in elem_value:
                    # whether its primitive or record, just recurse as the field will be overwritten in the schema
                    for e_k, e_v in element.items():
                        self._get_element_schema(e_k, e_v, elem_name)

        # update the schema_columns_dict with the element_schema dict and return it
        # accumulator style hence objs with missing keys do not matter
        # update the schema_columns_dict with the element_schema dict and return it
        self._update_schema_columns_dict(elem_name, element_schema)

    def _update_schema_columns_dict(self, elem_name: str, element_schema: dict):
        """ Precision based datatype hiearchy
        Determines whether to use the incoming record's schema or re-use current one
        Looser types cannot be replaced by more precise types
        e.g. null/none > string > float > int
        """

        incoming_type = element_schema["type"]
        curr_type = get_dict_value(self.schema_columns_dict, elem_name+".type").value
        if incoming_type in type_hierarchy:
            if get_dict_value(self.schema_columns_dict, elem_name+".type").value is None:
                self.schema_columns_dict[elem_name] = element_schema
            elif incoming_type in type_hierarchy[curr_type]:
                self.schema_columns_dict[elem_name] = element_schema
            else:
                pass  # do nth
        else:
            raise Exception(f"Unknown datatype: {incoming_type}, for column: {elem_name}, \
                found when parsing schema of records... terminating...")

    def _get_record_schema(self, record: List[Dict]) -> None:
        """Iterates through a list of dictionaries, checking each key-value pair for the value type, and updating
        self.schema_columns_dict with the relevant information"""
        for key, value in record.items():
            self._get_element_schema(key, value, '')

    def _construct_nesting_dict(self, schema_columns_dict: dict) -> dict:
        """Iterates through a schema dictionary checking how many levels each schema column is nested (based on the
        number of separators in the schema column name, eg 'k_1.kk_2' = 2)
        Stores this information in a dictionary (nesting_dict) with 'key' = no. of levels, and 'value' = a list of
        schema column names with the same level of nesting
        Example return value: {1: ['col_1', 'col_2'], 2: ['col_1.col_3', 'col_1.col_4'], 3: ['col_1.col_3.col_5']}
        """
        nesting_dict = {}

        if len(schema_columns_dict) == 0:
            logging.info("empty schema_columns_dict, returning empty dict")
            return nesting_dict

        for name in schema_columns_dict:
            elems = name.split(".")
            if len(elems) in nesting_dict:
                nesting_dict[len(elems)].append(name)
            else:
                nesting_dict[len(elems)] = [name]

        return nesting_dict

    def _construct_bq_schema(self, schema_columns_dict: dict) -> None:
        """Takes a schema_columns_dict object and constructs a list of dictionaries which can be consumed
        by the Big Query API as a schema
        """
        nesting_dict = self._construct_nesting_dict(schema_columns_dict)
        if len(nesting_dict) == 0:
            logging.info("empty nesting dict, returning function")
            return
        # Iterate over the nesting levels backwards, from the most nested level to level 2
        for nest_level in range(max(nesting_dict), 1, -1):
            # iterate over the schema keys of that nesting level
            for schema_key in nesting_dict[nest_level]:
                # A column of type "RECORD" must have fields, ie any empty dictionaries are removed
                if schema_columns_dict[schema_key]["type"] == "RECORD":
                    if "fields" not in schema_columns_dict[schema_key]:
                        schema_columns_dict.pop(schema_key)
                        continue
                parent = ".".join(schema_key.split(".")[:-1])
                # Update the parent column's fields with the present column
                if "fields" in schema_columns_dict[parent]:
                    schema_columns_dict[parent]["fields"].append(schema_columns_dict[schema_key])
                else:
                    schema_columns_dict[parent]["fields"] = [(schema_columns_dict[schema_key])]
                # Pop the present column from the dictionary
                schema_columns_dict.pop(schema_key)

        # Finally, we are only left with the level 1 columns,
        # that have been updated with its nested columns (in "fields"), if any.
        # Append these to the final_schema list
        for col_name, schema_column in schema_columns_dict.items():
            # check if 'schema_column' is a "RECORD" type without "fields", skip if so
            if schema_column["type"] == "RECORD":
                if "fields" not in schema_column:
                    continue
            # check if a default type has been specified for this
            if col_name in self.default_column_types:
                schema_column["type"] = self.default_column_types[col_name]

            self.bq_consumable_schema.append(schema_column)

    def update_schema_columns(self, batch: List[List[Dict]]) -> None:
        """Iterates through the records of a batch and updates self.schema_columns_dict with relevant information
        required to construct a Big Query schema"""
        for record in batch:
            self._get_record_schema(record)

    def get_bq_schema(self):
        self._construct_bq_schema(self.schema_columns_dict)
        return self.bq_consumable_schema
