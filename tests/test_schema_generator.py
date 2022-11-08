import pytest
import json
from bq_schema_generator.schema_generator import SchemaGenerator
from bq_schema_generator import batch_to_bq_schema


schema_generator = SchemaGenerator()

@pytest.mark.parametrize("value, expected_result", [("s_1", "STRING"),
                                                    ("345", "STRING")])
def test_get_type_string(value, expected_result):
    assert schema_generator._get_type(value) == expected_result

@pytest.mark.parametrize("value, expected_result", [(True, "BOOLEAN"),
                                                    (False, "BOOLEAN")])
def test_get_type_boolean(value, expected_result):
    assert schema_generator._get_type(value) == expected_result

@pytest.mark.parametrize("value, expected_result", [(3.1234, "FLOAT"),
                                                    (-0.999, "FLOAT"),
                                                    (1.0, "FLOAT")])
def test_get_type_float(value, expected_result):
    assert schema_generator._get_type(value) == expected_result

@pytest.mark.parametrize("value, expected_result", [(3, "FLOAT"),
                                                    (-1, "FLOAT"),
                                                    (0, "FLOAT"), ])
def test_get_type_integer(value, expected_result):
    assert schema_generator._get_type(value) == expected_result

@pytest.mark.parametrize("value, expected_result", [({}, "RECORD"),
                                                    ({"x_1": 123}, "RECORD"),
                                                    ({"x_2": "y_1", "x_3": "y_2"}, "RECORD")])
def test_get_type_record(value, expected_result):
    assert schema_generator._get_type(value) == expected_result

@pytest.mark.parametrize("value, expected_result", [([], None),
                                                    ([1, 2, 3], "FLOAT"),
                                                    (["a", 1, 2], "STRING")])
def test_get_type_list(value, expected_result):
    assert schema_generator._get_type(value) == expected_result

@pytest.mark.parametrize("value, expected_result", [(123, "NULLABLE"), ('abc', "NULLABLE"), (None, "NULLABLE"),
                                                    ([], None),
                                                    ([[]], "REPEATED"), ([1, 2, 3], "REPEATED"), ([1.43, 0, None, True],
                                                        "REPEATED")])
def test_get_mode(value, expected_result):
    assert schema_generator._get_mode(value) == expected_result

@pytest.mark.parametrize("value, expected_result", [("t_1", {"mode": "NULLABLE", "type": "STRING"}),
                                                    (-0.000, {"mode": "NULLABLE", "type": "FLOAT"}),
                                                    (True, {"mode": "NULLABLE", "type": "BOOLEAN"}),
                                                    ([1, 2, 3], {"mode": "REPEATED", "type": "FLOAT"}),
                                                    ([{"x_1": "y_1", "x_2": "y_2"}, {"x_3": "y_3", "x_4": "y_4"}], {"mode": "REPEATED", "type": "RECORD"}),
                                                    ({"x_2": "y_1", "x_3": "y_2"}, {"mode": "NULLABLE", "type": "RECORD"}),
                                                    ({}, {"mode": "NULLABLE", "type": "RECORD"}),
                                                    ([], {"mode": None, "type": None}), ])
def test_get_mode_type(value, expected_result):
    assert schema_generator._get_mode_type(value) == expected_result

"""
TEST get_element_schema
"""

value_schema_dict_1 = {}
value_elem_key_1 = 'k_1'
value_elem_value_1 = 1000
value_parents_1 = ''
expected_r_element_schema_1 = {"k_1": {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'}}

value_schema_dict_2 = {'p_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'p_1'}}
value_elem_key_2 = 'k_1'
value_elem_value_2 = True
value_parents_2 = 'p_1'
expected_r_element_schema_2 = {'p_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'p_1'},
                               'p_1.k_1': {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'k_1'}}

value_schema_dict_3 = {}
value_elem_key_3 = 'k_1'
value_elem_value_3 = {'kk_1': 'TEST', 'kk_2': 1000, 'kk_3': [1, 2, 3, 4], 'kk_4': [{'kkk_1': True}, {'kkk_2': False}]}
value_parents_3 = ''
expected_r_element_schema_3 = {'k_1': {'mode': 'NULLABLE', 'type': 'RECORD', 'name': 'k_1'},
                               'k_1.kk_1': {'mode': 'NULLABLE', 'type': 'STRING', 'name': 'kk_1'},
                               'k_1.kk_2': {'mode': 'NULLABLE', 'type': 'FLOAT', 'name': 'kk_2'},
                               'k_1.kk_3': {'mode': 'REPEATED', 'type': 'FLOAT', 'name': 'kk_3'},
                               'k_1.kk_4': {'mode': 'REPEATED', 'type': 'RECORD', 'name': 'kk_4'},
                               'k_1.kk_4.kkk_1': {'mode': 'NULLABLE', 'type': 'BOOLEAN', 'name': 'kkk_1'},
                               'k_1.kk_4.kkk_2': {'mode': 'NULLABLE', 'type': 'BOOLEAN', 'name': 'kkk_2'}
                               }

value_schema_dict_4 = {}
value_elem_key_4 = 'k_1'
value_elem_value_4 = {'kk_1': [{'kkk_1': True},
                               {'kkk_2': False},
                               {'kkk_3': [
                                   {'kkkk_1': 10},
                                   {'kkkk_2': 5}
                                ]}
                               ]}
value_parents_4 = ''
expected_r_element_schema_4 = {'k_1': {'mode': 'NULLABLE', 'type': 'RECORD', 'name': 'k_1'},
                               'k_1.kk_1': {'mode': 'REPEATED', 'type': 'RECORD', 'name': 'kk_1'},
                               'k_1.kk_1.kkk_1': {'mode': 'NULLABLE', 'type': 'BOOLEAN', 'name': 'kkk_1'},
                               'k_1.kk_1.kkk_2': {'mode': 'NULLABLE', 'type': 'BOOLEAN', 'name': 'kkk_2'},
                               'k_1.kk_1.kkk_3': {'mode': 'REPEATED', 'type': 'RECORD', 'name': 'kkk_3'},
                               'k_1.kk_1.kkk_3.kkkk_1': {'mode': 'NULLABLE', 'type': 'FLOAT', 'name': 'kkkk_1'},
                               'k_1.kk_1.kkk_3.kkkk_2': {'mode': 'NULLABLE', 'type': 'FLOAT', 'name': 'kkkk_2'},
                              }

value_schema_dict_5 = {}
value_elem_key_5 = 'k_1'
value_elem_value_5 = {'kk_1': [],
                      'kk_2': {},
                      'kk_3': {'kkk_1': {},
                               'kkk_2': []}}
value_parents_5 = ''
expected_r_element_schema_5 = {'k_1': {'mode': 'NULLABLE', 'name': 'k_1', 'type': 'RECORD'},
                               'k_1.kk_3': {'mode': 'NULLABLE', 'name': 'kk_3', 'type': 'RECORD'}}

@pytest.mark.parametrize("value_schema_dict, value_elem_k, value_elem_v, value_parents, "
                         "expected_result",
                         [(value_schema_dict_1, value_elem_key_1, value_elem_value_1, value_parents_1,
                          expected_r_element_schema_1),
                          (value_schema_dict_2, value_elem_key_2, value_elem_value_2, value_parents_2,
                          expected_r_element_schema_2),
                          (value_schema_dict_3, value_elem_key_3, value_elem_value_3, value_parents_3,
                          expected_r_element_schema_3),
                          (value_schema_dict_4, value_elem_key_4, value_elem_value_4, value_parents_4,
                          expected_r_element_schema_4),
                          (value_schema_dict_5, value_elem_key_5, value_elem_value_5, value_parents_5,
                          expected_r_element_schema_5),
                          ])
def test_get_element_schema(value_schema_dict, value_elem_k, value_elem_v, value_parents, expected_result):
    schema_generator = SchemaGenerator()
    schema_generator.schema_columns_dict = value_schema_dict
    schema_generator._get_element_schema(value_elem_k, value_elem_v, value_parents)
    assert schema_generator.schema_columns_dict == expected_result

"""
TEST update_schema_columns
"""

schema_dict = dict()

value_batch_schema_1 = [{"k_1": 10}]
expected_r_batch_schema_1 = {'k_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'}}

value_batch_schema_2 = [{"k_1": 10, "k_2": True, "k_3": "test"}, {"k_1": 5, "k_4": 1.4}]
expected_r_batch_schema_2 = {'k_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'},
                            'k_2': {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'k_2'},
                            'k_3': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'k_3'},
                            'k_4': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_4'}}

value_batch_schema_3 = [{"k_1": 10,
                         "k_2": True,
                         "k_3": {
                                "kk_1": 45,
                                "kk_2": "test",
                        }},
                        {"k_1": 5,
                         "k_2": False,
                         "k_3": {
                                "kk_1": 2,
                                "kk_2": "testing",
                                "kk_3": False
                        }}]
expected_r_batch_schema_3 = {'k_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'},
                            'k_2': {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'k_2'},
                            'k_3': {'mode': 'NULLABLE', 'type': "RECORD", 'name': 'k_3'},
                    'k_3.kk_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'kk_1'},
                    'k_3.kk_2': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'kk_2'},
                    'k_3.kk_3': {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'kk_3'}
                    }

value_batch_schema_4 = [{"k_1": 10,
                         "k_2": False,
                         "k_3": "10b",
                         "k_4": None,
                         "k_5": "string",
                         "k_6": {"kk_3": 10}
                         },
                        {"k_1": "10a",
                         "k_2": "False",
                         "k_3": 10,
                         "k_4": {"kk_1": 2},
                         "k_5": {"kk_2": "hello"},
                         "k_6": {"kk_3": "10c"}
                        }]
expected_r_batch_schema_4 = {'k_1': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'k_1'},
                            'k_2': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'k_2'},
                            'k_3': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'k_3'},
                    'k_4': {'mode': 'NULLABLE', 'type': "RECORD", 'name': 'k_4'},
                    'k_4.kk_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'kk_1'},
                    'k_5': {'mode': 'NULLABLE', 'type': "RECORD", 'name': 'k_5'},
                    'k_5.kk_2': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'kk_2'},
                    'k_6': {'mode': 'NULLABLE', 'type': "RECORD", 'name': 'k_6'},
                    'k_6.kk_3': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'kk_3'},
                    }

value_batch_schema_5 = [{"k_1": None}]
expected_r_batch_schema_5 = {} # func returns immediately

@pytest.mark.parametrize("value, expected_result", [(value_batch_schema_1, expected_r_batch_schema_1),
                                                    (value_batch_schema_2, expected_r_batch_schema_2),
                                                    (value_batch_schema_3, expected_r_batch_schema_3),
                                                    (value_batch_schema_4, expected_r_batch_schema_4),
                                                    (value_batch_schema_5, expected_r_batch_schema_5)])
def test_get_batch_schema(value, expected_result):
    schema_generator = SchemaGenerator()
    schema_generator.update_schema_columns(value)
    assert schema_generator.schema_columns_dict == expected_result

"""
TEST update_schema_columns_dict
"""
@pytest.mark.parametrize("value", [("NULL"), \
                                   (None)])
def test_update_schema_columns_dict_exception(value):
    schema_generator = SchemaGenerator()
    elem_name = 'k_1'
    element_schema = {"mode": "NULLABLE", "type": value}
    with pytest.raises(Exception) as e_info:
        schema_generator._update_schema_columns_dict(elem_name, element_schema)

"""
TEST construct_nesting_dict
"""

value_nesting_dict_1 = {'col_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'col_1'},
                        'col_2': {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'col_2'},
                        'col_1.col_3': {'mode': 'NULLABLE', 'type': "RECORD", 'name': 'col_1.col_3'},
                        'col_1.col_3.col_5': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'col_1.col_3.col_5'},
                        'col_1.col_4': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'col_1.col_4'}
                        }

expected_r_nesting_dict_1 = {1: ['col_1', 'col_2'], 2: ['col_1.col_3', 'col_1.col_4'], 3: ['col_1.col_3.col_5']}
@pytest.mark.parametrize("value, expected_result", [(value_nesting_dict_1, expected_r_nesting_dict_1)])
def test_construct_nesting_dict(value, expected_result):
    assert schema_generator._construct_nesting_dict(value) == expected_result

"""
TEST construct_bq_schema
"""

value_construct_bq_schema_1 = {'k_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'}}
expected_r_construct_bq_schema_1 = [{'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'}]

value_construct_bq_schema_2 = { 'k_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'},
                                'k_2': {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'k_2'},
                                'k_3': {'mode': 'NULLABLE', 'type': "RECORD", 'name': 'k_3'},
                                'k_3.kk_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'kk_1'},
                                'k_3.kk_2': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'kk_2'},
                                'k_3.kk_3': {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'kk_3'}
                                }
expected_r_construct_bq_schema_2 = [{'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'},
                                    {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'k_2'},
                                    {'mode': 'NULLABLE', 'type': "RECORD", 'name': 'k_3', 'fields':
                                        [{'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'kk_1'},
                                        {'mode': 'NULLABLE', 'type': "STRING", 'name': 'kk_2'},
                                        {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'kk_3'}]
                                    }]
value_construct_bq_schema_3 = { 'k_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'},
                                'k_3.kk_1': {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'kk_1'},
                                'k_2': {'mode': 'NULLABLE', 'type': 'RECORD', 'name': 'k_2'},
                                'k_3': {'mode': 'NULLABLE', 'type': 'RECORD', 'name': 'k_3'},
                                'k_2.kk_4.kkk_1' : {'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'kkk_1'},
                                'k_3.kk_2': {'mode': 'NULLABLE', 'type': "STRING", 'name': 'kk_2'},
                                'k_3.kk_3': {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'kk_3'},
                                'k_2.kk_4': {'mode': 'NULLABLE', 'type': 'RECORD', 'name': 'kk_4'},
                                }
expected_r_construct_bq_schema_3 = [{'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'k_1'},
                                    {'mode': 'NULLABLE', 'type': "RECORD", 'name': 'k_2', 'fields':
                                        [{'mode': 'NULLABLE', 'type': "RECORD", 'name': 'kk_4', "fields":
                                          [{'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'kkk_1'}]}]},
                                    {'mode': 'NULLABLE', 'type': "RECORD", 'name': 'k_3', 'fields':
                                        [{'mode': 'NULLABLE', 'type': "FLOAT", 'name': 'kk_1'},
                                        {'mode': 'NULLABLE', 'type': "STRING", 'name': 'kk_2'},
                                        {'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'kk_3'}]
                                    }]

value_construct_bq_schema_4 = {'k_1': {'mode': 'NULLABLE', 'name': 'k_1', 'type': 'RECORD'},
                               'k_1.kk_3': {'mode': 'NULLABLE', 'name': 'k_1.kk_3', 'type': 'RECORD'}}
expected_r_construct_bq_schema_4 = []

@pytest.mark.parametrize("value, expected_result", [(value_construct_bq_schema_1, expected_r_construct_bq_schema_1),
                                                    (value_construct_bq_schema_2, expected_r_construct_bq_schema_2),
                                                    (value_construct_bq_schema_3, expected_r_construct_bq_schema_3),
                                                    (value_construct_bq_schema_4, expected_r_construct_bq_schema_4)])
def test_construct_bq_schema(value, expected_result):
    schema_generator = SchemaGenerator()
    schema_generator._construct_bq_schema(value)
    assert schema_generator.bq_consumable_schema == expected_result

"""
TEST batch_to_bq_schema
"""
# WIP. change the test data to something that has no context
value_batch_to_bq_schema_0 = [{'k_1': []}, {'k_2': True}]
expected_r_batch_to_bq_schema_0 = [{'mode': 'NULLABLE', 'type': "BOOLEAN", 'name': 'k_2'}]
with open('tests/test_data.json', 'r') as f:
    test_values = json.load(f)

value_batch_to_bq_schema_1 = test_values["value_batch_to_bq_schema_1"]
expected_r_batch_to_bq_schema_1= test_values["expected_r_batch_to_bq_schema_1"]
value_batch_to_bq_schema_2 = test_values["value_batch_to_bq_schema_2"]
expected_r_batch_to_bq_schema_2 = test_values["expected_r_batch_to_bq_schema_2"]


@pytest.mark.parametrize("value, expected_result", [(value_batch_to_bq_schema_0, expected_r_batch_to_bq_schema_0),
                                                    (value_batch_to_bq_schema_1, expected_r_batch_to_bq_schema_1),
                                                    (value_batch_to_bq_schema_2, expected_r_batch_to_bq_schema_2)])
def test_batch_to_bq_schema(value, expected_result):
    assert batch_to_bq_schema(value) == expected_result
