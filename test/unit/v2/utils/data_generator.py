import random
from typing import Any, Type

from faker import Faker

fake = Faker()

type_to_random_value_map = {
    str: fake.sentence,
    int: fake.random_int,
    float: fake.random_digit,
    bool: fake.boolean,
}
type_to_random_value_map_key = type_to_random_value_map.copy()
type_to_random_value_map_key[str] = fake.word


def generate_random_dictionary(key_type: Type = str, value_type: Type = str) -> dict:
    d = {}
    num_keys = random.randint(1, 3)
    for i in range(num_keys):
        key = type_to_random_value_map_key[key_type]()
        current_value_type = value_type
        if current_value_type == Any:
            current_value_type = random.choice(list(type_to_random_value_map.keys()) + [dict])
        value = (
            generate_random_dictionary(key_type=key_type, value_type=value_type)
            if current_value_type is dict
            else type_to_random_value_map[current_value_type]()
        )
        d[key] = value
    return d
