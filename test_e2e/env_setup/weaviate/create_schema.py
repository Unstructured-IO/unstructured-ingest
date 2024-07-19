#!/usr/bin/env python3

import json
import os
from pathlib import Path

import weaviate

current_dir = Path(__file__).parent.absolute()

elements_file = current_dir / "elements.json"

weaviate_host_url = os.getenv("WEAVIATE_HOST_URL", "http://localhost:8080")
class_name = os.getenv("WEAVIATE_CLASS_NAME", "Elements")
new_class = None

with elements_file.open() as f:
    new_class = json.load(f)

client = weaviate.Client(
    url=weaviate_host_url,
)

if client.schema.exists(class_name):
    client.schema.delete_class(class_name)
client.schema.create_class(new_class)
