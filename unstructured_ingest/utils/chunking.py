import base64
import hashlib
import json
import zlib
from itertools import groupby


def id_to_hash(element: dict, sequence_number: int) -> str:
    """Calculates and assigns a deterministic hash as an ID.

    The hash ID is based on element's text, sequence number on page,
    page number and its filename.

    Args:
        sequence_number: index on page

    Returns: new ID value
    """
    filename = element["metadata"].get("filename")
    text = element["text"]
    page_number = element["metadata"].get("page_number")
    data = f"{filename}{text}{page_number}{sequence_number}"
    element["element_id"] = hashlib.sha256(data.encode()).hexdigest()[:32]
    return element["element_id"]


def assign_and_map_hash_ids(elements: list[dict]) -> list[dict]:
    # -- generate sequence number for each element on a page --
    elements = elements.copy()
    page_numbers = [e["metadata"].get("page_number") for e in elements]
    page_seq_pairs = [
        seq_on_page for page, group in groupby(page_numbers) for seq_on_page, _ in enumerate(group)
    ]

    # -- assign hash IDs to elements --
    old_to_new_mapping = {
        element["element_id"]: id_to_hash(element=element, sequence_number=seq_on_page_counter)
        for element, seq_on_page_counter in zip(elements, page_seq_pairs)
    }

    # -- map old parent IDs to new ones --
    for e in elements:
        parent_id = e["metadata"].get("parent_id")
        if not parent_id:
            continue
        e["metadata"]["parent_id"] = old_to_new_mapping[parent_id]

    return elements


def elements_from_base64_gzipped_json(raw_s: str) -> list[dict]:
    decoded_b64_bytes = base64.b64decode(raw_s)
    elements_json_bytes = zlib.decompress(decoded_b64_bytes)
    elements_json_str = elements_json_bytes.decode("utf-8")
    element_dicts = json.loads(elements_json_str)
    return element_dicts
