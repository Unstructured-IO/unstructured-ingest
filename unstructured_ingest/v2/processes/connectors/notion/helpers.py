import enum
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from urllib.parse import urlparse
from uuid import UUID

from htmlBuilder.attributes import Style
from htmlBuilder.tags import (
    Body,
    Div,
    Head,
    Html,
    HtmlTag,
    Ol,
    Table,
    Td,
    Th,
    Title,
    Tr,
    Ul,
)
from notion_client.errors import APIResponseError

import unstructured_ingest.v2.processes.connectors.notion.types.blocks as notion_blocks
from unstructured_ingest.v2.processes.connectors.notion.client import Client
from unstructured_ingest.v2.processes.connectors.notion.types.block import Block
from unstructured_ingest.v2.processes.connectors.notion.types.database import Database


@dataclass
class HtmlExtractionResponse:
    html: Optional[HtmlTag] = None
    child_pages: List[str] = field(default_factory=list)
    child_databases: List[str] = field(default_factory=list)


def process_block(
    current_block: dict,
    parent_page_id: str,
    client: Client,
    child_pages: list,
    child_databases: list,
) -> Tuple[dict, list, list, dict]:
    if isinstance(current_block["block"].block, notion_blocks.ChildPage) and current_block[
        "block"
    ].id != str(parent_page_id):
        child_pages.append(current_block["block"].id)
        return {}, child_pages, child_databases
    if isinstance(current_block["block"].block, notion_blocks.ChildDatabase):
        child_databases.append(current_block["block"].id)
        return {}, child_pages, child_databases

    # recursively go through all blocks in a page, store each block in a dictionary
    if current_block["block"].has_children:
        children = []
        for children_block in client.blocks.children.iterate_list(
            block_id=current_block["block"].id
        ):
            children.extend(children_block)
        if children:
            for child in children:
                child_block = {
                    "block": child,
                    "level": current_block["level"] + 1,
                    "children": [],
                    "parent_id": current_block["block"].id,
                }
                child_element, child_pages, child_databases = process_block(
                    child_block, parent_page_id, client, child_pages, child_databases
                )
                current_block["children"].append(child_element)
    return current_block, child_pages, child_databases


def flush_list(type: str, item_list: list, html: list) -> Tuple[list, list]:
    margin_left = 10 * (item_list[-1][1] - 1)
    style = Style(f"margin-left: {margin_left}px")
    if type == "bulleted_list":
        html.append(Ul([style], [item[2] for item in item_list]))
    else:
        html.append(Ol([style], [item[2] for item in item_list]))
    return [], html


def build_html(
    current_block: dict, bulleted_list: list, numbered_list: list
) -> Tuple[list, list, list]:
    html = []
    # extract current block's html
    if isinstance(current_block["block"].block, notion_blocks.BulletedListItem):
        if bulleted_list and current_block["parent_id"] != bulleted_list[-1][0]:
            bulleted_list, html = flush_list("bulleted_list", bulleted_list, html)
        bulleted_list.append(
            (current_block["parent_id"], current_block["level"], current_block["block"].get_html())
        )
        if bulleted_list and current_block["peers_rank"] == current_block["peers_count"] - 1:
            bulleted_list, html = flush_list("bulleted_list", bulleted_list, html)
    elif isinstance(current_block["block"].block, notion_blocks.NumberedListItem):
        if numbered_list and current_block["parent_id"] != numbered_list[-1][0]:
            numbered_list, html = flush_list("numbered_list", numbered_list, html)
        numbered_list.append(
            (current_block["parent_id"], current_block["level"], current_block["block"].get_html())
        )
        if numbered_list and current_block["peers_rank"] == current_block["peers_count"] - 1:
            numbered_list, html = flush_list("numbered_list", numbered_list, html)
    else:
        if bulleted_list:
            bulleted_list, html = flush_list("bulleted_list", bulleted_list, html)
        if numbered_list:
            numbered_list, html = flush_list("numbered_list", numbered_list, html)
        if (
            isinstance(current_block["block"].block, notion_blocks.TableRow)
            and current_block["peers_rank"] == 0
        ):
            current_block["block"].is_header = True
        if current_block["block"].get_html():
            html.append(current_block["block"].get_html())
        else:
            html.append([])
    # process current block's children
    if current_block["children"]:
        children_html = []
        for index, child in enumerate(current_block["children"]):
            if child:
                child["peers_rank"] = index
                child["peers_count"] = len(current_block["children"])
                child_html, bulleted_list, numbered_list = build_html(
                    child, bulleted_list, numbered_list
                )
                if child_html:
                    children_html.append(child_html)
        if isinstance(current_block["block"].block, notion_blocks.Column):
            html.append(
                Div(
                    [Style(f"width:{100 / current_block['peers_count']}%; float: left")],
                    children_html,
                )
            )
        elif isinstance(current_block["block"].block, notion_blocks.Table):
            html.append(Table([], children_html))
        else:
            html.append(Div([], children_html))

    return html, bulleted_list, numbered_list


def extract_page_html(
    client: Client,
    page_id: str,
    logger: logging.Logger,
) -> HtmlExtractionResponse:
    parent_page_id = UUID(page_id)
    parent_block: Block = client.blocks.retrieve(block_id=page_id)  # type: ignore
    head = None
    if isinstance(parent_block.block, notion_blocks.ChildPage):
        head = Head([], Title([], parent_block.block.title))
    current_block = {
        "block": parent_block,
        "level": 0,
        "children": [],
        "parent_id": None,
        "peers_rank": 0,
        "peers_count": 1,
    }
    logger.debug(f"processing page id: {page_id}")
    current_block, child_pages, child_databases = process_block(
        current_block, parent_page_id, client, [], []
    )
    html, _, _ = build_html(current_block, [], [])
    body = Body([], html)
    all_elements = [body]
    if head:
        all_elements = [head] + all_elements
    full_html = Html([], all_elements)
    return HtmlExtractionResponse(
        full_html,
        child_pages=child_pages,
        child_databases=child_databases,
    )


def extract_database_html(
    client: Client,
    database_id: str,
    logger: logging.Logger,
) -> HtmlExtractionResponse:
    logger.debug(f"processing database id: {database_id}")
    database: Database = client.databases.retrieve(database_id=database_id)  # type: ignore
    property_keys = list(database.properties.keys())
    property_keys = sorted(property_keys)
    table_html_rows = []
    child_pages: List[str] = []
    child_databases: List[str] = []
    # Create header row
    table_html_rows.append(Tr([], [Th([], k) for k in property_keys]))

    all_pages = []
    for page_chunk in client.databases.iterate_query(database_id=database_id):  # type: ignore
        all_pages.extend(page_chunk)

    logger.debug(f"creating {len(all_pages)} rows")
    for page in all_pages:
        if is_database_url(client=client, url=page.url):
            child_databases.append(page.id)
        if is_page_url(client=client, url=page.url):
            child_pages.append(page.id)
        properties = page.properties
        inner_html = [properties.get(k).get_html() for k in property_keys]  # type: ignore
        table_html_rows.append(
            Tr(
                [],
                [Td([], cell) for cell in [html if html else Div([], []) for html in inner_html]],
            ),
        )

    table_html = Table([], table_html_rows)

    return HtmlExtractionResponse(
        html=table_html,
        child_pages=child_pages,
        child_databases=child_databases,
    )


@dataclass
class ChildExtractionResponse:
    child_pages: List[str] = field(default_factory=list)
    child_databases: List[str] = field(default_factory=list)


class QueueEntryType(enum.Enum):
    DATABASE = "database"
    PAGE = "page"


@dataclass
class QueueEntry:
    type: QueueEntryType
    id: UUID


def get_recursive_content_from_page(
    client: Client,
    page_id: str,
    logger: logging.Logger,
) -> ChildExtractionResponse:
    return get_recursive_content(
        client=client,
        init_entry=QueueEntry(type=QueueEntryType.PAGE, id=UUID(page_id)),
        logger=logger,
    )


def get_recursive_content_from_database(
    client: Client,
    database_id: str,
    logger: logging.Logger,
) -> ChildExtractionResponse:
    return get_recursive_content(
        client=client,
        init_entry=QueueEntry(type=QueueEntryType.DATABASE, id=UUID(database_id)),
        logger=logger,
    )


def get_recursive_content(
    client: Client,
    init_entry: QueueEntry,
    logger: logging.Logger,
) -> ChildExtractionResponse:
    parents: List[QueueEntry] = [init_entry]
    child_pages: List[str] = []
    child_dbs: List[str] = []
    processed: List[str] = []
    while len(parents) > 0:
        parent: QueueEntry = parents.pop()
        processed.append(str(parent.id))
        if parent.type == QueueEntryType.PAGE:
            logger.debug(f"getting child data from page: {parent.id}")
            page_children = []
            try:
                for children_block in client.blocks.children.iterate_list(  # type: ignore
                    block_id=str(parent.id),
                ):
                    page_children.extend(children_block)
            except APIResponseError as api_error:
                logger.error(f"failed to get page with id {parent.id}: {api_error}")
                if str(parent.id) in child_pages:
                    child_pages.remove(str(parent.id))
                continue
            if not page_children:
                continue

            # Extract child pages
            child_pages_from_page = [
                c for c in page_children if isinstance(c.block, notion_blocks.ChildPage)
            ]
            if child_pages_from_page:
                child_page_blocks: List[notion_blocks.ChildPage] = [
                    p.block
                    for p in child_pages_from_page
                    if isinstance(p.block, notion_blocks.ChildPage)
                ]
                logger.debug(
                    "found child pages from parent page {}: {}".format(
                        parent.id,
                        ", ".join([block.title for block in child_page_blocks]),
                    ),
                )
            new_pages = [p.id for p in child_pages_from_page if p.id not in processed]
            new_pages = list(set(new_pages))
            child_pages.extend(new_pages)
            parents.extend(
                [QueueEntry(type=QueueEntryType.PAGE, id=UUID(i)) for i in new_pages],
            )

            # Extract child databases
            child_dbs_from_page = [
                c for c in page_children if isinstance(c.block, notion_blocks.ChildDatabase)
            ]
            if child_dbs_from_page:
                child_db_blocks: List[notion_blocks.ChildDatabase] = [
                    c.block
                    for c in page_children
                    if isinstance(c.block, notion_blocks.ChildDatabase)
                ]
                logger.debug(
                    "found child database from parent page {}: {}".format(
                        parent.id,
                        ", ".join([block.title for block in child_db_blocks]),
                    ),
                )
            new_dbs = [db.id for db in child_dbs_from_page if db.id not in processed]
            new_dbs = list(set(new_dbs))
            child_dbs.extend(new_dbs)
            parents.extend(
                [QueueEntry(type=QueueEntryType.DATABASE, id=UUID(i)) for i in new_dbs],
            )

            linked_to_others: List[notion_blocks.LinkToPage] = [
                c.block for c in page_children if isinstance(c.block, notion_blocks.LinkToPage)
            ]
            for link in linked_to_others:
                if (page_id := link.page_id) and (
                    page_id not in processed and page_id not in child_pages
                ):
                    child_pages.append(page_id)
                    parents.append(QueueEntry(type=QueueEntryType.PAGE, id=UUID(page_id)))
                if (database_id := link.database_id) and (
                    database_id not in processed and database_id not in child_dbs
                ):
                    child_dbs.append(database_id)
                    parents.append(
                        QueueEntry(type=QueueEntryType.DATABASE, id=UUID(database_id)),
                    )

        elif parent.type == QueueEntryType.DATABASE:
            logger.debug(f"getting child data from database: {parent.id}")
            database_pages = []
            try:
                for page_entries in client.databases.iterate_query(  # type: ignore
                    database_id=str(parent.id),
                ):
                    database_pages.extend(page_entries)
            except APIResponseError as api_error:
                logger.error(f"failed to get database with id {parent.id}: {api_error}")
                if str(parent.id) in child_dbs:
                    child_dbs.remove(str(parent.id))
                continue
            if not database_pages:
                continue

            child_pages_from_db = [
                p for p in database_pages if is_page_url(client=client, url=p.url)
            ]
            if child_pages_from_db:
                logger.debug(
                    "found child pages from parent database {}: {}".format(
                        parent.id,
                        ", ".join([p.url for p in child_pages_from_db]),
                    ),
                )
            new_pages = [p.id for p in child_pages_from_db if p.id not in processed]
            child_pages.extend(new_pages)
            parents.extend(
                [QueueEntry(type=QueueEntryType.PAGE, id=UUID(i)) for i in new_pages],
            )

            child_dbs_from_db = [
                p for p in database_pages if is_database_url(client=client, url=p.url)
            ]
            if child_dbs_from_db:
                logger.debug(
                    "found child database from parent database {}: {}".format(
                        parent.id,
                        ", ".join([db.url for db in child_dbs_from_db]),
                    ),
                )
            new_dbs = [db.id for db in child_dbs_from_db if db.id not in processed]
            child_dbs.extend(new_dbs)
            parents.extend(
                [QueueEntry(type=QueueEntryType.DATABASE, id=UUID(i)) for i in new_dbs],
            )

    return ChildExtractionResponse(
        child_pages=child_pages,
        child_databases=child_dbs,
    )


def is_valid_uuid(uuid_str: str) -> bool:
    try:
        UUID(uuid_str)
        return True
    except Exception:
        return False


def get_uuid_from_url(path: str) -> Optional[str]:
    strings = path.split("-")
    if len(strings) > 0 and is_valid_uuid(strings[-1]):
        return strings[-1]
    return None


def is_page_url(client: Client, url: str):
    parsed_url = urlparse(url)
    path = parsed_url.path.split("/")[-1]
    if parsed_url.netloc != "www.notion.so":
        return False
    page_uuid = get_uuid_from_url(path=path)
    if not page_uuid:
        return False
    check_resp = client.pages.retrieve_status(page_id=page_uuid)
    return check_resp == 200


def is_database_url(client: Client, url: str):
    parsed_url = urlparse(url)
    path = parsed_url.path.split("/")[-1]
    if parsed_url.netloc != "www.notion.so":
        return False
    database_uuid = get_uuid_from_url(path=path)
    if not database_uuid:
        return False
    check_resp = client.databases.retrieve_status(database_id=database_uuid)
    return check_resp == 200
