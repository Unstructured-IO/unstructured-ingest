CREATE TABLE elements (
    id TEXT PRIMARY KEY,
    record_id TEXT,
    element_id TEXT,
    text TEXT,
    embeddings TEXT,
    type TEXT,
    system TEXT,
    layout_width REAL,
    layout_height REAL,
    points TEXT,
    url TEXT,
    version TEXT,
    date_created TEXT,
    date_modified TEXT,
    date_processed TEXT,
    permissions_data TEXT,
    record_locator TEXT,
    category_depth INTEGER,
    parent_id TEXT,
    attached_filename TEXT,
    filetype TEXT,
    last_modified TEXT,
    file_directory TEXT,
    filename TEXT,
    languages TEXT,
    page_number TEXT,
    links TEXT,
    page_name TEXT,
    link_urls TEXT,
    link_texts TEXT,
    sent_from TEXT,
    sent_to TEXT,
    subject TEXT,
    section TEXT,
    header_footer_type TEXT,
    emphasized_text_contents TEXT,
    emphasized_text_tags TEXT,
    text_as_html TEXT,
    regex_metadata TEXT,
    detection_class_prob DECIMAL
);
