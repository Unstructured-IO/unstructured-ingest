import logging

logger = logging.getLogger("unstructured_ingest")


def remove_root_handlers(logger: logging.Logger) -> None:
    # NOTE(robinson): in some environments such as Google Colab, there is a root handler
    # that doesn't not mask secrets, meaning sensitive info such as api keys appear in logs.
    # Removing these when they exist prevents this behavior
    if logger.root.hasHandlers():
        for handler in logger.root.handlers:
            logger.root.removeHandler(handler)


def ingest_log_streaming_init(level: int) -> None:
    handler = logging.StreamHandler()
    handler.name = "ingest_log_handler"
    formatter = logging.Formatter("%(asctime)s %(processName)-10s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)

    # Only want to add the handler once
    if "ingest_log_handler" not in [h.name for h in logger.handlers]:
        logger.addHandler(handler)

    remove_root_handlers(logger)
    logger.setLevel(level)


def make_default_logger(level: int) -> logging.Logger:
    """Return a custom logger."""
    logger = logging.getLogger("unstructured_ingest")
    handler = logging.StreamHandler()
    handler.name = "ingest_log_handler"
    formatter = logging.Formatter("%(asctime)s %(processName)-10s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    remove_root_handlers(logger)
    return logger
