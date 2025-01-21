import base64
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests import Session

from unstructured_ingest.v2.logger import logger


def convert_image_tags(url: str, original_html: str, session: Optional[Session] = None) -> str:
    session = session or requests.Session()
    parsed_url = urlparse(url)
    base_url = parsed_url.scheme + "://" + parsed_url.netloc
    soup = BeautifulSoup(original_html, "html.parser")
    images = soup.find_all("img")
    for image in images:
        current_source = image["src"]
        if current_source.startswith("//"):
            source_url = f"{parsed_url.scheme}:{current_source}"
        elif current_source.startswith("http"):
            source_url = current_source
        else:
            source_url = base_url + current_source
        try:
            response = session.get(source_url)
            response.raise_for_status()
            image_content = response.content
            logger.debug(
                "img tag having src updated from {} to base64 content".format(image["src"])
            )
            image["src"] = f"data:image/png;base64,{base64.b64encode(image_content).decode()}"
        except Exception:
            logger.warning(f"failed to download image content from: {source_url}", exc_info=True)
    return str(soup)
