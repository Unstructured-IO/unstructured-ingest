import base64
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from uuid import NAMESPACE_DNS, uuid5

import requests
from bs4 import BeautifulSoup
from requests import Session

from unstructured_ingest.v2.interfaces import DownloadResponse, FileData, SourceIdentifiers
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
        except Exception as e:
            logger.warning(
                f"failed to download image content from {source_url}: {e}", exc_info=True
            )
    return str(soup)


def download_link(
    download_dir: Path, link: str, session: Optional[Session] = None, force_download: bool = False
) -> Path:
    session = session or requests.Session()
    filename = Path(urlparse(url=link).path).name
    download_path = download_dir / filename
    logger.debug(f"downloading file from {link} to {download_path}")
    if download_path.exists() and download_path.is_file() and not force_download:
        return download_path
    with download_path.open("wb") as downloaded_file:
        response = session.get(link)
        response.raise_for_status()
        downloaded_file.write(response.content)
    return download_path


def download_embedded_files(
    download_dir: Path,
    original_filedata: FileData,
    original_html: str,
    session: Optional[Session] = None,
    force_download: bool = False,
) -> list[DownloadResponse]:
    session = session or requests.Session()
    url = original_filedata.metadata.url
    parsed_url = urlparse(url)
    base_url = parsed_url.scheme + "://" + parsed_url.netloc
    soup = BeautifulSoup(original_html, "html.parser")
    tags = soup.find_all("a", href=True)
    hrefs = [tag["href"] for tag in tags if not tag["href"].startswith("#")]
    results = []
    for current_source in hrefs:
        download_dir.mkdir(parents=True, exist_ok=True)
        if current_source.startswith("//"):
            source_url = f"{parsed_url.scheme}:{current_source}"
        elif current_source.startswith("http"):
            source_url = current_source
        else:
            source_url = base_url + current_source
        try:
            downloaded_path = download_link(
                download_dir=download_dir,
                link=source_url,
                session=session,
                force_download=force_download,
            )
        except Exception as e:
            logger.warning(f"failed to download file content from {source_url}: {e}")
            continue
        result_file_data = original_filedata.model_copy(deep=True)
        result_file_data.metadata.url = source_url
        result_file_data.metadata.record_locator["parent_url"] = url
        result_file_data.identifier = str(
            uuid5(NAMESPACE_DNS, source_url + original_filedata.identifier)
        )
        filename = Path(urlparse(url=source_url).path).name
        result_file_data.source_identifiers = SourceIdentifiers(
            filename=filename, fullpath=filename
        )
        result_file_data.local_download_path = downloaded_path.as_posix()
        results.append(DownloadResponse(file_data=result_file_data, path=downloaded_path))
    return results
