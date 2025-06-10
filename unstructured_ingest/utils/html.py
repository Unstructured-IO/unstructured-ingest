import base64
from pathlib import Path
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse
from uuid import NAMESPACE_DNS, uuid5

from pydantic import BaseModel, Field

from unstructured_ingest.data_types.file_data import FileData, SourceIdentifiers
from unstructured_ingest.interfaces import DownloadResponse
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from bs4 import BeautifulSoup
    from bs4.element import Tag
    from requests import Session


class HtmlMixin(BaseModel):
    extract_images: bool = Field(
        default=False,
        description="if true, will download images and replace "
        "the html content with base64 encoded images",
    )
    extract_files: bool = Field(
        default=False, description="if true, will download any embedded files"
    )
    force_download: bool = Field(
        default=False,
        description="if true, will redownload extracted files even if they already exist locally",
    )
    allow_list: Optional[list[str]] = Field(
        default=None,
        description="list of allowed urls to download, if not set, "
        "will default to the base url the original HTML came from",
    )

    @requires_dependencies(["requests"])
    def get_default_session(self) -> "Session":
        import requests

        return requests.Session()

    def get_absolute_url(self, tag_link: str, url: str) -> str:
        parsed_url = urlparse(url)
        base_url = parsed_url.scheme + "://" + parsed_url.netloc
        if tag_link.startswith("//"):
            return f"{parsed_url.scheme}:{tag_link}"
        elif tag_link.startswith("http"):
            return tag_link
        else:
            tag_link = tag_link.lstrip("/")
            return f"{base_url}/{tag_link}"

    def download_content(self, url: str, session: "Session") -> bytes:
        response = session.get(url)
        response.raise_for_status()
        return response.content

    def can_download(self, url_to_download: str, original_url: str) -> bool:
        parsed_original_url = urlparse(original_url)
        base_url = parsed_original_url.scheme + "://" + parsed_original_url.netloc
        allow_list = self.allow_list or [base_url]
        for allowed_url in allow_list:
            if url_to_download.startswith(allowed_url):
                return True
        logger.info(f"Skipping url because it does not match the allow list: {url_to_download}")
        return False

    def extract_image_src(self, image: "Tag", url: str, session: "Session") -> "Tag":
        current_src = image["src"]
        if current_src.startswith("data:image/png;base64"):
            # already base64 encoded
            return image
        absolute_url = self.get_absolute_url(tag_link=image["src"], url=url)
        if not self.can_download(url_to_download=absolute_url, original_url=url):
            return image
        image_content = self.download_content(url=absolute_url, session=session)
        logger.debug("img tag having src updated from {} to base64 content".format(image["src"]))
        image["src"] = f"data:image/png;base64,{base64.b64encode(image_content).decode()}"
        return image

    @requires_dependencies(["bs4"])
    def extract_html_images(self, url: str, html: str, session: Optional["Session"] = None) -> str:
        from bs4 import BeautifulSoup

        session = session or self.get_default_session()
        soup = BeautifulSoup(html, "html.parser")
        images = soup.find_all("img")
        for image in images:
            self.extract_image_src(image=image, url=url, session=session)
        return str(soup)

    @requires_dependencies(["bs4"])
    def get_hrefs(self, url: str, html: str) -> list:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        tags = self._find_hyperlink_tags(soup)
        hrefs = [
            tag["href"]
            for tag in tags
            if not tag["href"].startswith("#") and Path(tag["href"]).suffix != ""
        ]
        absolute_urls = [self.get_absolute_url(tag_link=href, url=url) for href in hrefs]
        allowed_urls = [
            url_to_download
            for url_to_download in absolute_urls
            if self.can_download(url_to_download=url_to_download, original_url=url)
        ]
        return allowed_urls

    def write_content(self, content: bytes, path: Path) -> None:
        if path.exists() and path.is_file() and not self.force_download:
            return
        if not path.parent.exists():
            path.parent.mkdir(parents=True)
        with path.open("wb") as f:
            f.write(content)

    def get_download_response(
        self, url: str, download_dir: Path, file_data: FileData, session: "Session"
    ) -> DownloadResponse:
        filename = Path(urlparse(url=url).path).name
        download_path = download_dir / filename
        self.write_content(
            content=self.download_content(url=url, session=session), path=download_path
        )
        result_file_data = file_data.model_copy(deep=True)
        result_file_data.metadata.url = url
        if result_file_data.metadata.record_locator is None:
            result_file_data.metadata.record_locator = {}
        result_file_data.metadata.record_locator["parent_url"] = url
        result_file_data.identifier = str(uuid5(NAMESPACE_DNS, url + file_data.identifier))
        filename = Path(urlparse(url=url).path).name
        result_file_data.source_identifiers = SourceIdentifiers(
            filename=filename, fullpath=filename
        )
        result_file_data.local_download_path = download_path.as_posix()
        return DownloadResponse(file_data=result_file_data, path=download_path)

    def extract_embedded_files(
        self,
        url: str,
        html: str,
        download_dir: Path,
        original_filedata: FileData,
        session: Optional["Session"] = None,
    ) -> list[DownloadResponse]:
        session = session or self.get_default_session()
        urls_to_download = self.get_hrefs(url=url, html=html)
        return [
            self.get_download_response(
                url=url_to_download,
                download_dir=download_dir,
                file_data=original_filedata,
                session=session,
            )
            for url_to_download in urls_to_download
        ]

    @requires_dependencies(["bs4"])
    def _find_hyperlink_tags(self, html_soup: "BeautifulSoup") -> list["Tag"]:
        """Find hyperlink tags in the HTML.

        Overwrite this method to customize the tag search.
        """
        from bs4.element import Tag

        return [
            element for element in html_soup.find_all("a", href=True) if isinstance(element, Tag)
        ]
