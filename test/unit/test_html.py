import base64
from pathlib import Path

from bs4 import BeautifulSoup
from pytest_mock import MockerFixture

from unstructured_ingest.utils.html import HtmlMixin
from unstructured_ingest.v2.interfaces import FileData, SourceIdentifiers


def test_extract_images(mocker: MockerFixture):
    mixin = HtmlMixin(extract_images=True)
    mock_download_response = b"DOWNLOADED"
    expected_image_src = base64.b64encode(mock_download_response).decode()
    mocked_download_response = mocker.patch(
        "unstructured_ingest.utils.html.HtmlMixin.download_content",
        return_value=mock_download_response,
    )
    url = "http://mywebsite.com/path/to/page"
    html = """
    <img src="http://mywebsite.com/img1.jpg"/>
    <img src="http://notmywebsite.com/img2.jpg"/>
    <img src="img3.jpg"/>
    <img src="data:image/png;base64,24689654..."/>
    """
    expected_html = f"""
    <img src="data:image/png;base64,{expected_image_src}"/>
    <img src="http://notmywebsite.com/img2.jpg"/>
    <img src="data:image/png;base64,{expected_image_src}"/>
    <img src="data:image/png;base64,24689654..."/>
    """
    expected_soup = BeautifulSoup(expected_html, "html.parser")
    result = mixin.extract_html_images(url=url, html=html)
    result_soup = BeautifulSoup(result, "html.parser")
    assert expected_soup == result_soup
    assert mocked_download_response.call_count == 2
    urls_to_download = [
        call_args_list.kwargs["url"] for call_args_list in mocked_download_response.call_args_list
    ]
    assert urls_to_download == ["http://mywebsite.com/img1.jpg", "http://mywebsite.com/img3.jpg"]


def test_extract_images_allow_list(mocker: MockerFixture):
    mixin = HtmlMixin(
        extract_images=True, allow_list=["http://allowedwebsite1.com", "http://allowedwebsite2.com"]
    )
    mock_download_response = b"DOWNLOADED"
    expected_image_src = base64.b64encode(mock_download_response).decode()
    mocked_download_response = mocker.patch(
        "unstructured_ingest.utils.html.HtmlMixin.download_content",
        return_value=mock_download_response,
    )
    url = "http://mywebsite.com/path/to/page"
    html = """
    <img src="http://mywebsite.com/img1.jpg"/>
    <img src="http://notmywebsite.com/img2.jpg"/>
    <img src="http://allowedwebsite1.com/img2.jpg"/>
    <img src="http://allowedwebsite2.com/img2.jpg"/>
    """

    expected_html = f"""
    <img src="http://mywebsite.com/img1.jpg"/>
    <img src="http://notmywebsite.com/img2.jpg"/>
    <img src="data:image/png;base64,{expected_image_src}"/>
    <img src="data:image/png;base64,{expected_image_src}"/>
    """
    expected_soup = BeautifulSoup(expected_html, "html.parser")
    result = mixin.extract_html_images(url=url, html=html)
    result_soup = BeautifulSoup(result, "html.parser")
    assert expected_soup == result_soup
    assert mocked_download_response.call_count == 2
    urls_to_download = [
        call_args_list.kwargs["url"] for call_args_list in mocked_download_response.call_args_list
    ]
    assert urls_to_download == [
        "http://allowedwebsite1.com/img2.jpg",
        "http://allowedwebsite2.com/img2.jpg",
    ]


def test_extract_embedded_docs(mocker: MockerFixture):
    mixin = HtmlMixin(extract_files=True)
    mock_download_response = b"DOWNLOADED"
    mocked_download_response = mocker.patch(
        "unstructured_ingest.utils.html.HtmlMixin.download_content",
        return_value=mock_download_response,
    )
    mocked_write_content = mocker.patch("unstructured_ingest.utils.html.HtmlMixin.write_content")
    url = "http://mywebsite.com/path/to/page"
    html = """
    <a href="http://mywebsite.com/file.pdf"/>
    <a href="http://notmywebsite.com/file.pdf"/>
    <a href="http://mywebsite.com/another/link"/>
    <a href="another/link/2"/>
    <a href="file.doc"/>
    """
    file_data = FileData(
        source_identifiers=SourceIdentifiers(
            fullpath="file.txt",
            filename="file.txt",
        ),
        connector_type="my_connector",
        identifier="mock_file_data",
    )
    results = mixin.extract_embedded_files(
        url=url, html=html, download_dir=Path("/tmp/download/location"), original_filedata=file_data
    )
    assert len(results) == 2
    downloaded_urls = [r["file_data"].metadata.url for r in results]
    assert downloaded_urls == ["http://mywebsite.com/file.pdf", "http://mywebsite.com/file.doc"]
    assert mocked_download_response.call_count == 2
    assert mocked_write_content.call_count == 2
