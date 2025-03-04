import base64
from dataclasses import dataclass
from typing import Dict, List

import httpx
import requests

from unstructured_ingest.v2.errors import ProviderError, UserError
from unstructured_ingest.v2.logger import logger


@dataclass
class Comment:
    id: str
    author_id: str
    body: str
    parent_ticket_id: str
    metadata: dict


@dataclass
class ZendeskTicket:
    id: str
    subject: str
    description: str
    generated_ts: int
    metadata: dict

    def __lt__(self, other):
        return self.id < other.id


@dataclass
class ZendeskArticle:
    id: str
    author_id: str
    title: str
    content: str

    def __lt__(self, other):
        return self.id < other.id


class ZendeskClient:

    def __init__(self, token: str, subdomain: str, email: str):
        # should be okay to be blocking.
        url_to_check = f"https://{subdomain}.zendesk.com/api/v2/groups.json"
        auth = f"{email}/token", token
        try:
            response = requests.get(url_to_check, auth=auth)

            http_code = response.status_code

            if 400 <= http_code < 500:
                message = (
                    f"Failed to connect to {url_to_check} using zendesk response"
                    f"status code {http_code}"
                )
                return UserError(message)

            if http_code >= 500:
                return ProviderError(f"Failed to connect to {url_to_check} using zendesk response")

            if http_code != 200:
                raise Exception(f"Failed to connect to {url_to_check} using zendesk response")

        except Exception as e:
            raise RuntimeError(f"Failed to instantiate response: {e}") from e

        self._token = token
        self._subdomain = subdomain
        self._email = email
        self._auth = auth

    async def get_articles_async(self) -> List[ZendeskArticle]:
        """
        Retrieves article content from Zendesk asynchronously.
        """

        articles: List[ZendeskArticle] = []

        article_url = f"https://{self._subdomain}.zendesk.com/api/v2/help_center/articles.json"

        async with httpx.AsyncClient() as client:
            response = await client.get(article_url, auth=self._auth)

        if response.status_code == 200:
            articles_in_response: List[dict] = response.json()["articles"]

            articles = [
                ZendeskArticle(
                    id=str(entry["id"]),
                    author_id=str(entry["author_id"]),
                    title=str(entry["title"]),
                    content=entry["body"],
                )
                for entry in articles_in_response
            ]
            return articles

        raise RuntimeError(
            f"Articles were not able to be acquired from url: {article_url}."
            f"Status Code: {response.status_code}"
        )

    def get_articles(self) -> List[ZendeskArticle]:
        """
        retrieves article content from zendesk using requests
        """

        articles: List[ZendeskArticle] = []

        article_url = f"https://{self._subdomain}.zendesk.com/api/v2/help_center/articles.json"

        response = requests.get(article_url, auth=self._auth)

        if response.status_code == 200:
            articles_in_response: list[dict] = response.json()["articles"]
            for entry in articles_in_response:
                articles.append(
                    ZendeskArticle(
                        id=str(entry["id"]),
                        author_id=str(entry["author_id"]),
                        title=str(entry["title"]),
                        content=entry["body"],
                    )
                )

        else:
            raise RuntimeError(f"Articles wer enot able to be acquired from url: {article_url}")

        return articles

    def get_comments(self, ticket_id: int) -> List[Comment]:

        comments: List[Comment] = []

        comments_url = f"https://{self._subdomain}.zendesk.com/api/v2/tickets/{ticket_id}/comments"

        response = requests.get(comments_url, auth=self._auth)

        if response.status_code == 200:
            comments_in_response: List[dict] = response.json()["comments"]

            for entry in comments_in_response:
                comment = Comment(
                    id=entry["id"],
                    author_id=entry["author_id"],
                    body=entry["body"],
                    metadata=entry,
                    parent_ticket_id=ticket_id,
                )
                comments.append(comment)
        else:
            raise RuntimeError(
                f"Comments for ticket id:{ticket_id} could not be acquired from url: {comments_url}"
            )

        return comments

    async def get_comments_async(self, ticket_id: int) -> List["Comment"]:
        comments_url = f"https://{self._subdomain}.zendesk.com/api/v2/tickets/{ticket_id}/comments"

        async with httpx.AsyncClient() as client:
            response = await client.get(comments_url, auth=self._auth)

        if response.status_code == 200:
            return [
                Comment(
                    id=entry["id"],
                    author_id=entry["author_id"],
                    body=entry["body"],
                    metadata=entry,
                    parent_ticket_id=ticket_id,
                )
                for entry in response.json()["comments"]
            ]

        raise RuntimeError(
            f"Comments for ticket id:{ticket_id} could not be acquired from url: {comments_url}"
        )

    def get_users(self) -> List[dict]:

        users: List[dict] = []

        users_url = f"https://{self._subdomain}.zendesk.com/api/v2/users"

        response = requests.get(users_url, auth=self._auth)

        if response.status_code == 200:
            users_in_response: List[dict] = response.json()["users"]
            users = users_in_response

        else:
            raise RuntimeError(f"Users could not be acquried from url: {users_url}")

        return users

    async def get_users_async(self) -> List[dict]:
        users_url = f"https://{self._subdomain}.zendesk.com/api/v2/users"

        async with httpx.AsyncClient() as client:
            response = await client.get(users_url, auth=self._auth)

        if response.status_code == 200:
            return response.json()["users"]

        raise RuntimeError(f"Users could not be acquired from url: {users_url}")

    async def get_tickets_async(self) -> List["ZendeskTicket"]:
        tickets: List["ZendeskTicket"] = []
        tickets_url = f"https://{self._subdomain}.zendesk.com/api/v2/tickets"

        async with httpx.AsyncClient() as client:
            response = await client.get(tickets_url, auth=self._auth)

        if response.status_code == 200:
            tickets_in_response: List[dict] = response.json()["tickets"]
        else:
            message = (
                f"Tickets could not be acquired from url: {tickets_url} "
                f"status {response.status_code}"
            )
            raise RuntimeError(message)

        for entry in tickets_in_response:
            ticket = ZendeskTicket(
                id=entry["id"],
                subject=entry["subject"],
                description=entry["description"],
                generated_ts=entry["generated_timestamp"],
                metadata=entry,
            )
            tickets.append(ticket)

        return tickets

    def get_tickets(self) -> List[ZendeskTicket]:
        tickets: List[ZendeskTicket] = []

        tickets_url = f"https://{self._subdomain}.zendesk.com/api/v2/tickets"
        response = requests.get(tickets_url, auth=self._auth)
        if response.status_code == 200:
            tickets_in_response: List[dict] = response.json()["tickets"]
        else:
            message = (
                f"Tickets could not be acquired from url: {tickets_url}"
                + f"status {response.status_code}"
            )
            raise RuntimeError(message)

        for entry in tickets_in_response:
            ticket = ZendeskTicket(
                id=entry["id"],
                subject=entry["subject"],
                description=entry["description"],
                generated_ts=entry["generated_timestamp"],
                metadata=entry,
            )
            tickets.append(ticket)

        return tickets

    def get_article_attachments(self, article_id: str):
        """
        Handles article attachments such as images and stores them as UTF-8 encoded bytes.
        """

        # Construct the URL to retrieve attachments for a specific article
        article_attachment_url = (
            f"https://{self._subdomain}.zendesk.com/api/v2/help_center/articles/"
            f"{article_id}/attachments"
        )
        # Send the GET request to retrieve the attachments
        response = requests.get(article_attachment_url, auth=self._auth)

        if response.status_code == 200:
            # Parse the response and extract attachment information
            attachments_in_response: List[dict] = response.json().get("article_attachments", [])

            # Prepare a list to store attachment information
            attachments = []

            for attachment in attachments_in_response:
                attachment_data = {
                    "id": attachment["id"],
                    "file_name": attachment["file_name"],
                    "content_type": attachment["content_type"],
                    "size": attachment["size"],
                    "url": attachment["url"],
                    "content_url": attachment["content_url"],
                }
                attachments.append(attachment_data)

                # Download the attachment using the URL
                attachment_url = attachment["content_url"]
                download_response = requests.get(attachment_url, auth=self._auth)

                if download_response.status_code == 200:
                    # Encode the file content as base64 bytes
                    encoded_content = base64.b64encode(download_response.content).decode("utf-8")
                    logger.debug(f"Encoded attachment {attachment['file_name']} successfully.")

                    # You can store the encoded content as part of the attachment data
                    encoded_string_injection = (
                        f'data:{attachment_data["content_type"]};base64,{encoded_content}'
                    )
                    attachment_data["encoded_content"] = encoded_string_injection

                else:
                    logger.error(f"Failed to download attachment {attachment['file_name']}.")

            return attachments
        else:
            message = (
                f"Attachments could not be acquired from url: {article_attachment_url} "
                + f"status {response.status_code}"
            )
            raise RuntimeError(message)

    async def get_article_attachments_async(self, article_id: str):
        """
        Handles article attachments such as images and stores them as UTF-8 encoded bytes.
        """
        article_attachment_url = (
            f"https://{self._subdomain}.zendesk.com/api/v2/help_center/"
            f"articles/{article_id}/attachments"
        )

        async with httpx.AsyncClient() as client:
            response = await client.get(article_attachment_url, auth=self._auth)

            if response.status_code == 200:
                attachments_in_response: List[Dict] = response.json().get("article_attachments", [])
                attachments = []

                for attachment in attachments_in_response:
                    attachment_data = {
                        "id": attachment["id"],
                        "file_name": attachment["file_name"],
                        "content_type": attachment["content_type"],
                        "size": attachment["size"],
                        "url": attachment["url"],
                        "content_url": attachment["content_url"],
                    }

                    download_response = await client.get(attachment["content_url"], auth=self._auth)

                    if download_response.status_code == 200:
                        encoded_content = base64.b64encode(download_response.content).decode(
                            "utf-8"
                        )
                        attachment_data["encoded_content"] = (
                            f"data:{attachment_data['content_type']};base64,{encoded_content}"
                        )
                    else:
                        logger.error(f"Failed to download attachment {attachment['file_name']}.")

                    attachments.append(attachment_data)

                return attachments
            else:
                message = (
                    f"Attachments could not be acquired from URL: {article_attachment_url} "
                    f"Status {response.status_code}"
                )
                raise RuntimeError(message)
