import base64
from dataclasses import dataclass
from typing import Dict, List, Optional

import httpx

from unstructured_ingest.v2.errors import ProviderError, UserError
from unstructured_ingest.v2.logger import logger


@dataclass
class Comment:
    id: int
    author_id: str
    body: str
    parent_ticket_id: str
    metadata: dict


@dataclass
class ZendeskTicket:
    id: int
    subject: str
    description: str
    generated_ts: int
    metadata: dict

    def __lt__(self, other):
        return int(self.id) < int(other.id)


@dataclass
class ZendeskArticle:
    id: int
    author_id: str
    title: str
    content: str

    def __lt__(self, other):
        return int(self.id) < int(other.id)


class ZendeskClient:

    def __init__(self, token: str, subdomain: str, email: str):
        # should be okay to be blocking.
        url_to_check = f"https://{subdomain}.zendesk.com/api/v2/groups.json"
        auth = f"{email}/token", token

        response = httpx.get(url_to_check, auth=auth)

        self.handle_response(response, url_to_check)
        self._token = token
        self._subdomain = subdomain
        self._email = email
        self._auth = auth

    def handle_response(self, response: httpx.Response, url: Optional[str] = None):
        """
        Handles response code and raises appropriate errors based on the response status.
        """
        response_code = response.status_code

        if 400 <= response_code < 500:
            message = (
                f"Failed to connect to {url} using zendesk response, status code {response_code}"
                if url
                else f"Failed to connect using zendesk response, status code {response_code}"
            )
            raise UserError(message)

        if response_code >= 500:
            raise ProviderError(
                f"Failed to connect to {url} using zendesk response, status code {response_code}"
            )

        response.raise_for_status()

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
                    id=int(entry["id"]),
                    author_id=str(entry["author_id"]),
                    title=str(entry["title"]),
                    content=entry["body"],
                )
                for entry in articles_in_response
            ]
            return articles
        else:
            self.handle_response(response, url=article_url)

    async def get_comments_async(self, ticket_id: int) -> List["Comment"]:
        comments_url = f"https://{self._subdomain}.zendesk.com/api/v2/tickets/{ticket_id}/comments"

        async with httpx.AsyncClient() as client:
            response = await client.get(comments_url, auth=self._auth)

        if response.status_code == 200:
            return [
                Comment(
                    id=int(entry["id"]),
                    author_id=entry["author_id"],
                    body=entry["body"],
                    metadata=entry,
                    parent_ticket_id=ticket_id,
                )
                for entry in response.json()["comments"]
            ]
        else:
            self.handle_response(response)

    def get_users(self) -> List[dict]:

        users: List[dict] = []

        users_url = f"https://{self._subdomain}.zendesk.com/api/v2/users"

        response = httpx.get(users_url, auth=self._auth)

        if response.status_code == 200:
            users_in_response: List[dict] = response.json()["users"]
            users = users_in_response
        else:
            self.handle_response(response, url=users_url)

        return users

    async def get_tickets_async(self) -> List["ZendeskTicket"]:
        tickets: List["ZendeskTicket"] = []
        tickets_url = f"https://{self._subdomain}.zendesk.com/api/v2/tickets"

        async with httpx.AsyncClient() as client:
            response = await client.get(tickets_url, auth=self._auth)

        if response.status_code == 200:
            tickets_in_response: List[dict] = response.json()["tickets"]
        else:
            self.handle_response(response, url=tickets_url)

        for entry in tickets_in_response:
            ticket = ZendeskTicket(
                id=int(entry["id"]),
                subject=entry["subject"],
                description=entry["description"],
                generated_ts=entry["generated_timestamp"],
                metadata=entry,
            )
            tickets.append(ticket)

        return tickets

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
                self.handle_response(response, url=article_attachment_url)
