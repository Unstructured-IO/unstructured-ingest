import base64
from dataclasses import dataclass
from typing import Dict, List

from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.errors import ProviderError, RateLimitError, UserAuthError, UserError
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

    @requires_dependencies(["httpx"], extras="zendesk")
    def __init__(self, token: str, subdomain: str, email: str):
        import httpx

        # should be okay to be blocking.
        url_to_check = f"https://{subdomain}.zendesk.com/api/v2/groups.json"
        auth = f"{email}/token", token

        try:
            _ = httpx.get(url_to_check, auth=auth)
        except Exception as e:
            raise self.wrap_error(e=e)

        self._token = token
        self._subdomain = subdomain
        self._email = email
        self._auth = auth

    @requires_dependencies(["httpx"], extras="zendesk")
    def wrap_error(self, e: Exception) -> Exception:
        import httpx

        if not isinstance(e, httpx.HTTPStatusError):
            logger.error(f"unhandled exception from Zendesk client: {e}", exc_info=True)
            return e
        url = e.request.url
        response_code = e.response.status_code
        if response_code == 401:
            logger.error(
                f"Failed to connect via auth,"
                f"{url} using zendesk response, status code {response_code}"
            )
            return UserAuthError(e)
        if response_code == 429:
            logger.error(
                f"Failed to connect via rate limits"
                f"{url} using zendesk response, status code {response_code}"
            )
            return RateLimitError(e)
        if 400 <= response_code < 500:
            logger.error(
                f"Failed to connect to {url} using zendesk response, status code {response_code}"
            )
            return UserError(e)
        if response_code > 500:
            logger.error(
                f"Failed to connect to {url} using zendesk response, status code {response_code}"
            )
            return ProviderError(e)
        logger.error(f"unhandled http status error from Zendesk client: {e}", exc_info=True)
        return e

    @requires_dependencies(["httpx"], extras="zendesk")
    async def get_articles_async(self) -> List[ZendeskArticle]:
        """
        Retrieves article content from Zendesk asynchronously.
        """
        import httpx

        articles: List[ZendeskArticle] = []

        article_url = f"https://{self._subdomain}.zendesk.com/api/v2/help_center/articles.json"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(article_url, auth=self._auth)
                response.raise_for_status()
        except Exception as e:
            raise self.wrap_error(e=e)

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

    @requires_dependencies(["httpx"], extras="zendesk")
    async def get_comments_async(self, ticket_id: int) -> List["Comment"]:
        import httpx

        comments_url = f"https://{self._subdomain}.zendesk.com/api/v2/tickets/{ticket_id}/comments"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(comments_url, auth=self._auth)
                response.raise_for_status()
        except Exception as e:
            raise self.wrap_error(e=e)

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

    @requires_dependencies(["httpx"], extras="zendesk")
    def get_users(self) -> List[dict]:
        import httpx

        users: List[dict] = []

        users_url = f"https://{self._subdomain}.zendesk.com/api/v2/users"
        try:
            response = httpx.get(users_url, auth=self._auth)
            response.raise_for_status()
        except Exception as e:
            raise self.wrap_error(e=e)

        users_in_response: List[dict] = response.json()["users"]
        users = users_in_response

        return users

    @requires_dependencies(["httpx"], extras="zendesk")
    async def get_tickets_async(self) -> List["ZendeskTicket"]:
        import httpx

        tickets: List["ZendeskTicket"] = []
        tickets_url = f"https://{self._subdomain}.zendesk.com/api/v2/tickets"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(tickets_url, auth=self._auth)
                response.raise_for_status()
        except Exception as e:
            raise self.wrap_error(e=e)

        tickets_in_response: List[dict] = response.json()["tickets"]

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

    @requires_dependencies(["httpx"], extras="zendesk")
    async def get_article_attachments_async(self, article_id: str):
        """
        Handles article attachments such as images and stores them as UTF-8 encoded bytes.
        """
        import httpx

        article_attachment_url = (
            f"https://{self._subdomain}.zendesk.com/api/v2/help_center/"
            f"articles/{article_id}/attachments"
        )

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(article_attachment_url, auth=self._auth)
                response.raise_for_status()
        except Exception as e:
            raise self.wrap_error(e=e)

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

            try:
                async with httpx.AsyncClient() as client:
                    download_response = await client.get(attachment["content_url"], auth=self._auth)
                    download_response.raise_for_status()
            except Exception as e:
                raise self.wrap_error(e=e)

            encoded_content = base64.b64encode(download_response.content).decode("utf-8")
            attachment_data["encoded_content"] = (
                f"data:{attachment_data['content_type']};base64,{encoded_content}"
            )

            attachments.append(attachment_data)

        return attachments
