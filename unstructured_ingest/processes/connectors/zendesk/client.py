from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, AsyncGenerator, Literal, Optional, Union

from pydantic import BaseModel, Field, HttpUrl

from unstructured_ingest.errors_v2 import ProviderError, RateLimitError, UserAuthError, UserError
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.utils.string_and_date_utils import fix_unescaped_unicode

if TYPE_CHECKING:
    from httpx import AsyncClient, Client


class Attachment(BaseModel):
    # https://developer.zendesk.com/api-reference/ticketing/tickets/ticket-attachments/#json-format
    content_type: Optional[str] = None


class Via(BaseModel):
    # https://developer.zendesk.com/documentation/ticketing/reference-guides/via-object-reference/
    channel: Union[int, str]
    source: dict = Field(default_factory=dict)


class ZendeskComment(BaseModel):
    # https://developer.zendesk.com/api-reference/ticketing/tickets/ticket_comments/#json-format
    attachments: list[Attachment] = Field(default_factory=list)
    audit_id: Optional[int] = None
    author_id: Optional[int] = None
    body: Optional[str] = None
    created_at: Optional[datetime] = None
    html_body: Optional[str] = None
    id: Optional[int] = None
    metadata: Optional[dict] = None
    plain_body: Optional[str] = None
    public: Optional[bool] = None
    comment_type: Literal["Comment", "VoiceComment"] = Field(alias="type")
    uploads: list[str] = Field(default_factory=list)
    via: Optional[Via] = None

    def as_text(self) -> str:
        all_data = self.model_dump()
        filtered_data = {
            k: v
            for k, v in all_data.items()
            if k in ["id", "author_id", "body", "created_at"] and v is not None
        }
        return "".join(
            [f"{v}\n" for v in ["comment"] + [f"{k}: {v}" for k, v in filtered_data.items()]]
        )


class ZendeskTicket(BaseModel):
    # https://developer.zendesk.com/api-reference/ticketing/tickets/tickets/#json-format
    allow_attachments: bool = True
    allow_channelback: bool = True
    assignee_email: Optional[str] = None
    assignee_id: Optional[int] = None
    attribute_value_ids: list[int] = Field(default_factory=list)
    brand_id: Optional[int] = None
    collaborator_ids: list[int] = Field(default_factory=list)
    collaborators: list[Union[int, str, dict[str, str]]] = Field(default_factory=list)
    comment: Optional[ZendeskComment] = None
    created_at: Optional[datetime] = None
    custom_fields: list[dict[str, Any]] = Field(default_factory=list)
    custom_status_id: Optional[int] = None
    description: Optional[str] = None
    due_at: Optional[datetime] = None
    email_cc_ids: list[int] = Field(default_factory=list)
    email_ccs: list[dict[str, str]] = Field(default_factory=list)
    external_id: Optional[str] = None
    follower_ids: list[int] = Field(default_factory=list)
    followers: list[dict[str, str]] = Field(default_factory=list)
    followup_ids: list[int] = Field(default_factory=list)
    forum_topic_id: Optional[int] = None
    from_messaging_channel: bool
    generated_timestamp: Optional[datetime] = None
    group_id: Optional[int] = None
    has_incidents: bool = False
    id: Optional[int] = None
    is_public: bool = False
    macro_id: Optional[int] = None
    macro_ids: list[int] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    organization_id: Optional[int] = None
    priority: Optional[Literal["urgent", "high", "normal", "low"]] = None
    problem_id: Optional[int] = None
    raw_subject: Optional[str] = None
    recipient: Optional[str] = None
    requester: dict[str, str] = Field(default_factory=dict)
    requester_id: int
    safe_update: Optional[bool] = None
    satisfaction_rating: Optional[Union[str, dict[str, Any]]] = None
    sharing_agreement_ids: list[int] = Field(default_factory=list)
    status: Optional[Literal["new", "open", "pending", "hold", "solved", "closed"]] = None
    subject: Optional[str] = None
    submitter_id: Optional[int] = None
    tags: list[str] = Field(default_factory=list)
    ticket_form_id: Optional[int] = None
    ticket_type: Optional[Literal["problem", "incident", "question", "task"]] = Field(
        default=None, alias="type"
    )
    updated_at: Optional[datetime] = None
    updated_stamp: Optional[str] = None
    url: Optional[HttpUrl] = None
    via: Optional[Via] = None
    via_followup_source_id: Optional[int] = None
    via_id: Optional[int] = None
    voice_comment: Optional[dict] = None

    def as_text(self) -> str:
        all_data = self.model_dump()
        filtered_data = {
            k: v
            for k, v in all_data.items()
            if k in ["id", "subject", "description", "created_at"] and v is not None
        }
        return "".join(
            [f"{v}\n" for v in ["ticket"] + [f"{k}: {v}" for k, v in filtered_data.items()]]
        )


class ZendeskArticle(BaseModel):
    # https://developer.zendesk.com/api-reference/help_center/help-center-api/articles/#json-format
    author_id: Optional[int] = None
    body: Optional[str] = None
    comments_disabled: bool = False
    content_tag_ids: list[str] = Field(default_factory=list)
    created_at: Optional[datetime] = None
    draft: bool = False
    edited_at: Optional[datetime] = None
    html_url: Optional[HttpUrl] = None
    id: int
    label_names: list[str] = Field(default_factory=list)
    locale: str
    outdated: bool = False
    outdated_locales: list[str] = Field(default_factory=list)
    permission_group_id: int
    position: Optional[int] = None
    promoted: bool = False
    section_id: Optional[int] = None
    source_locale: Optional[str] = None
    title: str
    updated_at: Optional[datetime] = None
    url: Optional[HttpUrl] = None
    user_segment_id: Optional[int] = None
    user_segment_ids: list[int] = Field(default_factory=list)
    vote_count: Optional[int] = None
    vote_sum: Optional[int] = None

    def as_html(self) -> str:
        html = self.body
        if title := self.title:
            html = f"<h1>{title}</h1>{html}"
        return fix_unescaped_unicode(f"<body class='Document' >{html}</body>")


class ZendeskArticleAttachment(BaseModel):
    # https://developer.zendesk.com/api-reference/help_center/help-center-api/article_attachments/#json-format
    article_id: Optional[int] = None
    content_type: Optional[str] = None
    content_url: Optional[HttpUrl] = None
    created_at: Optional[datetime] = None
    guide_media_id: Optional[str] = None
    id: Optional[int] = None
    inline: bool = False
    locale: Optional[str] = None
    size: Optional[int] = None
    updated_at: Optional[datetime] = None
    url: Optional[HttpUrl] = None


@dataclass
class ZendeskClient:
    token: str
    subdomain: str
    email: str
    max_page_size: int = 100
    _async_client: "AsyncClient" = field(init=False, default=None)
    _client: "Client" = field(init=False, default=None)
    _base_url: str = field(init=False, default=None)

    async def __aenter__(self) -> "ZendeskClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._async_client.aclose()

    @requires_dependencies(["httpx"], extras="zendesk")
    def __post_init__(self):
        import httpx

        auth = f"{self.email}/token", self.token
        self._client = httpx.Client(auth=auth)
        self._async_client = httpx.AsyncClient(auth=auth)
        self._base_url = f"https://{self.subdomain}.zendesk.com/api/v2"

        # Run check
        try:
            url_to_check = f"{self._base_url}/groups.json"
            resp = self._client.head(url_to_check)
            resp.raise_for_status()
        except Exception as e:
            raise self.wrap_error(e=e)

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

    async def fetch_content(self, url: str, content_key: str) -> AsyncGenerator[dict, None]:
        url = f"{url}?page[size]={self.max_page_size}"
        while True:
            try:
                response = await self._async_client.get(url)
                response.raise_for_status()
            except Exception as e:
                raise self.wrap_error(e=e)

            data = response.json()
            for content in data[content_key]:
                yield content

            has_more = data.get("meta", {}).get("has_more", False)
            if not has_more:
                break

            url = data["links"]["next"]

    async def get_articles(self) -> AsyncGenerator[ZendeskArticle, None]:
        """
        Retrieves article content from Zendesk asynchronously.
        """
        article_url = f"https://{self.subdomain}.zendesk.com/api/v2/help_center/articles.json"

        try:
            async for article_dict in self.fetch_content(url=article_url, content_key="articles"):
                yield ZendeskArticle.model_validate(article_dict)
        except Exception as e:
            raise self.wrap_error(e=e)

    async def get_comments(self, ticket_id: int) -> AsyncGenerator[ZendeskComment, None]:
        comments_url = f"https://{self.subdomain}.zendesk.com/api/v2/tickets/{ticket_id}/comments"

        try:
            async for comment_dict in self.fetch_content(url=comments_url, content_key="comments"):
                yield ZendeskComment.model_validate(comment_dict)
        except Exception as e:
            raise self.wrap_error(e=e)

    async def get_tickets(self) -> AsyncGenerator[ZendeskTicket, None]:
        tickets_url = f"https://{self.subdomain}.zendesk.com/api/v2/tickets"

        try:
            async for ticket_dict in self.fetch_content(url=tickets_url, content_key="tickets"):
                yield ZendeskTicket.model_validate(ticket_dict)
        except Exception as e:
            raise self.wrap_error(e=e)

    async def get_article_attachments(
        self, article_id: int
    ) -> AsyncGenerator[ZendeskArticleAttachment, None]:
        """
        Handles article attachments such as images and stores them as UTF-8 encoded bytes.
        """
        article_attachment_url = (
            f"https://{self.subdomain}.zendesk.com/api/v2/help_center/"
            f"articles/{article_id}/attachments"
        )

        try:
            async for attachment_dict in self.fetch_content(
                url=article_attachment_url, content_key="article_attachments"
            ):
                yield ZendeskArticleAttachment.model_validate(attachment_dict)
        except Exception as e:
            raise self.wrap_error(e=e)
