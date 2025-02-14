"""Reddit connector for fetching posts from subreddits."""

from dataclasses import dataclass, field
from datetime import datetime
import typing as t
from pathlib import Path

from pydantic import Field, Secret

from unstructured_ingest.error import SourceConnectionError, SourceConnectionNetworkError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
)
from unstructured_ingest.v2.processes.connector_registry import SourceRegistryEntry

if t.TYPE_CHECKING:
    from praw import Reddit


CONNECTOR_TYPE = "reddit"


class RedditAccessConfig(AccessConfig):
    """Configuration for Reddit API access."""
    client_secret: str = Field(
        description="Reddit API client secret", 
        sensitive=True
    )


class RedditConnectionConfig(ConnectionConfig):
    """Configuration for connecting to Reddit."""
    access_config: Secret[RedditAccessConfig]
    client_id: str = Field(description="Reddit API client ID")
    user_agent: str = Field(description="User agent string for Reddit API")

    @requires_dependencies(["praw"], extras="reddit")
    def get_client(self) -> "Reddit":
        """Get a Reddit API client."""
        from praw import Reddit

        access_config = self.access_config.get_secret_value()
        return Reddit(
            client_id=self.client_id,
            client_secret=access_config.client_secret,
            user_agent=self.user_agent,
        )


class RedditIndexerConfig(IndexerConfig):
    """Configuration for Reddit post indexing."""
    subreddit_name: str = Field(description="Name of the subreddit to fetch posts from")
    num_posts: int = Field(description="Number of posts to fetch", gt=0)
    search_query: t.Optional[str] = Field(
        default=None,
        description="Optional search query to filter posts"
    )


@dataclass
class RedditIndexer(Indexer):
    """Indexer for Reddit posts."""
    connector_type: str = CONNECTOR_TYPE
    connection_config: RedditConnectionConfig
    index_config: RedditIndexerConfig

    def precheck(self) -> None:
        """Check if the Reddit connection is valid."""
        try:
            client = self.connection_config.get_client()
            # Try to access the subreddit to verify permissions
            subreddit = client.subreddit(self.index_config.subreddit_name)
            next(subreddit.hot(limit=1))
        except Exception as e:
            raise SourceConnectionError(f"Failed to validate connection: {e}")

    def run(self, **kwargs: t.Any) -> t.Generator[FileData, None, None]:
        """Get documents from Reddit."""
        client = self.connection_config.get_client()
        subreddit = client.subreddit(self.index_config.subreddit_name)

        if self.index_config.search_query:
            posts = subreddit.search(
                self.index_config.search_query,
                limit=self.index_config.num_posts,
            )
        else:
            posts = subreddit.hot(limit=self.index_config.num_posts)

        for post in posts:
            yield FileData(
                identifier=post.id,
                connector_type=CONNECTOR_TYPE,
                source_identifiers=SourceIdentifiers(
                    filename=f"{post.id}.md",
                    fullpath=f"{self.index_config.subreddit_name}/{post.id}.md",
                ),
                metadata=FileDataSourceMetadata(
                    date_created=str(datetime.utcfromtimestamp(post.created_utc).timestamp()),
                    date_modified=None,
                    url=post.permalink,
                    record_locator={"id": post.id},
                ),
                additional_metadata={
                    "subreddit": post.subreddit.display_name,
                    "author": str(post.author),
                    "title": post.title,
                },
            )


class RedditDownloaderConfig(DownloaderConfig):
    """Configuration for Reddit post downloading."""
    pass


@dataclass
class RedditDownloader(Downloader):
    """Downloader for Reddit posts."""
    connection_config: RedditConnectionConfig
    download_config: RedditDownloaderConfig = field(default_factory=RedditDownloaderConfig)
    connector_type: str = CONNECTOR_TYPE

    @SourceConnectionNetworkError.wrap
    def _get_post(self, post_id: str) -> t.Optional["praw.models.Submission"]:
        """Get a Reddit post using PRAW."""
        from praw import Reddit
        from praw.models import Submission

        client = self.connection_config.get_client()
        return Submission(client, post_id)

    def _check_post_exists(self, post) -> bool:
        """Check if a post exists and is accessible."""
        return (post.author != "[deleted]" or post.author is not None) and (
            post.selftext != "[deleted]" or post.selftext != "[removed]"
        )

    def run(self, file_data: FileData, **kwargs: t.Any) -> DownloadResponse:
        """Download a Reddit post."""
        download_path = self.get_download_path(file_data)
        if download_path is None:
            raise ValueError("Generated invalid download path.")

        post = self._get_post(file_data.metadata.record_locator["id"])
        if post is None:
            raise ValueError(f"Failed to retrieve post {file_data.identifier}")

        if not self._check_post_exists(post):
            raise ValueError(f"Post {file_data.identifier} was deleted or removed")

        text_to_write = f"# {post.title}\n{post.selftext}"
        download_path.parent.mkdir(parents=True, exist_ok=True)
        with open(download_path, "w", encoding="utf8") as f:
            f.write(text_to_write)

        return self.generate_download_response(file_data=file_data, download_path=download_path)


reddit_source_entry = SourceRegistryEntry(
    indexer=RedditIndexer,
    indexer_config=RedditIndexerConfig,
    downloader=RedditDownloader,
    downloader_config=RedditDownloaderConfig,
    connection_config=RedditConnectionConfig,
)