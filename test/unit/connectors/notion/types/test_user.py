import pytest
from htmlBuilder.attributes import Href
from htmlBuilder.tags import A, Div, Text

from unstructured_ingest.processes.connectors.notion.types.user import Bots, User


@pytest.fixture
def user() -> User:
    return User(
        object={},
        id="123",
        type="user",
        name="John Doe",
        avatar_url="https://example.com/user_avatar.jpg",
    )


@pytest.fixture
def bot() -> Bots:
    return Bots(
        object={},
        id="456",
        bot={},
        owner={},
        type="bot",
        workspace_name="Workspace",
        name="Bot Name",
        avatar_url="https://example.com/bot_avatar.jpg",
    )


def test_user_get_text_with_name_and_avatar_url(user: User):
    assert user.get_text() == "[John Doe](https://example.com/user_avatar.jpg)"


def test_user_get_text_with_name_only(user: User):
    user.avatar_url = None
    assert user.get_text() == "John Doe"


def test_user_get_text_with_no_name_or_avatar_url(user: User):
    user.avatar_url = None
    user.name = None
    assert user.get_text() is None


def test_user_get_html_with_name_and_avatar_url(user: User):
    html = user.get_html()
    assert isinstance(html, A)
    assert len(html.attributes) == 1
    assert isinstance(html.attributes[0], Href)
    assert html.attributes[0].value == "https://example.com/user_avatar.jpg"
    assert len(html.inner_html) == 1
    assert isinstance(html.inner_html[0], Text)
    assert html.inner_html[0].text == "John Doe"


def test_user_get_html_with_name_only(user: User):
    user.avatar_url = None
    html = user.get_html()
    assert isinstance(html, Div)
    assert len(html.inner_html) == 1
    assert isinstance(html.inner_html[0], Text)
    assert html.inner_html[0].text == "John Doe"


def test_user_get_html_with_no_name_or_avatar_url(user: User):
    user.avatar_url = None
    user.name = None
    html = user.get_html()
    assert isinstance(html, Div)
    assert len(html.inner_html) == 1
    assert isinstance(html.inner_html[0], Text)
    assert html.inner_html[0].text == ""


def test_bots_get_text_with_name_and_avatar_url(bot: Bots):
    assert bot.get_text() == "[Bot Name](https://example.com/bot_avatar.jpg)"


def test_bots_get_text_with_name_only(bot: Bots):
    bot.avatar_url = None
    assert bot.get_text() == "Bot Name"


def test_bots_get_text_with_no_name_or_avatar_url(bot: Bots):
    bot.avatar_url = None
    bot.name = None
    assert bot.get_text() is None


def test_bots_get_html_with_name_and_avatar_url(bot: Bots):
    html = bot.get_html()
    assert isinstance(html, A)
    assert len(html.attributes) == 1
    assert isinstance(html.attributes[0], Href)
    assert html.attributes[0].value == "https://example.com/bot_avatar.jpg"
    assert len(html.inner_html) == 1
    assert isinstance(html.inner_html[0], Text)
    assert html.inner_html[0].text == "Bot Name"


def test_bots_get_html_with_name_only(bot: Bots):
    bot.avatar_url = None
    html = bot.get_html()
    assert isinstance(html, Div)
    assert len(html.inner_html) == 1
    assert isinstance(html.inner_html[0], Text)
    assert html.inner_html[0].text == "Bot Name"


def test_bots_get_html_with_no_name_or_avatar_url(bot: Bots):
    bot.avatar_url = None
    bot.name = None
    html = bot.get_html()
    assert isinstance(html, Div)
    assert len(html.inner_html) == 1
    assert isinstance(html.inner_html[0], Text)
    assert html.inner_html[0].text == ""
