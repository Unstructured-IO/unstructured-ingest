from dataclasses import dataclass
from typing import List

import requests


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


class ZendeskWrapper:

    def __init__(self, token: str, subdomain: str, email: str):

        url_to_check = f"https://{subdomain}.zendesk.com/api/v2/groups.json"
        auth = f"{email}/token", token
        try:
            response = requests.get(url_to_check, auth=auth)

            if response.status_code != 200:
                raise Exception(f"Failed to connect to {url_to_check} using zendesk response")

        except Exception as e:
            raise RuntimeError(f"Failed to instantiate response: {e}") from e

        self._token = token
        self._subdomain = subdomain
        self._email = email
        self._auth = auth

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
                f"Comments for ticket id:{ticket_id} could not be acquried from url: {comments_url}"
            )

        return comments

    def get_users(self) -> List[dict]:

        users: List[dict] = []

        users_url = f"https://{self._subdomain}.zendesk.com/api/v2/users"

        response = requests.get(users_url, auth=self._auth)

        if response.status_code == 200:
            users_in_response: List[dict] = response.json()["users"]

            # TODO(convert this into a dataclass later, right now just set it as a list of dicts)
            users = users_in_response

        else:
            raise RuntimeError(f"Users could not be acquried from url: {users_url}")

        return users

    def get_tickets(self, ticket_id=None) -> List[ZendeskTicket]:
        tickets: List[ZendeskTicket] = []

        if ticket_id is None:
            tickets_url = f"https://{self._subdomain}.zendesk.com/api/v2/tickets"
            response = requests.get(tickets_url, auth=self._auth)
            if response.status_code == 200:
                tickets_in_response: List[dict] = response.json()["tickets"]

            else:
                raise RuntimeError(f"Tickets could not be acquired from url: {tickets_url}")
        else:
            # get some tickets according to id
            if isinstance(ticket_id, list):
                raise NotImplementedError("Multiple id queries not implemented yet")

            if isinstance(ticket_id, int):
                pass

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


if __name__ == "__main__":

    print("Running zendesk wrapper test")

    ENDPOINT = "unstructuredhelp.zendesk.com"
    API_TOKEN = "MjdxjP8ffGEorZfUUOOdyiNlyhMYrf1KwvHucCez"

    # # Define your credentials
    # credentials = {
    #     "email": "test@unstructured.io",
    #     "token": API_TOKEN,
    #     "subdomain": "unstructuredhelp"
    # }

    z = ZendeskWrapper(token=API_TOKEN, subdomain=ENDPOINT, email="test@unstructured.io")

    tickets = z.get_tickets()

    first_ticket = tickets[0]

    comments = z.get_comments(ticket_id=first_ticket.id)

    breakpoint()
