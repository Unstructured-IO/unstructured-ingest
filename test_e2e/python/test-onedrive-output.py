import argparse
import os
import sys

from office365.graph_client import GraphClient
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.http.http_method import HttpMethod

def get_client(client_id, client_secret, tenant_id):
    authority_url = f"https://login.microsoftonline.com/{tenant_id}"
    credentials = ClientCredential(client_id, client_secret)
    client = GraphClient(acquire_token_func=lambda: credentials.acquire_token(authority_url))
    return client

def check_file_exists(client, user_pname, destination_path):
    drive = client.users[user_pname].drive
    try:
        file_item = drive.root.get_by_path(destination_path).get().execute_query()
        print(f"File '{destination_path}' exists in OneDrive.")
        return True
    except Exception as e:
        print(f"File '{destination_path}' does not exist in OneDrive.")
        return False

def delete_file(client, user_pname, destination_path):
    drive = client.users[user_pname].drive
    try:
        file_item = drive.root.get_by_path(destination_path)
        file_item.delete_object().execute_query()
        print(f"File '{destination_path}' deleted from OneDrive.")
    except Exception as e:
        print(f"Failed to delete file '{destination_path}' from OneDrive: {e}")

def main():
    parser = argparse.ArgumentParser(description="Test OneDrive Output")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check", help="Check if file exists in OneDrive")
    check_parser.add_argument("--client-id", required=True)
    check_parser.add_argument("--client-secret", required=True)
    check_parser.add_argument("--tenant-id", required=True)
    check_parser.add_argument("--user-pname", required=True)
    check_parser.add_argument("--destination-path", required=True)

    delete_parser = subparsers.add_parser("delete", help="Delete file from OneDrive")
    delete_parser.add_argument("--client-id", required=True)
    delete_parser.add_argument("--client-secret", required=True)
    delete_parser.add_argument("--tenant-id", required=True)
    delete_parser.add_argument("--user-pname", required=True)
    delete_parser.add_argument("--destination-path", required=True)

    args = parser.parse_args()

    client = get_client(args.client_id, args.client_secret, args.tenant_id)

    if args.command == "check":
        exists = check_file_exists(client, args.user_pname, args.destination_path)
        if not exists:
            sys.exit(1)
    elif args.command == "delete":
        delete_file(client, args.user_pname, args.destination_path)

if __name__ == "__main__":
    main()