#!/usr/bin/env python
from __future__ import print_function

from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive",
]

WORK_DIR = Path(__file__).parent


def get_creds() -> Credentials:
    creds = None
    if WORK_DIR.joinpath("token.json").exists():
        creds = Credentials.from_authorized_user_file(
            WORK_DIR / "token.json", SCOPES
        )
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                WORK_DIR / "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(WORK_DIR / "token.json", "w") as token:
            token.write(creds.to_json())
    return creds


def main():
    """Upload file with conversion
    Returns: ID of the file uploaded

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """
    # creds, _ = google.auth.default()

    creds = get_creds()

    try:
        # create drive api client
        service = build("drive", "v3", credentials=creds)

        file_metadata = {
            "name": "My Report",
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        media = MediaFileUpload(
            WORK_DIR / "report.csv", mimetype="text/csv", resumable=True
        )
        # pylint: disable=maybe-no-member
        file = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        print(f'File with ID: "{file.get("id")}" has been uploaded.')

    except HttpError as error:
        print(f"An error occurred: {error}")
        file = None

    return file.get("id")


if __name__ == "__main__":
    main()
