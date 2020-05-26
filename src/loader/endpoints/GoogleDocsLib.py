import pickle
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import io
import pandas as pd

# Takes the id and token and reads the bytes of a file
def getFileBytes(file_id, token_path="token.pickle"):
    token = pickle.load(open("token.pickle", "rb"))
    drive_service = build("drive", "v3", credentials=token)
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    return fh


# Simply saves a google file to disk
def downloadGoogleFile(file_id, out_file_path, token_path="token.pickle"):
    fh = getFileBytes(file_id, token_path)
    out_file = open(out_file_path, "wb")
    out_file.write(fh.getvalue())
    out_file.close()


# Returns it as a Pandas DF
def downloadGoogleFileDF(file_id, token_path="token.pickle"):
    fh = getFileBytes(file_id, token_path)
    s = str(fh.getvalue(), "utf-8")
    data = io.StringIO(s)
    df = pd.read_csv(data)
    return df


# Gens a token file for use in the above function if needed.
def genToken(credentials_path, out_token_path):
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
    creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open(out_token_path, "wb") as token:
        pickle.dump(creds, token)
