import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Read the private_key from the environment variable
private_key = os.getenv("FIREBASE_PRIVATE_KEY")
private_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if private_key is None:
    raise ValueError("PRIVATE_KEY not set in .env file")

# Create the JSON data
json_data = {
    "type": "service_account",
    "project_id": "estate-390b4",
    "private_key_id": "391ec9b01d7edf2228274a1dbb5c9126c1c503e7",
    "private_key": private_key,
    "client_email": "firebase-adminsdk-vdn1h@estate-390b4.iam.gserviceaccount.com",
    "client_id": "108941979756550798633",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-vdn1h%40estate-390b4.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

# Write JSON data to a file
with open(private_path, "w") as json_file:
    json.dump(json_data, json_file, indent=4)

print("JSON file created successfully.")