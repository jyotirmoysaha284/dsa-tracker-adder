import gspread
import requests
from google.cloud import secretmanager # New import for Secret Manager
from google.cloud import storage # New import for Cloud Storage
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime, timedelta


# --- Google Sheets Configuration ---
# Will be retrieved from Secret Manager
SERVICE_ACCOUNT_KEY_JSON_SECRET_NAME = os.environ.get('SERVICE_ACCOUNT_KEY_JSON_SECRET_NAME', 'dsa-tracker-service-account-key')
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT') # Automatically set in Cloud Run

# Google Sheet Name (as it appears in your Google Drive)
SPREADSHEET_NAME = os.environ.get('SPREADSHEET_NAME', 'Master DSA Sheet')

# The specific sheet (tab) name within your spreadsheet
WORKSHEET_NAME = os.environ.get('WORKSHEET_NAME', 'Sheet1')

# --- Gemini API Configuration ---
# Your Gemini API Key, loaded from Secret Manager
GEMINI_API_KEY_SECRET_NAME = os.environ.get('GEMINI_API_KEY_SECRET_NAME', 'gemini-api-key')
GEMINI_MODEL = 'gemini-1.5-flash'
# GEMINI_API_URL will be constructed after retrieving the API key

# --- Cloud Storage Configuration for problems.txt ---
GCS_BUCKET_NAME = os.environ.get('GCS_BUCKET_NAME') # e.g., 'your-dsa-tracker-bucket'
INPUT_BLOB_NAME = os.environ.get('INPUT_BLOB_NAME', 'problems.txt') # The name of the file in the GCS bucket
PROCESSED_BLOB_PREFIX = os.environ.get('PROCESSED_BLOB_PREFIX', 'processed_problems/') # Prefix for archived files

# --- Helper Functions ---

def get_secret(secret_name):
    """Fetches a secret from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error accessing secret '{secret_name}': {e}")
        raise

def get_problem_name_from_gemini(problem_link, api_key):
    """
    Uses Google Gemini API to extract the problem name from a given link.
    """
    GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={api_key}"
    print("Attempting to extract problem name using Gemini API...")
    prompt = (
        f"Extract the exact problem name from the following coding problem link. "
        f"Only return the problem name, nothing else. If it's a LeetCode problem, "
        f"ensure the name includes the problem number if present. "
        f"If it's a TakeUForward link, provide the main topic/problem title. "
        f"Link: {problem_link}"
    )

    chat_history = []

    chat_history.append({"role": "user", "parts": [{"text": prompt}]})

    payload = {
        "contents": chat_history,
        "generationConfig": {
            "temperature": 0.1,  # Lower temperature for more precise extraction
            "topP": 0.9,
            "topK": 40,
        }
    }

    try:
        response = requests.post(GEMINI_API_URL,
                                 headers={'Content-Type': 'application/json'},
                                 data=json.dumps(payload))
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        result = response.json()

        if result.get("candidates") and len(result["candidates"]) > 0 and \
                result["candidates"][0].get("content") and \
                result["candidates"][0]["content"].get("parts") and \
                len(result["candidates"][0]["content"]["parts"]) > 0:

            problem_name = result["candidates"][0]["content"]["parts"][0]["text"].strip()
            # Clean up any potential markdown or extra formatting from the LLM response
            problem_name = problem_name.replace("```", "").replace("json", "").replace("Problem Name:", "").strip()
            print(f"Problem name extracted by Gemini: '{problem_name}'")
            return problem_name
        else:
            print("Gemini API did not return a valid problem name in the expected format.")
            print(f"Gemini response: {json.dumps(result, indent=2)}")
            return "Unknown Problem (via Gemini)"

    except requests.exceptions.RequestException as e:
        print(f"Error calling Gemini API: {e}")
        return "Error Fetching Name (via Gemini)"
    except json.JSONDecodeError:
        print("Error decoding JSON response from Gemini API. Response might not be valid JSON.")
        return "Error Fetching Name (via Gemini)"
    except Exception as e:
        print(f"An unexpected error occurred with Gemini API: {e}")
        return "Error (via Gemini)"




def process_problems_from_gcs(worksheet, gemini_api_key):
    """
    Reads problem details from Cloud Storage, processes them, and archives the file.
    """
    if not GCS_BUCKET_NAME:
        print("Error: GCS_BUCKET_NAME environment variable not set. Cannot process problems from GCS.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    input_blob = bucket.blob(INPUT_BLOB_NAME)

    if not input_blob.exists():
        print(f"Input file '{INPUT_BLOB_NAME}' not found in bucket '{GCS_BUCKET_NAME}'. No problems to add.")
        return

    try:
        input_content = input_blob.download_as_text()
        all_lines = [line.strip() for line in input_content.splitlines() if line.strip()]

        num_lines = len(all_lines)
        if num_lines == 0:
            print(f"Input file '{INPUT_BLOB_NAME}' is empty. No problems to add.")
            return

        if num_lines % 3 != 0:
            print(f"Error: Input file '{INPUT_BLOB_NAME}' contains {num_lines} lines. "
                  f"Expected a multiple of 3 lines per problem (link, topic, date). "
                  f"Please check the file format.")
            return

        num_problems = num_lines // 3
        print(f"Found {num_problems} problem(s) to process from '{INPUT_BLOB_NAME}'.")

        for i in range(num_problems):
            start_index = i * 3
            problem_link = all_lines[start_index]
            topic_name = all_lines[start_index + 1]
            first_practiced_date_str = all_lines[start_index + 2]

            print(
                f"\nProcessing problem #{i + 1}: Link='{problem_link}', Topic='{topic_name}', Date='{first_practiced_date_str}'")

            try:
                first_practiced_date_obj = datetime.strptime(first_practiced_date_str, "%d-%m-%Y").date()
            except ValueError:
                print(
                    f"Error: Invalid date format for problem #{i + 1} in input file: '{first_practiced_date_str}'. Expected DD-MM-YYYY. Skipping this problem.")
                continue

            problem_name = get_problem_name_from_gemini(problem_link, gemini_api_key)

            # Calculate revision dates
            first_practiced_on_sheet_format = first_practiced_date_obj.strftime("%Y-%m-%d")
            first_revision_date = (first_practiced_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            second_revision_date = (first_practiced_date_obj + timedelta(days=1 + 3)).strftime("%Y-%m-%d")
            third_revision_date = (first_practiced_date_obj + timedelta(days=1 + 3 + 7)).strftime("%Y-%m-%d")
            fourth_revision_date = (first_practiced_date_obj + timedelta(days=1 + 3 + 7 + 15)).strftime("%Y-%m-%d")
            fifth_revision_date = (first_practiced_date_obj + timedelta(days=1 + 3 + 7 + 15 + 30)).strftime("%Y-%m-%d")

            row_data = [
                topic_name,
                problem_name,
                problem_link,
                first_practiced_on_sheet_format,
                first_revision_date,
                second_revision_date,
                third_revision_date,
                fourth_revision_date,
                fifth_revision_date
            ]

            try:
                worksheet.append_row(row_data)
                print(f"Successfully added problem '{problem_name}' to tracker.")
            except Exception as e:
                print(f"An error occurred while writing problem '{problem_name}' to the spreadsheet: {e}")
                # Decide if you want to stop or continue. For now, we'll continue.

        # --- ARCHIVING THE INPUT FILE AFTER ALL PROBLEMS ARE PROCESSED ---
        # Only archive if at least one problem was successfully processed

        if num_problems > 0:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Construct the new (archived) blob name
            processed_blob_name = f"{PROCESSED_BLOB_PREFIX}{os.path.basename(INPUT_BLOB_NAME).replace('.txt', '')}_{timestamp}.txt"

            # 1. Copy the original blob to the new (archived) location/name
            # The copy_blob method is called on the source bucket, not the blob itself.
            # It takes the source blob, the destination bucket, and the new name/path for the copy.
            new_blob = bucket.copy_blob(
                blob=input_blob,  # The original blob object
                destination_bucket=bucket,  # The destination bucket (can be the same as source for rename)
                new_name=processed_blob_name  # The desired new name/path for the copied blob
            )

            # 2. Delete the original blob
            # This completes the "move" operation.
            input_blob.delete()

            print(f"File '{INPUT_BLOB_NAME}' moved to '{new_blob.name}' in bucket '{GCS_BUCKET_NAME}'.")
        else:
            print("No problems were processed successfully, so the input file was not archived.")

    except Exception as e:
        print(f"An error occurred while processing the input file from GCS: {e}")

def main():
    print("Starting DSA Tracker Adder script...")

    # --- Retrieve secrets ---
    try:
        service_account_key_json = get_secret(SERVICE_ACCOUNT_KEY_JSON_SECRET_NAME)
        gemini_api_key = get_secret(GEMINI_API_KEY_SECRET_NAME)
        print("Secrets retrieved successfully.")
    except Exception as e:
        print(f"Failed to retrieve secrets: {e}")
        return  # Exit if secrets cannot be fetched

    # --- Write the service account key JSON to a temporary file ---
    # Cloud Run provides a /tmp directory for temporary files
    TEMP_KEY_FILE_PATH = '/tmp/temp_service_account_key.json'
    # TEMP_KEY_FILE_PATH = 'temp_service_account_key.json'
    try:
        with open(TEMP_KEY_FILE_PATH, 'w') as f:
            json.dump(json.loads(service_account_key_json), f)
        print(f"Temporary service account key file created at {TEMP_KEY_FILE_PATH}")
    except Exception as e:
        print(f"Error creating temporary key file: {e}")
        return  # Exit if key file cannot be created

    # Authenticate with Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(TEMP_KEY_FILE_PATH, scope)
        client = gspread.authorize(creds)
        print("Google Sheets authentication successful.")
    except Exception as e:
        print(f"Error during Google Sheets authentication: {e}")
        print("Please ensure the service account JSON is valid and the sheet is shared with it.")
        return

    finally:
        # Clean up the temporary key file
        if os.path.exists(TEMP_KEY_FILE_PATH):
            os.remove(TEMP_KEY_FILE_PATH)
            print(f"Temporary service account key file removed from {TEMP_KEY_FILE_PATH}")

    try:
        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        print(f"Connected to spreadsheet '{SPREADSHEET_NAME}', worksheet '{WORKSHEET_NAME}'.")
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Spreadsheet '{SPREADSHEET_NAME}' not found. Check name and service account access.")
        return
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Worksheet '{WORKSHEET_NAME}' not found in '{SPREADSHEET_NAME}'. Check name.")
        return
    except Exception as e:
        print(f"An unexpected error occurred while opening the spreadsheet: {e}")
        return

    # Process problems from GCS
    process_problems_from_gcs(worksheet, gemini_api_key)

    print("DSA Tracker Adder script finished.")

if __name__ == "__main__":
    main()