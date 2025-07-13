import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials
import json # Required for Gemini API payload
from datetime import datetime, timedelta
import os # New import for environment variables
from dotenv import load_dotenv # New import for loading .env file

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Path to your service account key file (downloaded from Google Cloud)
# This path is constructed using the filename from .env and the project directory
SERVICE_ACCOUNT_KEY_FILENAME = os.getenv('SERVICE_ACCOUNT_KEY_FILENAME')
# Construct the full path to your service account key file
# Assuming it's in a 'secrets' subdirectory within your project
SERVICE_ACCOUNT_KEY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'secrets', SERVICE_ACCOUNT_KEY_FILENAME)

# Google Sheet Name (as it appears in your Google Drive)
SPREADSHEET_NAME = 'Master DSA Sheet'

# The specific sheet (tab) name within your spreadsheet
WORKSHEET_NAME = 'Sheet1'  # Change if your sheet name is different, e.g., 'Problems'

# --- Gemini API Configuration ---
# Your Gemini API Key, loaded from environment variables
API_KEY = os.getenv('GEMINI_API_KEY')
# The Gemini model to use. User requested 'gemini-1.5-flash'.
GEMINI_MODEL = 'gemini-1.5-flash'
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={API_KEY}"


# --- File for Inputs ---
# This is the file where you'll put the problem link, topic, and date each day
INPUT_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'problems.txt')
# Path for processed problems (optional, for archiving)
PROCESSED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'processed_problems')


# --- Helper Functions ---

def get_problem_name_from_gemini(problem_link):
    """
    Uses Google Gemini API to extract the problem name from a given link.
    """
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




def process_input_file(worksheet): # worksheet is now passed as an argument
    """
    Reads problem details from INPUT_FILE_PATH, processes them, and clears the file.
    Expected format in file:
    Problem Link
    Topic Name
    First Practiced On Date (DD-MM-YYYY)
    """
    if not os.path.exists(INPUT_FILE_PATH) or os.stat(INPUT_FILE_PATH).st_size == 0:
        print(f"Input file '{INPUT_FILE_PATH}' not found or is empty. No problems to add.")
        return

    try:
        with open(INPUT_FILE_PATH, 'r') as f:
            # Read all non-empty, stripped lines
            all_lines = [line.strip() for line in f if line.strip()]

        num_lines = len(all_lines)
        if num_lines == 0:
            print(f"Input file '{INPUT_FILE_PATH}' is empty after stripping. No problems to add.")
            return

        if num_lines % 3 != 0:
            print(f"Error: Input file '{INPUT_FILE_PATH}' contains {num_lines} lines. "
                  f"Expected a multiple of 3 lines per problem (link, topic, date). "
                  f"Please check the file format.")
            return

        num_problems = num_lines // 3
        print(f"Found {num_problems} problem(s) to process from '{INPUT_FILE_PATH}'.")

        for i in range(num_problems):
            start_index = i * 3
            problem_link = all_lines[start_index]
            topic_name = all_lines[start_index + 1]
            first_practiced_date_str = all_lines[start_index + 2]

            print(f"\nProcessing problem #{i+1}: Link='{problem_link}', Topic='{topic_name}', Date='{first_practiced_date_str}'")

            try:
                first_practiced_date_obj = datetime.strptime(first_practiced_date_str, "%d-%m-%Y").date()
            except ValueError:
                print(f"Error: Invalid date format for problem #{i+1} in input file: '{first_practiced_date_str}'. Expected DD-MM-YYYY. Skipping this problem.")
                continue # Skip to the next problem

            problem_name = get_problem_name_from_gemini(problem_link)

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
            os.makedirs(PROCESSED_DIR, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archived_file_name = f"processed_problems_{timestamp}.txt"

            os.rename(INPUT_FILE_PATH, os.path.join(PROCESSED_DIR, archived_file_name))
            print(f"\nAll problems processed. Input file moved to '{os.path.join(PROCESSED_DIR, archived_file_name)}'")
        else:
            print("No problems were processed successfully, so the input file was not archived.")

    except Exception as e:
        print(f"An error occurred while processing the input file: {e}")

def main():
    # Authenticate with Google Sheets ONCE
    # Authenticate with Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    try:
        if not os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
            print(f"Error: Service account key file not found at '{SERVICE_ACCOUNT_KEY_PATH}'.")
            print("Please ensure the file is uploaded and the path in .env is correct.")
            return

        # Use gspread.service_account for authentication
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_KEY_PATH, scope)
        client = gspread.authorize(creds)

    except Exception as e:
        print(f"Error during Google Sheets authentication. Make sure '{SERVICE_ACCOUNT_KEY_PATH}' is correct and accessible.")
        print(f"Details: {e}")
        return

    try:
        spreadsheet = client.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: Spreadsheet '{SPREADSHEET_NAME}' not found.")
        print("Please ensure the spreadsheet name is correct and the service account has editor access.")
        return
    except gspread.exceptions.WorksheetNotFound:
        print(f"Error: Worksheet '{WORKSHEET_NAME}' not found in '{SPREADSHEET_NAME}'.")
        print("Please ensure the worksheet name is correct.")
        return
    except Exception as e:
        print(f"An unexpected error occurred while opening the spreadsheet: {e}")
        return

    # Pass the authenticated worksheet object to the processing function
    process_input_file(worksheet)

if __name__ == "__main__":
    main()