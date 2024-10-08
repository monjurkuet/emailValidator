import requests
import sqlite3
import logging
from contextlib import closing
from tqdm import tqdm
from multiprocessing import Pool, current_process
import json

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATABASE_PATH = 'database.db'
API_URL = "http://localhost:8080/v0/check_email"

def fetch_unchecked_emails(cursor):
    """Fetch emails that have not been checked yet."""
    cursor.execute("SELECT email FROM emails WHERE email NOT IN (SELECT email FROM checked)")
    return [row[0] for row in cursor.fetchall()]

def check_email(email):
    """Send a POST request to check the validity of the email."""
    data = {
        "to_email": email,
        "from_email": "evetyler51@gmail.com",  # Optional
        "hello_name": "gmail.com"               # Optional
    }
    
    logging.info(f"[{current_process().name}] Checking email: {email}")
    try:
        response = requests.post(API_URL, json=data)
        if response.status_code == 200:
            return email, True, json.dumps(response.json())  # Return email, is_valid, response
        else:
            logging.error(f"[{current_process().name}] Request failed for {email}: {response.status_code} - {response.text}")
            return email, False, str(response.status_code)  # Return email, is_valid, error message
    except Exception as e:
        logging.error(f"[{current_process().name}] Exception occurred for {email}: {str(e)}")
        return email, False, str(e)  # Return email, is_valid, error message

def insert_check_result(email, is_valid, response_json):
    """Insert or update the result of the email check into the checked table."""
    with closing(sqlite3.connect(DATABASE_PATH)) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("""
                INSERT OR REPLACE INTO checked (email, is_valid, response_json)
                VALUES (?, ?, ?)
            """, (email, is_valid, response_json))
            conn.commit()
            logging.info(f"Data inserted for: {email}" if is_valid else f"Failed to insert data for: {email}")

def main():
    """Main function to check emails and store results in the database."""
    with closing(sqlite3.connect(DATABASE_PATH)) as conn:
        with closing(conn.cursor()) as cursor:
            email_list = fetch_unchecked_emails(cursor)
            logging.info(f'Emails found: {len(email_list)}')

            # Create a pool of workers
            with Pool(processes=4) as pool:  # You can adjust the number of processes
                for email, is_valid, response in tqdm(pool.imap(check_email, email_list), total=len(email_list), desc='Checking emails', unit='email'):
                    # Insert result to the database immediately
                    insert_check_result(email, int(is_valid), str(response))

if __name__ == "__main__":
    main()
