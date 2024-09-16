import os
import time
import threading
from flask import Flask, request, jsonify
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import mysql.connector
from mysql.connector import Error

# Initialize Flask web application
app = Flask(__name__)

# Google Sheets API setup
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]  # Scopes for Google Sheets API access
SPREADSHEET_ID = '1TpSM7i5ZEM2OPgMHtZdZvR7Z9idkfH1qIXmyyubTOBQ'  # ID of the Google Sheets document
RANGE_NAME = 'Sheet1!A1:G1000'  # Range of data in the Google Sheet to be synced

# MySQL database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'google_sql',  # Name of the database
    'user': 'root',  # Database username
    'password': '3010',  # Database password
    'autocommit': False,  # Disable autocommit for better transaction control
}

# Function to authenticate and return Google Sheets API service
def get_google_sheets_service():
    creds = None

    # Check if token.json (containing OAuth credentials) exists
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    # If credentials are missing or expired, refresh or re-authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    # Return the authenticated Google Sheets service
    return build('sheets', 'v4', credentials=creds)

# Function to retrieve data from the Google Sheet
def get_sheet_data(service):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    return result.get('values', [])

# Function to update data in the Google Sheet
def update_sheet_data(service, values):
    body = {'values': values}
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
        valueInputOption='USER_ENTERED', body=body).execute()

# Function to establish a connection to the MySQL database and handles exception
def get_db_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    return None

# Function to fetch data from the 'car_data' table
def get_db_data(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM car_data")
    rows = cursor.fetchall()
    cursor.close()
    return rows

# Function to insert/update data in 'car_data' table
def update_db_data(connection, data):
    cursor = connection.cursor()
    for row in data:
        query = """
        INSERT INTO car_data 
        (Car_ID, Car_Name, Cylinders, Displacement, Horsepower, Weight, Origin)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Car_Name = VALUES(Car_Name),
        Cylinders = VALUES(Cylinders),
        Displacement = VALUES(Displacement),
        Horsepower = VALUES(Horsepower),
        Weight = VALUES(Weight),
        Origin = VALUES(Origin)
        """
        cursor.execute(query, row)
    connection.commit()
    cursor.close()

# Function to format Google Sheets data to be compatible with MySQL (list of tuples)
def sheet_to_db_format(sheet_data):
    if not sheet_data:
        return []
    # Assume the first row is headers
    headers = sheet_data[0]
    data = sheet_data[1:]
    return [
        [
            int(row[0]) if row[0].isdigit() else row[0],  # Car_ID
            row[1] if len(row) > 1 and row[1] != '' else None,  # Car_Name
            int(row[2]) if len(row) > 2 and row[2].isdigit() else 0,  # Cylinders
            int(row[3]) if len(row) > 3 and row[3].isdigit() else 0,  # Displacement
            int(row[4]) if len(row) > 4 and row[4].isdigit() else 0,  # Horsepower
            int(row[5]) if len(row) > 5 and row[5].isdigit() else 0,  # Weight
            row[6] if len(row) > 6 and row[6] != '' else None  # Origin
        ]
        for row in data
    ]

# Function to format MySQL data for Google Sheets (list of lists with headers)
def db_to_sheet_format(db_data):
    headers = ['Car_ID', 'Car_Name', 'Cylinders', 'Displacement', 'Horsepower', 'Weight', 'Origin']
    return [headers] + [[str(cell) for cell in row] for row in db_data]

# Function to get unprocessed changes from the database
def get_db_changes(connection):
    cursor = connection.cursor(dictionary=True)
    
    # Select unprocessed changes
    select_query = """
    SELECT * FROM changes_log
    WHERE processed = FALSE
    ORDER BY changed_at ASC
    LIMIT 100
    """
    cursor.execute(select_query)
    changes = cursor.fetchall()
    
    # Mark changes as processed
    if changes:
        change_ids = [change['id'] for change in changes]
        update_query = """
        UPDATE changes_log
        SET processed = TRUE
        WHERE id IN (%s)
        """ % ','.join(['%s'] * len(change_ids))
        cursor.execute(update_query, change_ids)
    
    connection.commit()
    cursor.close()
    return changes

# Function to apply database changes to Google Sheets
def apply_db_changes_to_sheet(service, changes):
    sheet_data = get_sheet_data(service)
    headers = sheet_data[0]
    data_dict = {row[0]: row for row in sheet_data[1:]}

    # Apply changes based on operation type
    for change in changes:
        if change['operation'] == 'DELETE':
            if str(change['car_id']) in data_dict:
                del data_dict[str(change['car_id'])]
        elif change['operation'] in ['INSERT', 'UPDATE']:
            data_dict[str(change['car_id'])] = [
                str(change['car_id']),
                change['car_name'],
                str(change['cylinders']),
                str(change['displacement']),
                str(change['horsepower']),
                str(change['weight']),
                change['origin']
            ]

    # Update the sheet with the modified data
    new_sheet_data = [headers] + list(data_dict.values())
    update_sheet_data(service, new_sheet_data)

# Main function to sync data between Google Sheets and MySQL
def sync_data():
    sheets_service = get_google_sheets_service()
    db_connection = get_db_connection()

    if not db_connection:
        print("Failed to connect to the database. Exiting.")
        return

    try:
        while True:

            # Sync data from Google Sheets to MySQL
            sheet_data = get_sheet_data(sheets_service)
            if sheet_data:
                sheet_data_formatted = sheet_to_db_format(sheet_data)
                if sheet_data_formatted:
                    print("Updating database from sheet...")
                    update_db_data(db_connection, sheet_data_formatted)

            # Sync data from MySQL to Google Sheets
            db_changes = get_db_changes(db_connection)
            if db_changes:
                print("Updating sheet from database...")
                apply_db_changes_to_sheet(sheets_service, db_changes)

            time.sleep(10)  # Sync interval (10 seconds)

    except KeyboardInterrupt:
        print("Sync stopped by user.")
        
    finally:
        if db_connection.is_connected():
            db_connection.close()

# Flask API route to update MySQL database via HTTP request
@app.route('/api/update_mysql', methods=['POST'])
def update_mysql():
    data = request.json
    sheet_name = data['sheetName']
    updates = data['updates']

    connection = get_db_connection()

    # if there is an error in connecting
    if not connection:
        return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500

    cursor = connection.cursor()

    try:
        for update in updates:
            row = update['row']
            col = update['col']
            value = update['value']

            # Map column numbers to MySQL fields
            field_map = {
                1: 'Car_ID',
                2: 'Car_Name',
                3: 'Cylinders',
                4: 'Displacement',
                5: 'Horsepower',
                6: 'Weight',
                7: 'Origin'
            }

            if col in field_map:
                field = field_map[col]
                
                # Handle different data types
                if field in ['Cylinders', 'Displacement', 'Horsepower', 'Weight']:
                    value = int(value) if value != '' else 0
                elif field == 'Car_ID':
                    value = int(value)
                elif value == '':
                    value = None

                # Update the MySQL database
                query = f"""
                    INSERT INTO car_data ({field})
                    VALUES (%s)
                    ON DUPLICATE KEY UPDATE
                    {field} = VALUES({field})
                """
                cursor.execute(query, (value,))

        connection.commit()
        return jsonify({'status': 'success', 'message': 'Database updated successfully'}), 200

    except Error as e:
        print(f"Error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Function to run Flask app
def run_flask():
    app.run(host='0.0.0.0', port=5000)

# Entry point of the script
if __name__ == "__main__":
    # Start Flask server in a separate thread
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.start()

    # Start the sync process between Google Sheets and MySQL
    sync_data()
