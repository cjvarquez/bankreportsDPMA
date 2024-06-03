from flask import Flask, request, jsonify
import mysql.connector
import os
import shutil
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG)

class DataUploader:
    def __init__(self, host, user, password, database, table):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = table

    def upload_data(self, file_path):
        parsed_data = []  # List to store parsed data for the response
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = connection.cursor()

            with open(file_path, 'r') as file:
                # Skip the header line
                next(file)

                for line_number, line in enumerate(file, start=1):
                    logging.debug(f"Processing line {line_number}: {line.strip()}")
                    parts = line.strip().split(':', 4)
                    parts += [''] * (5 - len(parts))

                    name, mid, tid, date, email_address = parts
                    sql = f"""INSERT INTO {self.table} (NAME, MID, TID, DATE, EMAIL) 
                              VALUES (%s, %s, %s, %s, %s)"""
                    values = (name, mid, tid, date, email_address)
                    cursor.execute(sql, values)
                    
                    parsed_data.append({
                        "name": name,
                        "mid": mid,
                        "tid": tid,
                        "date": date,
                        "email_address": email_address
                    })

            connection.commit()
            logging.info("Data uploaded successfully")
        except Exception as e:
            logging.error("Failed to upload data:", exc_info=e)
        finally:
            connection.close()
        
        return parsed_data

# Provide your database connection details
host = 'localhost'
user = 'root'
password = '12345'
database = 'banknetreports'
table = 'AUP'

uploader = DataUploader(host, user, password, database, table)

@app.route('/upload', methods=['POST'])
def upload_file():
    data = request.json
    if not data or 'file_path' not in data:
        logging.error("No file path provided")
        return jsonify({"error": "No file path provided"}), 400

    file_path = data['file_path']
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return jsonify({"error": "File not found"}), 404

    logging.info(f"Processing file: {file_path}")
    # Process the file and upload data to the database
    parsed_data = uploader.upload_data(file_path)

    logging.debug(f"Parsed data: {parsed_data}")

    # Move file to dumpfolder
    dump_folder = r'C:\Users\AppDev Guest\bankreports\dumpfolder'
    if not os.path.exists(dump_folder):
        os.makedirs(dump_folder)
    shutil.move(file_path, os.path.join(dump_folder, os.path.basename(file_path)))
    logging.info(f"File moved to {dump_folder}")

    return jsonify(parsed_data)

if __name__ == '__main__':
    app.run(debug=True)
