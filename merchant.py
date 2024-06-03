from flask import Blueprint, request, jsonify
import mysql.connector
import os
import shutil
import logging
from datetime import datetime

merchant_bp = Blueprint('merchant', __name__)

class DataUploader:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def upload_data(self, file_path, table):
        parsed_data = []  # List to store parsed data for the response
        batch_data = []   # List to store batch data for insertion
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
                    parts = line.strip().split(':')
                    parts += [''] * (10 - len(parts))  # Adjust the length to 10 parts

                    name, mid, tid, date, data1, data2, data3, data4, data5, email_address = parts
                    created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    values = (name, mid, tid, date, email_address, data1, data2, data3, data4, data5, created_at)
                    batch_data.append(values)

                    parsed_data.append({
                        "name": name,
                        "mid": mid,
                        "tid": tid,
                        "date": date,
                        "email_address": email_address,
                        "data1": data1,
                        "data2": data2,
                        "data3": data3,
                        "data4": data4,
                        "data5": data5,
                        "created_at": created_at
                    })

            if batch_data:
                sql = f"""INSERT INTO {table} (MNAME, MID, TID, DATE, EMAIL_ADD, DATA_1, DATA_2, DATA_3, DATA_4, DATA_5, created_at) 
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.executemany(sql, batch_data)
                connection.commit()
                logging.info("Data uploaded successfully")
        except Exception as e:
            logging.error("Failed to upload data:", exc_info=e)
        finally:
            if connection.is_connected():
                connection.close()

        return parsed_data

# Provide your database connection details
host = 'localhost'
user = 'root'
password = '12345'
database = 'bankreports'

uploader = DataUploader(host, user, password, database)

@merchant_bp.route('/upload', methods=['POST'])
def upload_file():
    data = request.json
    if not data or 'file_path' not in data or 'table' not in data:
        logging.error("No file path or table name provided")
        return jsonify({"error": "No file path or table name provided"}), 400

    file_path = data['file_path']
    table = data['table']

    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return jsonify({"error": "File not found"}), 404

    logging.info(f"Processing file: {file_path}")
    # Process the file and upload data to the database
    parsed_data = uploader.upload_data(file_path, table)

    logging.debug(f"Parsed data: {parsed_data}")

    # Move file to dumpfolder
    dump_folder = r'C:\Users\AppDev Guest\bankreports\dumpfolder'
    if not os.path.exists(dump_folder):
        os.makedirs(dump_folder)
    shutil.move(file_path, os.path.join(dump_folder, os.path.basename(file_path)))
    logging.info(f"File moved to {dump_folder}")

    return jsonify(parsed_data)
