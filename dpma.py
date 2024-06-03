from flask import Blueprint, jsonify, request
import os
import zipfile
import tempfile
import mysql.connector
from mysql.connector import Error

dpma_bp = Blueprint('dpma', __name__)

def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='bankreports',
            user='root',
            password='12345',
            connection_timeout=300,
            auth_plugin='mysql_native_password'
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
        return None

def insert_data(connection, table_name, data):
    try:
        cursor = connection.cursor()
        query = f"""
            INSERT INTO {table_name} (
                Datetime,
                Acquirer_Bank_Code,
                Issuer_Bank_Code,
                Merchant_Depository_Bank_Code,
                Acquirer_Trace_Number,
                BancNet_Sequence_Number,
                Issuer_Bank_Account_Number,
                Transaction_Code,
                Transaction_Date,
                Transaction_Time,
                Merchant_ID,
                Branch_Code,
                Terminal_Code,
                Purchase_Amount,
                Commission_Transaction_Fee_Amount,
                POS_Merchant_Aggregator_Share,
                Response_Type,
                Response_Code,
                Reversal_Code,
                Network_Type,
                Merchant_Name
            ) VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, data)
        connection.commit()
    except Error as e:
        print(f"Error: {e}")

@dpma_bp.route('/parse', methods=['POST'])
def parse_zip():
    data = request.json
    zip_path = data.get('zippath')
    tabledpma = data.get('tabledpma')

    if not zip_path or not tabledpma:
        return jsonify({"error": "Invalid input"}), 400

    if not os.path.exists(zip_path):
        return jsonify({"error": "ZIP file not found"}), 404

    field_indices = {
        "Acquirer Bank Code": (0, 4),
        "Issuer Bank Code": (4, 8),
        "Merchant Depository Bank Code": (8, 12),
        "Acquirer Trace Number": (12, 18),
        "BancNet Sequence Number": (18, 24),
        "Issuer Bank Account Number": (24, 40),
        "Transaction Code": (40, 43),
        "Transaction Date": (43, 51),
        "Transaction Time": (51, 57),
        "Merchant ID": (57, 72),
        "Branch Code": (72, 76),
        "Terminal Code": (76, 80),
        "Purchase Amount": (80, 95),
        "Commission / Transaction Fee Amount": (95, 110),
        "POS Merchant Aggregator Share": (110, 125),
        "Response Type": (125, 126),
        "Response Code": (126, 130),
        "Reversal Code": (130, 134),
        "Network Type": (134, 135),
        "Merchant Name": (135, 185)
    }

    parsed_data = []

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            with tempfile.TemporaryDirectory() as temp_dir:
                zip_ref.extractall(temp_dir)

                for filename in os.listdir(temp_dir):
                    file_path = os.path.join(temp_dir, filename)
                    if not os.path.isfile(file_path):
                        continue

                    try:
                        with open(file_path, 'r') as f:
                            lines = f.readlines()

                        for line in lines:
                            if line.startswith("T"):
                                break

                            parsed_line = tuple(line[start:end].strip() for field, (start, end) in field_indices.items())
                            parsed_data.append(parsed_line)

                    except IOError as e:
                        return jsonify({"error": str(e)}), 500

        if parsed_data:
            connection = create_connection()
            if connection:
                insert_data(connection, tabledpma, parsed_data)
                connection.close()

    except PermissionError as e:
        return jsonify({"error": f"PermissionError: {e}"}), 500

    return jsonify(parsed_data)
