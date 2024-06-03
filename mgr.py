from flask import Flask, request, jsonify, Blueprint
import mysql.connector
import pandas as pd
from mysql.connector import Error
from datetime import datetime, timedelta
import os

app = Flask(__name__)

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


def get_current_month_date_range():
    today = datetime.today()
    first_day_of_month = today.replace(day=1)
    next_month = today.replace(day=28) + timedelta(days=4)
    last_day_of_month = next_month - timedelta(days=next_month.day)
    return first_day_of_month.strftime('%Y-%m-%d'), last_day_of_month.strftime('%Y-%m-%d')

def execute_query(connection, tabledpma, tablemerchant):
    start_date, end_date = get_current_month_date_range()
    query = f"""
        SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''));

        WITH cte AS (
            SELECT 
                m.MNAME AS Merchant_Name,
                CONCAT(d.Acquirer_Bank_Code, d.Terminal_Code) AS Terminal_Number,
                SUM(CASE WHEN d.Response_Type = 'A' THEN d.Purchase_Amount ELSE 0 END) AS Approved_Amount,
                SUM(CASE WHEN d.Transaction_Code = 'SLR' AND d.Response_Type = 'C' THEN d.Purchase_Amount ELSE 0 END) AS Reversal_Amount,
                SUM(CASE WHEN d.Response_Type = 'A' THEN d.Purchase_Amount ELSE 0 END) - 
                SUM(CASE WHEN d.Transaction_Code = 'SLR' AND d.Response_Type = 'C' THEN d.Purchase_Amount ELSE 0 END) AS Net_Amount,
                m.data_2 AS MDR_FLT,
                CAST(m.data_3 AS DECIMAL) / 100 AS MDR_FLT_Amount,
                SUM(CASE WHEN d.Response_Type = 'A' THEN 1 ELSE 0 END) AS Approved_Count,
                SUM(CASE WHEN d.Transaction_Code = 'SLR' AND d.Response_Type = 'C' THEN 1 ELSE 0 END) AS Reversal_Count,
                SUM(CASE WHEN d.Response_Type = 'A' THEN 1 ELSE 0 END) - 
                SUM(CASE WHEN d.Transaction_Code = 'SLR' AND d.Response_Type = 'C' THEN 1 ELSE 0 END) AS Net_Count,
                SUM(CASE WHEN d.Acquirer_Bank_Code = d.Issuer_Bank_Code THEN d.Purchase_Amount ELSE 0 END) AS On_US_Amount,
                SUM(CASE WHEN d.Acquirer_Bank_Code = d.Issuer_Bank_Code THEN d.Purchase_Amount ELSE 0 END) AS Off_US_Amount,
                SUM(d.POS_Merchant_Aggregator_Share) AS POS_Merchant_Aggregator_Share,
                m.data_4 AS MGR_Amount,
                m.DATE AS Date_Installed
            FROM {tabledpma} d
            LEFT JOIN {tablemerchant} m
            ON CONCAT(d.Acquirer_Bank_Code, d.Terminal_Code) = m.TID
            WHERE STR_TO_DATE(d.Transaction_Date, '%m%d%Y') BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY m.MNAME, CONCAT(d.Acquirer_Bank_Code, d.Terminal_Code), m.created_at
        )
        SELECT
            Merchant_Name,
            Terminal_Number,
            FORMAT(Approved_Amount, 2) AS Approved_Amount,
            FORMAT(Reversal_Amount, 2) AS Reversal_Amount,
            FORMAT(Net_Amount, 2) AS Net_Amount,
            MDR_FLT,
            FORMAT(MDR_FLT_Amount, 2) AS MDR_FLT_Amount,
            Approved_Count,
            Reversal_Count,
            Net_Count,
            FORMAT(Net_Count * 10, 2) AS MDR_FLT_Amount_For_Sharing,
            FORMAT(On_US_Amount, 2) AS On_US_Amount,
            FORMAT(Off_US_Amount, 2) AS Off_US_Amount,
            FORMAT((Net_Count * 10) * 10 / 2 / 10, 2) AS FLT_Share_Amount,
            FORMAT((Net_Count * 10) * 10 / 2 / 10, 2) AS TSI_Share_Amount,
            FORMAT(((Net_Count * 10) * 10 / 2) + ((Net_Count * 10) * 10 / 2), 2) AS Revenue,
            MGR_Amount,
            FORMAT(
                CASE WHEN MGR_Amount - ((Net_Count * 10) * 10 / 2 / 10) < 0
                    THEN 0.0 
                    ELSE MGR_Amount - ((Net_Count * 10) * 10 / 2 / 10) END, 2
            ) AS Additional_Amount,
            Date_Installed
        FROM cte;
    """
    
    cursor = connection.cursor()
    for result in cursor.execute(query, multi=True):
        if result.with_rows:
            columns = result.column_names
            rows = result.fetchall()
    return columns, rows

def save_to_excel(columns, rows, file_path):
    df = pd.DataFrame(rows, columns=columns)
    df.to_excel(file_path, index=False)

@app.route('/generate-report', methods=['POST'])
def generate_report():
    data = request.json
    tabledpma = data.get('tabledpma')
    tablemerchant = data.get('tablemerchant')

    if not tabledpma or not tablemerchant:
        return jsonify({"error": "Missing table names in the request body"}), 400

    connection = create_connection()
    if not connection:
        return jsonify({"error": "Failed to create a database connection"}), 500

    try:
        columns, rows = execute_query(connection, tabledpma, tablemerchant)
        file_path = os.path.join(os.getcwd(), 'MGR Report (APRIL 2024)PVP_2.xlsx')
        save_to_excel(columns, rows, file_path)
        return jsonify({"message": f"Report saved to {file_path}"})
    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if connection.is_connected():
            connection.close()

@app.route('/upload', methods=['POST'])
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

if __name__ == "__main__":
    app.run(debug=True)
