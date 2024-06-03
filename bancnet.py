from flask import Flask, request, jsonify
import mysql.connector

app = Flask(__name__)

class TransactionParser:
    def __init__(self, host, user, password, database, table):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = table

        self.transaction_structure = [
            (4, "Acquirer Bank Code"),
            (4, "ATM Branch Code"),
            (4, "ATM Terminal Number"),
            (4, "Issuer Bank Code"),
            (8, "Transaction Date"),
            (6, "Transaction Time"),
            (6, "BancNet Sequence Number"),
            (6, "Acquirer Trace Number"),
            (3, "Transaction Code"),
            (11, "Transaction Amount"),
            (1, "Reversal Code"),
            (1, "Acquirer Network Flag"),
            (1, "Acquirer Membership Type"),
            (1, "ATM Terminal Site Code"),
            (1, "ATM Terminal Area Code"),
            (1, "Issuer Network Flag"),
            (1, "Issuer Membership Type"),
            (1, "Channel/Device Used"),
            (2, "Account Type"),
            (11, "Convenience/Service Fee Amount"),
            (11, "Access Fee Amount"),
            (11, "Transaction Fee"),
            (11, "Acquirer Share"),
            (11, "BancNet Share")
        ]

    def insert_into_database(self, parsed_transactions):
        try:
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            cursor = connection.cursor()

            for transaction in parsed_transactions:
                primary_data, secondary_data = transaction.split('    ')
                index = 0
                parsed_transaction = {}
                for length, field in self.transaction_structure[:10]:
                    parsed_transaction[field] = primary_data[index:index + length].strip()
                    index += length

                index = 0
                for length, field in self.transaction_structure[10:]:
                    parsed_transaction[field] = secondary_data[index:index + length].strip()
                    index += length

                sql = f"""INSERT INTO {self.table} (Reversal_Code, Acquirer_Bank_Code, ATM_Branch_Code, ATM_Terminal_Number,
                                                     Issuer_Bank_Code, Transaction_Date, Transaction_Time,
                                                     BancNet_Sequence_Number, Acquirer_Trace_Number, Transaction_Code,
                                                     Transaction_Amount, Acquirer_Network_Flag, Acquirer_Membership_Type,
                                                     ATM_Terminal_Site_Code, ATM_Terminal_Area_Code, Issuer_Network_Flag,
                                                     Issuer_Membership_Type, Channel_Device_Used, Account_Type,
                                                     Convenience_Service_Fee_Amount, Access_Fee_Amount, Transaction_Fee,
                                                     Acquirer_Share, BancNet_Share)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                values = tuple(parsed_transaction[field] for _, field in self.transaction_structure)
                cursor.execute(sql, values)

            connection.commit()
            return jsonify({"message": "Successful"})
        except Exception as e:
            return jsonify({"message": f"Not successful: {e}"})
        finally:
            connection.close()

@app.route('/parse_transactions', methods=['POST'])
def parse_transactions():
    data = request.get_json()
    file_path = data.get('file_path')
    host = data.get('host')
    user = data.get('user')
    password = data.get('password')
    database = data.get('database')
    table = data.get('table')

    parser = TransactionParser(
        host=host,
        user=user,
        password=password,
        database=database,
        table=table
    )
    transactions = parser.parse_transactions()
    return parser.insert_into_database(transactions)

if __name__ == '__main__':
    app.run(debug=True)
