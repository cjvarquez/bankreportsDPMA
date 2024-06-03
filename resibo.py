import datetime
from prettytable import PrettyTable

def generate_report():
    # Report header information
    report_date = datetime.datetime.now().strftime("%m/%d/%Y")
    report_time = datetime.datetime.now().strftime("%I:%M:%S%p")
    report_title = "TANGENT\nDEBIT POS SETTLEMENT REPORT OF PVP POS\nFOR APR 01, 2024"
    report_page = "PAGE NO. : 1"
    report_id = "REPORT ID : DPOS14"
    
    # Data for the report
    merchants = [
        ["SUYO MULTI PURPOSE COOPERATIVE-LA UNION 2", "0.00", "329,916.00", "329,916.00", "330,546.00"],
        ["RN-CT LENDING INVESTOR CORP", "0.00", "50,300.00", "50,300.00", "50,450.00"],
        ["ALANO & SONS CREDIT CORP. - ZAMBOANGA", "63,870.00", "238,850.00", "301,920.00", "302,360.00"],
        ["ANTRECCO - LIBERTAD", "6,410.00", "232,480.00", "238,890.00", "239,380.00"],
        ["YOJIN CREDIT RESOURCE CORPORATION - LA UNION", "12,100.00", "373,100.00", "385,200.00", "386,620.00"],
        ["SUMOROY EMPLOYEES TEACHERS & COMMUNITY MPC", "0.00", "71,649.00", "71,649.00", "71,799.00"],
        ["ANTRECCO LANGIHAN SATELLITE OFFICE", "0.00", "65,230.00", "65,230.00", "65,360.00"],
        ["ZAMBOANGA CITY GE MPC", "0.00", "212,448.00", "212,448.00", "212,680.00"],
        ["G.A.A. LENDING CORPORATION", "0.00", "32,470.00", "32,470.00", "32,570.00"],
        ["RURAL BANK OF MAGDALENA (LAGUNA) INC MAIN OFFICE", "0.00", "95,340.00", "95,340.00", "95,500.00"],
        ["ZAMBOANGA CITY GENEREAL EMPLOYEES, RETIREES MPC", "0.00", "132,934.00", "132,934.00", "133,104.00"],
        ["ANTRECCO MAIN OFFICE", "0.00", "253,830.00", "253,830.00", "254,120.00"],
        ["ALANO & SONS CREDIT CORP.- BUUG", "0.00", "28,400.00", "28,400.00", "28,490.00"],
        ["CREDITLANE LENDING INVESTORS CORP.", "0.00", "6,100.00", "6,100.00", "6,110.00"],
        ["ISABELA COPY CENTER", "0.00", "6,190.00", "6,190.00", "6,200.00"]
    ]
    totals = ["TOTAL:", "82,380.00", "2,128,437.00", "2,210,817.00", "2,215,297.00"]
    
    # Create the table
    table = PrettyTable()
    table.field_names = ["Merchant Name", "AS CB and DB", "AS DB ONLY", "NetSettlement", "Transaction Amount"]
    for merchant in merchants:
        table.add_row(merchant)
    
    # Format the report
    report = (
        f"DATE   : {report_date}\n"
        f"TIME   : {report_time}\n\n"
        f"{report_title}\n"
        f"{report_page}\n"
        f"{report_id}\n\n"
        f"{table}\n"
        f"{' '.join(totals)}\n"
    )
    
    return report

# Generate the report
report = generate_report()

# Save the report to a text file
with open("report.txt", "w") as file:
    file.write(report)

print("Report saved to report.txt")
