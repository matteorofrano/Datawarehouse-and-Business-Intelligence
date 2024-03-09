import csv
import pyodbc


def upload_db(connection, cursor, query, values):
    cursor.execute(query, values)
    connection.commit()


#open connection to the DB
server = 'tcp:lds.di.unipi.it' 
database = 'Group_ID_14_DB' 
username = 'Group_ID_14' 
password = 'L5WDFF33' 
connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()


sql='INSERT INTO Incident(incident_id) VALUES(?)'





with open('Police.csv', 'r', newline='') as input_csv, open('Incident.csv', 'w', newline='') as output_csv:
    csvreader = csv.DictReader(input_csv)
    new_fieldnames = ['incident_id'] # add the columns to the csv
    csvwriter = csv.DictWriter(output_csv, fieldnames=new_fieldnames)
    csvwriter.writeheader()


    unique_rows=set()
    for row in csvreader:
        incident_id = row['incident_id']
        if incident_id not in unique_rows:#used to do not have duplicated rows
            unique_rows.add(incident_id)
            upload_db(cnxn, cursor, sql, (incident_id))
            csvwriter.writerow({'incident_id': incident_id})


cursor.close()
cnxn.close()
    