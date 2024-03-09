import csv
import pyodbc


def upload_db(connection, cursor, query, values):
    cursor.execute(query, values)
    connection.commit()


server = 'tcp:lds.di.unipi.it' 
database = 'Group_ID_14_DB' 
username = 'Group_ID_14' 
password = 'L5WDFF33' 
connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()



sql= 'INSERT INTO Gun(Gun_id,is_stolen,gun_type) VALUES(?,?,?)'


with open('Police.csv', 'r', newline='') as input_csv, open('Gun.csv', 'w', newline='') as output_csv:
    csvreader = csv.DictReader(input_csv)
    new_fieldnames = ['gun_id', 'is_stolen', 'gun_type'] #add the columns to the csv
    csvwriter = csv.DictWriter(output_csv, fieldnames=new_fieldnames)
    csvwriter.writeheader()


    unique_rows=set()
    id=0
    for row in csvreader:
        stolen = row['gun_stolen']
        type= row['gun_type']
        row_tuple=(stolen, type)
        if row_tuple not in unique_rows:#used to do not have duplicated rows
            unique_rows.add(row_tuple)
            id+=1
            upload_db(cnxn, cursor, sql, (id, stolen, type))
            csvwriter.writerow({'gun_id': id, 'is_stolen': stolen, 'gun_type': type})

cursor.close()
cnxn.close()

    