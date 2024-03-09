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


sql= 'INSERT INTO Participant(participant_id, age_group, gender, status, type) VALUES(?,?,?,?,?)'



with open('Police.csv', 'r', newline='') as input_csv, open('Participant.csv', 'w', newline='') as output_csv:
    csvreader = csv.DictReader(input_csv)
    new_fieldnames = ['participant_id', 'age_group', 'gender', 'status', 'type'] # add the columns to the csv
    csvwriter = csv.DictWriter(output_csv, fieldnames=new_fieldnames)
    csvwriter.writeheader()

    unique_rows=set()
    id=0
    for row in csvreader:
        age_group = row['participant_age_group']
        gender= row['participant_gender']
        status= row['participant_status']
        type= row['participant_type']

        row_tuple=(age_group, gender, status, type)
        if row_tuple not in unique_rows:#used to do not have duplicated rows
            unique_rows.add(row_tuple)
            id+=1
            upload_db(cnxn, cursor, sql, (id, age_group, gender, status, type))
            csvwriter.writerow({'participant_id': id, 'age_group': age_group, 'gender': gender, 'status': status, 'type':type})
    
cursor.close()
cnxn.close()
