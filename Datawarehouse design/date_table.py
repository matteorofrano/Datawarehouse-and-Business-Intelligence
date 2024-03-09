import csv
import math
from datetime import datetime
import xml.etree.ElementTree as ET
import pyodbc


def upload_db(connection, cursor, query, values):
    cursor.execute(query, values)
    connection.commit()


def date_features(dict_row, date):
    date_components = datetime.strptime(date, "%Y-%m-%d")
    day =date_components.day
    day_of_the_week = date_components.strftime("%A")
    month =date_components.strftime("%B")
    quarter =math.ceil(date_components.month/3)
    year =date_components.year

    return day, day_of_the_week, month, quarter, year
    

# parse the XML file
xml_path="dates.xml"
tree = ET.parse(xml_path)
root = tree.getroot()

# create a dictionary to store the date values
date_dict = {}
for row in root.findall(".//row"):
    date_pk = row.find("date_pk").text
    date = row.find("date").text
    date_time=date.split(" ")
    date_dict[date_pk] = date_time[0]



#open connection to the DB
server = 'tcp:lds.di.unipi.it' 
database = 'Group_ID_14_DB' 
username = 'Group_ID_14' 
password = 'L5WDFF33' 
connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()


sql= 'INSERT INTO Date(date_id, date, day, month, quarter, year, week_day) VALUES(?,?,?,?,?,?,?)'

# read the CSV file and update it with dates from the XML
with open('Police.csv', 'r', newline='') as input_csv, open('Date.csv', 'w', newline='') as output_csv:
    csvreader = csv.DictReader(input_csv)
    new_fieldnames = ['date_id', 'date', 'day', 'day_of_the_week', 'month', 'quarter','year'] # Add the new columns to the csv
    csvwriter = csv.DictWriter(output_csv, fieldnames=new_fieldnames)
    csvwriter.writeheader()
    print(new_fieldnames)

    unique_rows=set()
    for row in csvreader:
        date_id = row['date_fk']
        if date_id not in unique_rows:#used to do not have duplicated rows
        #insert the values related to the date
            date= date_dict.get(date_id, '')
            day, week_day, month, quarter, year=date_features(row, date)
            unique_rows.add(date_id)

            upload_db(cnxn, cursor, sql, (date_id, date, day, month, quarter, year, week_day))
            csvwriter.writerow({'date_id': date_id, 'date': date, 'day': day, 'day_of_the_week': week_day, 'month': month, 'quarter': quarter, 'year': year})

            
            
cursor.close()
cnxn.close()