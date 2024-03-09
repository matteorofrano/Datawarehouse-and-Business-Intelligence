
import csv
from scipy.spatial import cKDTree
from getconti import getConti
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


sql= 'INSERT INTO Geography(geo_id, latitude, longitude, city, state, country, continent) VALUES(?,?,?,?,?,?,?)'

searcher=getConti()#create an object of getConti

full_data="full_dataset_csv.csv" #data from north american cities

with open(full_data, 'r') as csv_file:
    city_reader = csv.reader(csv_file)

    cities_info = []
    coordinates=[]

    next(city_reader)#to skip header
    for row in city_reader:
        cities_info.append([row[3], row[4], row[6]])#we keep city, state, contry
        coordinates.append([float(row[-2]), float(row[-1])])#we store latitude and longitude

kdtree = cKDTree(coordinates)#I create the kdtree
print(cities_info[:10])


with open('Police.csv', 'r', newline='') as input_csv, open('Geography.csv', 'w', newline='') as output_csv:
    csvreader = csv.DictReader(input_csv)
    new_fieldnames = ['geo_id', 'latitude', 'longitude', 'city', 'state', 'country', 'continent'] # Add the columns to the csv
    csvwriter = csv.DictWriter(output_csv, fieldnames=new_fieldnames)
    csvwriter.writeheader()

    unique_rows=set()
    id=0
    for row in csvreader:
        latitude, longitude=float(row['latitude']), float(row['longitude'])
        row_tuple = (latitude, longitude)
        if row_tuple not in unique_rows:#used to do not have duplicated rows
            unique_rows.add(row_tuple)

            id+=1
            _, index = kdtree.query([latitude, longitude])
            city = cities_info[index][0]
            state= cities_info[index][1]
            country= cities_info[index][2]
            continent= searcher.getContinents(country)

            upload_db(cnxn, cursor, sql, (id, latitude, longitude, city, state, country, continent))
            csvwriter.writerow({'geo_id': id, 'latitude': latitude, 'longitude': longitude, 'city': city, 'state': state, 'country': country, 'continent': continent})


cursor.close()
cnxn.close()






