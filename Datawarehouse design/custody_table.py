import csv
import json
import pyodbc

def upload_db(connection, cursor, query, values):#to load data in the database
    cursor.execute(query, values)
    connection.commit()

def json_mapping(line ,json_mappings, columns):#to compute the crime_gravity
    crime_gravity=1
    for  i, map in enumerate(json_mappings):
        crime_gravity*=map[line[columns[i]]]
    return crime_gravity




#open connection to the DB
server = 'tcp:lds.di.unipi.it' 
database = 'Group_ID_14_DB' 
username = 'Group_ID_14' 
password = 'L5WDFF33' 
connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()


sql= 'INSERT INTO Custody(custody_id, participant_id, gun_id, geo_id, incident_id, date_id, crime_gravity) VALUES(?,?,?,?,?,?,?)'








json_files = ['dict_partecipant_age.json', 'dict_partecipant_status.json', 'dict_partecipant_type.json']
mappings = []
for file in json_files:
    with open(file, 'r') as map:
        mappings.append(json.load(map))



def create_custody_file(police_file, participant_file, gun_file, geo_file, date_file):#custody file is the last one executed and uses data from the other tables created
    # read the data from the CSV files
    def read_csv(filename):
        with open(filename, 'r') as file:
            reader = csv.DictReader(file)
            return list(reader)

    police_data = read_csv(police_file)
    participant_data = read_csv(participant_file)
    gun_data = read_csv(gun_file)
    geo_data = read_csv(geo_file)
    date_data = read_csv(date_file)

    # create dictionaries for each already uploaded table
    participant_dict = {}
    for participant_entry in participant_data:
        key = (participant_entry['age_group'], participant_entry['gender'], participant_entry['status'], participant_entry['type'])
        participant_dict[key] = participant_entry

    gun_dict = {}
    for gun_entry in gun_data:
        key = (gun_entry['is_stolen'], gun_entry['gun_type'])
        gun_dict[key] = gun_entry

    geo_dict = {}
    for geo_entry in geo_data:
        key = (geo_entry['latitude'], geo_entry['longitude'])
        geo_dict[key] = geo_entry
        
    date_dict = {}
    for date_entry in date_data:
        key = (date_entry['date_id'])
        date_dict[key] = date_entry

    with open('custody.csv', 'w', newline='') as file:
        fieldnames = ['custody_id', 'participant_id', 'gun_id', 'geo_id', 'date_id', 'incident_id', 'crime_gravity']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()

        custody_set = set()

        for police_entry in police_data:
            custody_id = police_entry['custody_id']

            if custody_id not in custody_set:
                custody_set.add(custody_id)

                # Participant matching
                participant_key = (police_entry['participant_age_group'], police_entry['participant_gender'], police_entry['participant_status'], police_entry['participant_type'])
                matched_participant = participant_dict.get(participant_key)

                # Gun matching
                gun_key = (police_entry['gun_stolen'], police_entry['gun_type'])
                matched_gun = gun_dict.get(gun_key)

                # Geographic matching
                geo_key = (police_entry['latitude'], police_entry['longitude'])
                matched_geo = geo_dict.get(geo_key)

                # Date matching                
                date_key = (police_entry['date_fk'])
                matched_date = date_dict.get(date_key)

                if matched_participant and matched_date and matched_gun and matched_geo:#if matched we compute the crime_gravity and write the row in the new file
                    crime_gravity =json_mapping(matched_participant , mappings, ['age_group', 'status', 'type'])

                    
                    writer.writerow({
                        'custody_id': custody_id,
                        'participant_id': matched_participant['participant_id'],
                        'gun_id': matched_gun['gun_id'],
                        'geo_id': matched_geo['geo_id'],
                        'date_id': matched_date['date_id'],
                        'incident_id': police_entry['incident_id'],
                        'crime_gravity': crime_gravity
                    })


# call the function with the respective file names
create_custody_file('Police.csv', 'Participant.csv', 'Gun.csv', 'Geography.csv', 'Date.csv')



with open('custody.csv', 'r', newline='') as custody:
    reader = csv.DictReader(custody)
    for row in (reader):
        upload_db(cnxn, cursor, sql, (row['custody_id'], row['participant_id'], row['gun_id'], row['geo_id'], int(row['date_id']), row['incident_id'], row['crime_gravity']))


print('finished')
cursor.close()
cnxn.close()


