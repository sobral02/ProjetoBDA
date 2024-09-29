import pandas as pd
import json
from pymongo import MongoClient

client = MongoClient('localhost',27017)

f_drivers = pd.read_csv('csvs/drivers.csv')
f_constructors = pd.read_csv('csvs/constructors.csv')
f_races = pd.read_csv('csvs/races.csv')
f_results = pd.read_csv('csvs/results.csv')


#Operations for drivers
f_drivers['forename'] = f_drivers['forename'] +" "+f_drivers['surname']
f_drivers=f_drivers.rename(columns={'forename':'name'})
f_drivers=f_drivers.drop(['number','code','url','surname'],axis=1)

f_drivers = f_drivers.to_json(orient="records")

#Operations for constructors
f_constructors=f_constructors.drop(['constructorRef','url'],axis=1)

f_constructors = f_constructors.to_json(orient="records")

#Operations for races
f_races = f_races.drop([x for x in f_races.columns if x not in ['raceId','year','round','name','date']],axis=1)

f_races = f_races.to_json(orient="records")

#Operations for results 
f_results = f_results.drop([x for x in f_results.columns if x not in ['resultId','driverId','raceId','constructorId','grid','position','points']],axis=1)
f_results['position'] = f_results['position'].replace('\\N',None)
f_results['position'] = pd.to_numeric(f_results['position'],errors='coerce')

f_results=f_results.to_json(orient="records")

data = [json.loads(f_drivers),json.loads(f_constructors),json.loads(f_races),json.loads(f_results)]

db = client['formulaOneDatabase']

# verificação da DB
database_name = 'formulaOneDatabase'
if database_name in client.list_database_names():
    print(f"The database '{database_name}' exists.\n")
else:
    print(f"The database '{database_name}' does not exist. Please create it.\n")
    
cluster_drivers = db.drivers
cluster_constructors = db.constructors
cluster_races = db.races
cluster_results = db.results

cluster_all = [cluster_drivers,cluster_constructors,cluster_races,cluster_results]

#verificação para os Clusters
for x, collection in enumerate(cluster_all):
    collection_name = collection.name  
    if collection_name in db.list_collection_names():
        print(f"The collection '{collection_name}' exists.\n")
    else:
        print(f"The collection '{collection_name}' does not exist.\n")

for x in cluster_all:
    x.drop()

for x,f in enumerate(cluster_all):
    new_result= cluster_all[x].insert_many(data[x])
    print(f"Inserted: {new_result.inserted_ids}")

client.close()