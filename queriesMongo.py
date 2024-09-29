from numpy import mean, size
from pymongo import MongoClient
import pprint
client = MongoClient('localhost',27017)

db = client['formulaOneDatabase']
cluster_drivers = db.drivers
cluster_constructors = db.constructors
cluster_races = db.races
cluster_results = db.results
cluster_drivers.drop_indexes()
cluster_results.drop_indexes()
cluster_races.drop_indexes()
cluster_constructors.drop_indexes()

query_disc = {'name' : {'$regex':'^C'}}

query_disc_result = cluster_constructors.find(query_disc)

for x in query_disc_result:
    pprint.pprint(x['name'])


my_query = {'name': 'Monaco Grand Prix'}

result_my_query = cluster_races.find(my_query).sort([('year', 1)])

print("First query result (Monaco Grand Prix years): ")
for x in result_my_query:
    pprint.pprint(x['year'])

explanation = result_my_query.explain()
total_docs_examined = explanation["executionStats"]["totalDocsExamined"]
execution_time_millis = explanation["executionStats"]["executionTimeMillis"]
n_returned = explanation["executionStats"]["nReturned"]

# Print the explanation
print("\nExplanation for the first query without indexes:")
print(f'Total docs examined = {total_docs_examined}')
print(f'NReturned = {n_returned}')
print(f'Total time of the query in milliseconds = {execution_time_millis}')

print("\n")

# Create an index on the 'name' field
cluster_races.create_index([('name', 1)])

# Use both indexes in the query
my_query = {'name': 'Monaco Grand Prix'}

# Explain the query with indexes
explanation = result_my_query.explain()

# Print the first query result (Monaco Grand Prix years)
print("First query result with indexes: ")

# Print the explanation for the first query
total_docs_examined = explanation["executionStats"]["totalDocsExamined"]
execution_time_millis = explanation["executionStats"]["executionTimeMillis"]
n_returned = explanation["executionStats"]["nReturned"]

print("\nExplanation for the first query with indexs:")
print(f'Total docs examined = {total_docs_examined}')
print(f'NReturned = {n_returned}')
print(f'Total time of the query in milliseconds = {execution_time_millis}')

cluster_races.drop_indexes()


print("\n")

print("---------------------------------------------------------------------------------------") 

my_query = {'$and':[{'driverId':3},{'position':1}]}
result_my_query = cluster_results.find(my_query)
print("Second query result (races ids where driver with Id 3 (Nico Roseberg) won): ")
for x in result_my_query:
    pprint.pprint(x['raceId'])
    
explanation = result_my_query.explain()
total_docs_examined = explanation["executionStats"]["totalDocsExamined"]
execution_time_millis = explanation["executionStats"]["executionTimeMillis"]
n_returned = explanation["executionStats"]["nReturned"]

# Print the explanation
print("\nExplanation for the second query without indexes:")
print(f'Total docs examined = {total_docs_examined}')
print(f'NReturnd = {n_returned}')
print(f'Total time of the query in milliseconds= {execution_time_millis}')

print("\n")

# Create indexes on the fields involved in the query conditions
cluster_results.create_index([('driverId', 1), ('position', 1)])

# Define the query
my_query = {'$and': [{'driverId': 3}, {'position': 1}]}

# Explain the query with indexes
explanation = cluster_results.find(my_query).explain()

# Print the second query result (races ids where driver with Id 3 won)
print("Second query result with indexes: ")
result_my_query = cluster_results.find(my_query)

# Print the explanation for the second query
total_docs_examined = explanation["executionStats"]["totalDocsExamined"]
execution_time_millis = explanation["executionStats"]["executionTimeMillis"]
n_returned = explanation["executionStats"]["nReturned"]

print("\nExplanation for the second query with indexes:")
print(f'Total docs examined = {total_docs_examined}')
print(f'NReturned = {n_returned}')
print(f'Total time of the query in milliseconds = {execution_time_millis}')

cluster_results.drop_indexes()

print("\n")


print("---------------------------------------------------------------------------------------") 

print("First complex query:\nQuery: Races where people starting with letter L driving for Mercedes won the race.\n")
# Complex queries


# races where people starting with letter L driving for Mercedes won the race.
pipeline = [
    {
        "$lookup": {
            "from": "drivers",
            "localField": "driverId",
            "foreignField": "driverId",
            "as": "driver"
        }
    },
    {
        "$lookup": {
            "from": "constructors",
            "localField": "constructorId",
            "foreignField": "constructorId",
            "as": "constructor"
        }
    },
    {
        "$lookup": {
            "from": "races",
            "localField": "raceId",
            "foreignField": "raceId",
            "as": "race"
        }
    },
    {
        "$unwind": "$driver"
    },
    {
        "$unwind": "$constructor"
    },
    {
        "$unwind": "$race"
    },
    {
        '$match':{
            'driver.name':{'$regex':'^L'},
            'constructor.name':'Mercedes',
            'position':1
            }
    },
    {
        "$project": {
            "_id": 0,
            "result_string": {
                "$concat": [
                    {"$toString": "$driver.name"},
                    " ended in position number ",
                    {"$toString":"$position"},
                    " at the ",
                    {"$toString": "$race.year"},
                    " ",
                    {"$toString": "$race.name"},
                    " for ",
                    {"$toString": "$constructor.name"}
                ]
            }
        }
    }
]
times_no_index = list()
for _ in range(10):
    explain_result = db.command(
        'explain', 
        {
            'aggregate': 'results', 
            'pipeline': pipeline, 
            'cursor': {}
        }, 
        verbosity='executionStats'
    )

    execution_time_millis_estimate = explain_result['stages'][-1]['executionTimeMillisEstimate']
    times_no_index.append(execution_time_millis_estimate)

print(f'Mean Total time in milliseconds for the first complex query = {mean(times_no_index)}')


cluster_results.create_index([
    ('driverId', 1),
    ('raceId', -1),
    ('constructorId', 1),
    ('resultId', 1),
    ('position', 1),
])

times_index = list()
for _ in range(10):
    explain_result = db.command(
        'explain', 
        {
            'aggregate': 'results', 
            'pipeline': pipeline, 
            'cursor': {}
        }, 
        verbosity='executionStats'
    )

    execution_time_millis_estimate = explain_result['stages'][-1]['executionTimeMillisEstimate']
    times_index.append(execution_time_millis_estimate)

result = list(cluster_results.aggregate(pipeline))
print(f'Mean Total time in milliseconds for the first complex query with indexes = {mean(times_index)}\n')


cluster_results.drop_indexes()


if result:
    print(f"{len(result)} results found. \nFirst one: ")
    print(result[0]["result_string"] + "\n")

print("---------------------------------------------------------------------------------------") 

print("Second complex query:\nMonaco Grand Prix points for Max Verstappen \n")
#Monaco Grand Prix points for Max Verstappen
pipeline = [
    {
        "$lookup": {
            "from": "drivers",
            "localField": "driverId",
            "foreignField": "driverId",
            "as": "driver"
        }
    },
    {
        "$lookup": {
            "from": "constructors",
            "localField": "constructorId",
            "foreignField": "constructorId",
            "as": "constructor"
        }
    },
    {
        "$lookup": {
            "from": "races",
            "localField": "raceId",
            "foreignField": "raceId",
            "as": "race"
        }
    },
    {
        "$unwind": "$driver"
    },
    {
        "$unwind": "$constructor"
    },
    {
        "$unwind": "$race"
    },
    {
        '$match':{
            'driver.name':'Max Verstappen',
            'constructor.name':'Red Bull',
            'race.name':'Monaco Grand Prix'
        }
    },
    {
        "$group": {
            "_id": "Max Verstappen",
            "points": {"$sum": "$points"}
        }
    }
]

times_no_index = list()
for _ in range(10):
    explain_result = db.command(
        'explain', 
        {
            'aggregate': 'results', 
            'pipeline': pipeline, 
            'cursor': {}
        }, 
        verbosity='executionStats'
    )

    execution_time_millis_estimate = explain_result['stages'][-1]['executionTimeMillisEstimate']
    times_no_index.append(execution_time_millis_estimate)

print(f'Mean Total time in milliseconds for the second complex query = {mean(times_no_index)}')

cluster_results.create_index([
    ('driverId', 1),
    ('constructorId', 1),
    ('raceId', 1),
    ('points', 1),
])


times_index = list()
for _ in range(10):
    explain_result = db.command(
        'explain', 
        {
            'aggregate': 'results', 
            'pipeline': pipeline, 
            'cursor': {}
        }, 
        verbosity='executionStats'
    )

    execution_time_millis_estimate = explain_result['stages'][-1]['executionTimeMillisEstimate']
    times_index.append(execution_time_millis_estimate)


result = list(cluster_results.aggregate(pipeline))
print(f'Mean Total time in milliseconds for the second complex query with indexes = {mean(times_index)} \n')


if result:
    print(f"Max Verstappen has accumulated {result[0]['points']} points in all Monaco Grand Prix that he has raced for Red Bull. \n")
    
    
print("---------------------------------------------------------------------------------------") 
print("UPDATE OPERATION\n")

#UPDATE OP

my_query = {'$and':[{'name':"Portuguese Grand Prix"},{'year': {'$gt': 2022}}]}
result_my_query = cluster_races.find(my_query)
print("Before the update, query result: ")
for x in result_my_query:
    pprint.pprint(x)
    
print("\n")   

# Consulta para encontrar um "Portuguese Grand Prix" com ano igual a 2023
query1 = {'name': 'Portuguese Grand Prix', 'year': 2023}
result1 = cluster_races.find_one(query1)

# Consulta para encontrar um "Portuguese Grand Prix" com ano igual a 1958
query2 = {'name': 'Portuguese Grand Prix', 'year': 1958}
result2 = cluster_races.find_one(query2)

if result1:
    # Atualiza para 1958 se o ano é 2023
    update_query = {'_id': result1['_id']}
    update_operation = {'$set': {'year': 1958}}
    cluster_races.update_one(update_query, update_operation)

elif result2:
    # e atualiza para 2023 se estiver a 1958
    update_query = {'_id': result2['_id']}
    update_operation = {'$set': {'year': 2023}}
    cluster_races.update_one(update_query, update_operation)

my_query_after = {'$and': [{'name': "Portuguese Grand Prix"}, {'year': {'$gt': 2022}}]}
result_my_query_after = list(cluster_races.find(my_query_after))

print("After the update, query result: ")
for x in result_my_query_after:
    pprint.pprint(x)
    
print("\n")
print("INSERT OPERATION\n")
    
#INSERT OP

# Consulta para encontrar o maior raceId existente
max_race_id_query = {'name': 'Portuguese Grand Prix'}
max_race_id_result = cluster_races.find().sort('raceId', -1).limit(1)

max_race_id = 1000  # Valor padrão se não houver registros anteriores
max_race_id_list = list(max_race_id_result)
if max_race_id_list:
    max_race_id = max_race_id_list[0]['raceId']

# Novo raceId para o próximo elemento
new_race_id = max_race_id + 1

# Inserir novo elemento
new_race = {
    'name': 'Portuguese Grand Prix',
    'date': '2024-01-10',
    'year': 2024,
    'round': 1,
    'raceId': new_race_id
}
new_driver_id = list(cluster_drivers.find().sort('driverId',-1).limit(1))[0]['driverId']+1
new_driver = {
    'driverId':new_driver_id,
    'driverRef':'gmp',
    'name':'Goncalo Miguel Prazeres',
    'dob':'2002-09-22',
    'nationality':'Portuguese'
}
new_constructor_id = list(cluster_constructors.find().sort('constructorId',-1).limit(1))[0]['constructorId']+1
new_constructor = {
    'constructorId': new_constructor_id,
    'name': 'FCUL',
    'nationality':'Portuguese'
}
new_result={
    'resultId':list(cluster_results.find().sort('resultId',-1).limit(1))[0]['resultId']+1,
    'raceId': new_race_id,
    'driverId':new_driver_id,
    'constructorId':new_constructor_id,
    'grid':20,
    'position':1,
    'points':25
}

#Races where goncalo miguel prazeres ended first
pipeline = [
    {
        "$lookup": {
            "from": "drivers",
            "localField": "driverId",
            "foreignField": "driverId",
            "as": "driver"
        }
    },
    {
        "$lookup": {
            "from": "constructors",
            "localField": "constructorId",
            "foreignField": "constructorId",
            "as": "constructor"
        }
    },
    {
        "$lookup": {
            "from": "races",
            "localField": "raceId",
            "foreignField": "raceId",
            "as": "race"
        }
    },
    {
        "$unwind": "$driver"
    },
    {
        "$unwind": "$constructor"
    },
    {
        "$unwind": "$race"
    },
    {
        '$match':{
            'driver.name':'Goncalo Miguel Prazeres',
            'position':1
            }
    },
    {
        "$project": {
            "_id": 0,
            "result_string": {
                "$concat": [
                    {"$toString": "$driver.name"},
                    " ended in position number ",
                    {"$toString":"$position"},
                    " at the ",
                    {"$toString": "$race.year"},
                    " ",
                    {"$toString": "$race.name"},
                    " for ",
                    {"$toString": "$constructor.name"},
                    " starting at grid ",
                    {"$toString":"$grid"}

                ]
            }
        }
    }
]

#before inserting

result = list(cluster_results.aggregate(pipeline))
print("Before the insertion, query result:")
if result:
    print(f"{len(result)} results found. First one: ")
    print(result[0]["result_string"] + "\n")
print("\n")

cluster_races.insert_one(new_race)
cluster_drivers.insert_one(new_driver)
cluster_constructors.insert_one(new_constructor)
cluster_results.insert_one(new_result)


result = list(cluster_results.aggregate(pipeline))
print("After the insertion, query result:")
if result:
    print(f"{len(result)} results found. First one: ")
    print(result[0]["result_string"] + "\n")
    
print("\n")
    
#ELIMINAR O NOVO ELEMENTO PARA QUE NÂO HAJA PROBLEMAS AO CORRER
delete_criteria = {
    'name': 'Portuguese Grand Prix',
    'year': 2024,
    'round': 1
}
cluster_races.delete_one(delete_criteria)
cluster_drivers.delete_one({'name':'Goncalo Miguel Prazeres'})
cluster_constructors.delete_one(new_constructor)
cluster_results.delete_one(new_result)

result = list(cluster_results.aggregate(pipeline))
print("After deletion, query result:")
if result:
    print(f"{len(result)} results found. First one: ")
    print(result[0]["result_string"] + "\n")
    


 