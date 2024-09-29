import mysql.connector
from numpy import mean
import pandas as pd
import time

mydb = mysql.connector.connect( host="localhost", user="root", password = 'RicardoeTiago2002')

mycursor = mydb.cursor()


mydb = mysql.connector.connect(
host="localhost",
user="root",
password="RicardoeTiago2002",
database="ProjetoBD"
)

mycursor = mydb.cursor()

#------------------------------------------------1st Simple query--------------------------------\n

query = "EXPLAIN SELECT year FROM races WHERE name = 'Monaco Grand Prix' ORDER BY year ASC"
print("Monaco Grand Prix years")

print("--------------------------------No Indexes--------------------------------\n")
# Query without index
time_i = time.time()
mycursor.execute(query)
results = mycursor.fetchall()
time_f = time.time()

total_time_no_index = time_f - time_i


print(f"Total time taken (No indexes): {total_time_no_index} seconds")
if results:
    for row in results:
        print("Number of Files Examined ", row[9])
        print("\n")
        print(row)
        
else:
    print("No data found.")
        

print("\n--------------------------------With Indexes--------------------------------\n")
# Create index
mycursor.execute("CREATE INDEX idx_races_name ON races(name ASC)")


# Query with index
time_i = time.time()
mycursor.execute(query)
results = mycursor.fetchall()
time_f = time.time()

total_time_with_index = time_f - time_i

print(f"Total time taken (With indexes): {total_time_with_index} seconds")
if results:
    for row in results:
        print("Number of Files Examined ", row[9])
        print("\n")
        print(row)
        
else:
    print("No data found.")

# Drop index
mycursor.execute("DROP INDEX idx_races_name ON races")


    
print("\n")


#----------------------------------------2nd Simple Query------------------------------------------------------#
print("\n--------------------------------Second simple query--------------------------------\n")
print("Races ids where driver with Id 3 (Nico Roseberg) won:")
query = """
    EXPLAIN SELECT r.raceId
    FROM results r
    WHERE r.driverId = 3 AND r.position = 1
"""

print("--------------------------------No Indexes--------------------------------\n")
time_i = time.time()
mycursor.execute(query)
time_f = time.time()
results = mycursor.fetchall()
print("\nTotal time taken without indexes (SECOND SIMPLE QUERY)", time_f - time_i)
if results:
    for row in results:
        print("Number of Files Examined ", row[9])
        print("\n")
        print(row)
else:
    print("No data found.")


    

print("\n--------------------------------With Indexes--------------------------------\n")
mycursor.execute("CREATE TABLE results_backup AS SELECT * FROM results")
mydb.commit()
mycursor.execute("CREATE INDEX index_result3 ON results (driverId ASC, position ASC)")

time_i = time.time()
mycursor.execute(query)
time_f = time.time()
results = mycursor.fetchall()

print("\nTotal time taken with indexes (SECOND SIMPLE QUERY):", time_f - time_i)
if results:
    for row in results:
        print("Number of Files Examined ", row[9])
        print("\n")
        print(row)
        
else:
    print("No data found.")
    

mycursor.execute("DROP TABLE results")
mycursor.execute("CREATE TABLE results AS SELECT* FROM results_backup")
mycursor.execute("DROP TABLE results_backup")
mydb.commit()




print("\n")


#----------------------------------------1st Complex Query------------------------------------------------------#

print("\n--------------------------------First complex query--------------------------------\n")
print("Races where people starting with letter L driving for Mercedes won the race.:")

query = """
    EXPLAIN SELECT d.name AS driver_name, ra.name AS race_name
    FROM drivers d
    JOIN results r ON d.driverId = r.driverId
    JOIN races ra ON r.raceId = ra.raceId
    JOIN constructors c ON r.constructorId = c.constructorId
    WHERE d.name LIKE '`L%`' 
        AND c.name = 'Mercedes' 
        AND r.position = 1
"""
print("--------------------------------No Indexes--------------------------------\n")

times_without_indexes = list()
for _ in range(10):
    time_i = time.time()
    mycursor.execute(query)
    time_f = time.time()
    results = mycursor.fetchall()
    times_without_indexes.append(time_f - time_i)
    

print(f"\nMean Total time taken without indexes (FIRST COMPLEX QUERY): {mean(times_without_indexes):.7f}")
if results:
    for row in results:
        print("Number of Files Examined ", row[9])
        print("\n")
        print(row)
        
else:
    print("No data found.")

print("\n--------------------------------With Indexes--------------------------------\n")
mycursor.execute("CREATE INDEX index3 ON results(driverId ASC, raceId DESC, constructorId ASC, resultId ASC, position ASC)")

times_with_indexes = list()
for _ in range(10):
    time_i = time.time()
    mycursor.execute(query)
    time_f = time.time()
    results = mycursor.fetchall()
    times_with_indexes.append(time_f - time_i)
    

print(f"\nMean Total time taken with indexes FIRST COMPLEX QUERY : {mean(times_with_indexes):.7f}") 
if results:
    for row in results:
        print("Number of Files Examined ", row[9])
        print("\n")
        print(row)
        
else:
    print("No data found.")

    
mycursor.execute("DROP INDEX index3 ON results")



print("\n----------------------------------------------------------------\n")

#--------------------------------2nd Complex Query--------------------------------#


print("\n--------------------------------Second complex query--------------------------------")
print("Monaco Grand Prix points for Max Verstappen")

query = """
    EXPLAIN SELECT SUM(r.points) AS total_points
    FROM drivers d
    JOIN results r ON d.driverId = r.driverId
    JOIN races ra ON r.raceId = ra.raceId
    JOIN constructors c ON r.constructorId = c.constructorId
    WHERE d.name = 'Max Verstappen' 
      AND ra.name = 'Monaco Grand Prix'
      AND c.name = 'Red Bull'
"""
print("--------------------------------No Indexes--------------------------------\n")
times_without_indexes = list()
for _ in range(10):
    time_i = time.time()
    mycursor.execute(query)
    time_f = time.time()
    results = mycursor.fetchall()
    times_without_indexes.append(time_f - time_i)


print(f"\nMean Total time taken without indexes (SECOND COMPLEX QUERY): {mean(times_without_indexes):.7f}")
if results:
    for row in results:
        print("Number of Files Examined ", row[9])
        print("\n")
        print(row)
        
else:
    print("No data found.")
    

print("\n--------------------------------With Indexes--------------------------------\n")    

mycursor.execute("CREATE INDEX index4 ON results(driverId ASC, constructorId ASC, raceId ASC, points ASC)")

times_with_indexes = list()
for _ in range(10):
    time_i = time.time()
    mycursor.execute(query)
    time_f = time.time()
    results = mycursor.fetchall()
    times_with_indexes.append(time_f - time_i)

mycursor.execute("DROP INDEX index4 ON results")

print(f'\nMean Total time taken with indexes (SECOND COMPLEX QUERY):  {mean(times_with_indexes):.7f}')
if results:
    for row in results:
        print("Number of Files Examined ", row[9])
        print("\n")
        print(row)
        
else:
    print("No data found.")

print("\n----------------------------------------------------------------\n")
