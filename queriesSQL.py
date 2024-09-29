import datetime
import mysql.connector
import pandas as pd


mydb = mysql.connector.connect( host="localhost", user="root", password = 'RicardoeTiago2002')

mycursor = mydb.cursor()


mydb = mysql.connector.connect(
host="localhost",
user="root",
password="RicardoeTiago2002",
database="ProjetoBD"
)

mycursor = mydb.cursor()




#-------------------------------------------------- QUERIES -------------------------------------------------------------------#


mycursor.execute("SELECT year FROM races WHERE name = 'Monaco Grand Prix' ORDER BY year ASC")

results = mycursor.fetchall()
# Display 
if results:
    print("First query result (Monaco Grand Prix years): ")
    for row in results:
        print(row[0])
else:
    print("No data found.")
    
print("\n")

# Encontra todas os ids das corridas onde o piloto com id 3 venceu
query = """
    SELECT r.raceId
    FROM results r
    WHERE r.driverId = 3 AND r.position = 1
"""

mycursor.execute(query)
results = mycursor.fetchall()

if results:
    print("Second query result (races ids where driver with Id 3 (Nico Roseberg) won):")
    for row in results:
        print(row[0])
else:
    print("No data found.")

print("\n")

print("---------------------------------------------------------------------------------------") 

print("First complex query:\nQuery: Races where people starting with letter L driving for Mercedes won the race.\n")
# Todos os pilotos que começam com a letra L e já venceram uma corrida pela mercedes
query = """
    SELECT COUNT(*) AS driver_count
    FROM drivers d
    JOIN results r ON d.driverId = r.driverId
    JOIN races ra ON r.raceId = ra.raceId
    JOIN constructors c ON r.constructorId = c.constructorId
    WHERE d.name LIKE 'L%' 
      AND c.name = 'Mercedes' 
      AND r.position = 1
"""

mycursor.execute(query)

driver_count = mycursor.fetchone()

if driver_count:
    print("Number of drivers whose names start with 'L' in Mercedes winning races:", driver_count[0])
else:
    print("No data found.")

print("\n")

print("---------------------------------------------------------------------------------------") 

print("Second complex query:\nMonaco Grand Prix points for Max Verstappen while driving for Red Bull \n")

# Todos os pontos de Max Verstappen no GP do Mónaco
query = """
    SELECT SUM(r.points) AS total_points
    FROM drivers d
    JOIN results r ON d.driverId = r.driverId
    JOIN races ra ON r.raceId = ra.raceId
    JOIN constructors c ON r.constructorId = c.constructorId
    WHERE d.name = 'Max Verstappen' 
      AND ra.name = 'Monaco Grand Prix'
      AND c.name = 'Red Bull'
"""

# Execute the query with Verstappen's driver reference
mycursor.execute(query)

# Fetch the result
result = mycursor.fetchone()

# Display the total points earned by Verstappen in Monaco
if result:
    total_points = result[0]
    print(f"Total points earned by Verstappen in Monaco Grand Prix: {total_points}")
else:
    print("No data found.")

print("\n")

#-------------------------------------------------- QUERIES -------------------------------------------------------------------#

#-------------------------------------------------- UPDATE -------------------------------------------------------------------#

# Update para trocar a data do GP de Portugal de 1958. Se tiver em 1958, da update para 2023. Se tiver em 2023 volta para 1958

query = """
    SELECT *
    FROM races
    WHERE year = 1958 AND name = 'Portuguese Grand Prix'
"""

mycursor.execute(query)

result = mycursor.fetchone()

if result:

    update_query = """
        UPDATE races
        SET year = 2023
        WHERE year = 1958 AND name = 'Portuguese Grand Prix'
    """

    mycursor.execute(update_query)

    mydb.commit()
    
    print("Race year updated to 2023.")

else:

    update_query = """
        UPDATE races
        SET year = 1958
        WHERE year = 2023 AND name = 'Portuguese Grand Prix'
    """

    mycursor.execute(update_query)

    mydb.commit()

    print("Race year updated to 1958.")


#-------------------------------------------------- UPDATE -------------------------------------------------------------------#

#-------------------------------------------------- INSERT -------------------------------------------------------------------#

# Inserir um novo GP em Portugal
new_race_details = {
    'name': 'Portuguese Grand Prix',
    'date': '2024-01-10',
    'year': 2024,
    'round': 1,
}

insert_query = """
    INSERT INTO races (year, round, name, date)
    VALUES (%(year)s, %(round)s, %(name)s, %(date)s)
"""

mycursor.execute(insert_query, new_race_details)

mydb.commit()

mycursor.execute("SELECT * FROM races WHERE name='Portuguese Grand Prix' AND year=2024")
results = mycursor.fetchall()
for x in results:
    print(x)
mycursor.execute("DELETE FROM races WHERE name='Portuguese Grand Prix' AND year=2024")
mydb.commit()
#-------------------------------------------------- INSERT -------------------------------------------------------------------#

