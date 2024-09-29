import mysql.connector
import pandas as pd


mydb = mysql.connector.connect( host="localhost", user="root", password = 'RicardoeTiago2002')

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE ProjetoBD")

mydb = mysql.connector.connect(
host="localhost",
user="root",
password="RicardoeTiago2002",
database="ProjetoBD"
)

mycursor = mydb.cursor()

#-------------------------------------------------- RACES TABLE -------------------------------------------------------------#
tabela_races = """
    CREATE TABLE races (
        raceId INT AUTO_INCREMENT PRIMARY KEY,
        year INT NOT NULL,
        round INT NOT NULL,
        name VARCHAR(100) NOT NULL,
        date DATE NOT NULL
    )
"""

mycursor.execute(tabela_races)

columns_to_drop = [3, 6,7,8,9,10,11,12,13,14,15,16,17]
data = pd.read_csv('csvs/races.csv')
data.drop(data.columns[columns_to_drop], axis=1, inplace=True)

for row in data.itertuples(index=False):
    sql = """
        INSERT INTO races (raceId, year, round, name, date)
        VALUES (%s, %s, %s, %s, %s)
    """
    values = (row[0], row[1], row[2], row[3], row[4])  # Assuming columns are in the right order
    mycursor.execute(sql, values)


mydb.commit()



#-------------------------------------------------- RACES TABLE -------------------------------------------------------------#


#-------------------------------------------------- DRIVERS TABLE -------------------------------------------------------------#

drivers_data = pd.read_csv('csvs/drivers.csv')
drivers_data.drop(columns=["number", "code", "url"], inplace=True)
drivers_data['name'] = drivers_data['forename'] + ' ' + drivers_data['surname']
drivers_data.drop(columns=["forename", "surname"], inplace=True)

tabela_drivers = """
    CREATE TABLE drivers (
        driverId INT AUTO_INCREMENT PRIMARY KEY,
        driverRef CHAR(25) UNIQUE NOT NULL,
        name char(50) NOT NULL,
        dob DATE NOT NULL,
        nationality CHAR(30) NOT NULL
    )
"""

mycursor.execute(tabela_drivers)

for row in drivers_data.itertuples(index=False):
    sql = """
        INSERT INTO drivers (driverId, driverRef, name, dob, nationality)
        VALUES (%s, %s, %s, %s, %s)
    """
    values = (row[0], row[1], row[4], row[2], row[3]) # As colunas ficam nesta ordem após as operações
    mycursor.execute(sql, values)


mydb.commit()

#-------------------------------------------------- DRIVERS TABLE -------------------------------------------------------------#

#-------------------------------------------------- CONSTRUCTORS TABLE -------------------------------------------------------------#

constructors_data = pd.read_csv('csvs/constructors.csv')
constructors_data.drop(columns=["constructorRef", "url"], inplace=True)

tabela_constructors = """
    CREATE TABLE constructors (
        constructorId INT AUTO_INCREMENT PRIMARY KEY,
        name char(30) UNIQUE NOT NULL,
        nationality char(30) NOT NULL
    )
"""

mycursor.execute(tabela_constructors)

for row in constructors_data.itertuples(index=False):
    sql = """
        INSERT INTO constructors (constructorId, name, nationality)
        VALUES (%s, %s, %s)
    """
    values = (row[0], row[1], row[2])
    mycursor.execute(sql, values)


mydb.commit()


#-------------------------------------------------- CONSTRUCTORS TABLE -------------------------------------------------------------#



#-------------------------------------------------- RESULTS TABLE -------------------------------------------------------------#

results_data = pd.read_csv('csvs/results.csv')
results_data.drop(columns=["number", "positionText", "positionOrder", "laps", "time", "milliseconds"
                           , "fastestLap", "rank", "fastestLapTime", "fastestLapSpeed", "statusId"],
                             inplace=True)

results_data['position'] = results_data['position'].replace('\\N', None)



tabela_results = """
    CREATE TABLE results (
        resultId INT AUTO_INCREMENT PRIMARY KEY,
        raceId INT NOT NULL,
        driverId INT NOT NULL,
        constructorId INT NOT NULL,
        grid INT NOT NULL,
        position INT,
        points INT NOT NULL,
        FOREIGN KEY (driverId) REFERENCES drivers(driverId),
        FOREIGN KEY (raceId) REFERENCES races(raceId),
        FOREIGN KEY (constructorId) REFERENCES constructors(constructorId)
    )
"""

mycursor.execute(tabela_results)

for row in results_data.itertuples(index=False):
    sql = """
        INSERT INTO results (resultId, raceId, driverId, constructorId, grid, position, points)
        VALUES (%s, %s, %s,%s, %s, %s, %s)
    """

    values = (row[0], row[1], row[2],row[3], row[4], row[5],row[6])
    mycursor.execute(sql, values)


mydb.commit()


#-------------------------------------------------- RESULTS TABLE -------------------------------------------------------------#




mycursor.close()
mydb.close()