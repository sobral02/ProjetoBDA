import mysql.connector

mydb = mysql.connector.connect( host="localhost", user="root", password = 'RicardoeTiago2002')
mycursor = mydb.cursor()
mycursor.execute("DROP DATABASE ProjetoBD")