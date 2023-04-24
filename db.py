import mysql.connector

connection=mysql.connector.connect(**{
    'host' : '127.0.0.1',
    'database' : 'zecdata' ,
    'user' : 'root',
    'port' : 3306,
    'password' : 'password'
})

mycursor = connection.cursor()
mycursor.execute("SELECT * FROM student2")
myresult = mycursor.fetchall()
for x in myresult:
  print(x)