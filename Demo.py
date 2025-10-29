import mysql.connector
from mysql.connector import errorcode

try:
   
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Root@123",
        database="demo"   
    )
    print("Connection established successfully!")


    mycursor = mydb.cursor()

 
    insert_query = "INSERT INTO Emp (id, name, salary, dep) VALUES (%s, %s, %s, %s)"
    data = (9, "Rahul", 60000, "accounts")
    mycursor.execute(insert_query, data)
    mydb.commit()
    print("Record inserted successfully!")


    update_query = "UPDATE Emp SET Name='Rushali' WHERE id = 7"
    mycursor.execute(update_query, (120000, 2))
    mydb.commit()
    print(" Record updated successfully!")


    delete_query = "DELETE FROM Emp WHERE id = %s"
    mycursor.execute(delete_query, (5,))
    mydb.commit()
    print(" Record deleted successfully!")

   
    select_query = "SELECT * FROM Emp"
    mycursor.execute(select_query)
    records = mycursor.fetchall()
    print("\n Current records in Emp table:")
    for row in records:
        print(row)

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Wrong username or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print(" Database does not exist")
    else:
        print(" Error:", err)

finally:
    if 'mydb' in locals() and mydb.is_connected():
        mydb.close()
        print("\n MySQL connection closed.")
