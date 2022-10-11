# import pyodbc module
import pyodbc
import threading


#g_hostname = "******"
#g_port = "****"
#g_database = "******"
#g_login = "****"
#g_password = "******"

# disable connection pooling
def test():
    pyodbc.pooling = False

# create connection
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=host,port;DATABASE=db;UID=user;PWD=password', autocommit=True)
    print('connected\n')

# create cursor
    cursor = connection.cursor()

# execute SQL statement
    for j in range(5):
        cursor.execute('insert into Products select productname, manufacturer, productcount, price from Products; /* {} */'.format(j))
        #ft = cursor.fetchall()
        print('insert into Products select productname, manufacturer, productcount, price from Products; /* {} */'.format(j))
# close cursor
        #cursor.close()

# disconnect
    connection.close()
    print('disconnected\n')

#threads
threadCount = 1
threads = []
for a in range(threadCount):
    t = threading.Thread(target=test)
    threads.append(t)
    t.start()
for b in threads:
    b.join()
print('Completed\n')