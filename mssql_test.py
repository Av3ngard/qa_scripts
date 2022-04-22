# import pyodbc module
import pyodbc
import threading


g_hostname = "192.168.10.113"
g_port = "1433"
g_database = "albertqa"
g_login = "sa"
g_password = "84218421"

# disable connection pooling
def test():
    pyodbc.pooling = False

# create connection
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.10.113,1433;DATABASE=albertqa;UID=sa;PWD=84218421', autocommit=True)
    print('connected\n')

# create cursor
    cursor = connection.cursor()

# execute SQL statement
    for j in range(5):
        cursor.execute('insert into Products select productname, manufacturer, productcount, price from Products; /* {} */'.format(j))
        #jepa = cursor.fetchall()
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