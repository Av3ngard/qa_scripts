# import pyodbc module
import pyodbc
import threading

# disable connection pooling
def test():
    pyodbc.pooling = False

# create connection
    connection = pyodbc.connect('DSN=Teradata')
    print('connected\n')

# create cursor
    cursor = connection.cursor()

# execute SQL statement
    for j in range(10):
        cursor.execute('select * from mock_data /* {} */'.format(j))
        #ft = cursor.fetchall()
        print('select * from mock_data /* {} */'.format(j))
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