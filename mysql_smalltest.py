import pyodbc
import threading
import mysql.connector

def test():
    con = None
    try:
        con = mysql.connector.connect(user='albertqa', password='Armor-409',
                              host='albertqa-db.mysql.database.azure.com',
                              port = '3306',
                              database='albertqa', connection_timeout=7200)
    except pyodbc.DatabaseError as e:
        print(e)
        return
    print('connected\n')
    cur = con.cursor()
    for j in range(1000):
        cur.execute('select * from huge_table where id = ' + str(j).format(j))
        print('select * from huge_table where id = ' + str(j) + ' /* {} */'.format(j))
        jepa = cur.fetchall()
       #print('№ {}, Thread = {}, res {}'.format(j, threading.get_ident(), jepa))
        # print('\n')
        #time.sleep(20000)

    cur.close()
    con.close()
    print('disconnected\n')
threadCount = 5
threads = []
for a in range(threadCount):
    t = threading.Thread(target=test)
    threads.append(t)
    t.start()
for b in threads:
    b.join()
print('Completed\n')