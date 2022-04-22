import pyodbc
import threading
import mysql.connector

def test():
    con = None
    try:
        con = mysql.connector.connect(user='albertqa', password='84218421',
                              host='192.168.10.212',
                              port = '4000',
                              database='albertqa_test', connection_timeout=7200)
    except pyodbc.DatabaseError as e:
        print(e)
        return
    print('connected\n')
    cur = con.cursor()
    for j in range(10):
        cur.execute('select * from test_huge_book where book_id = ' + str(j).format(j))
        print('select * from test_huge_book where book_id = ' + str(j) + ' /* {} */'.format(j))
        jepa = cur.fetchall()
       #print('№ {}, Thread = {}, res {}'.format(j, threading.get_ident(), jepa))
        # print('\n')
        #time.sleep(20000)

    cur.close()
    con.close()
    print('disconnected\n')
threadCount = 1
threads = []
for a in range(threadCount):
    t = threading.Thread(target=test)
    threads.append(t)
    t.start()
for b in threads:
    b.join()
print('Completed\n')