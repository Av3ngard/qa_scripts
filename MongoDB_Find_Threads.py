from pymongo import MongoClient
import threading
#Step 1: Connect to MongoDB - Note: Change connection string as needed where "mongodb://youruser:password@instanceip:port/"
def test():
    try:
        client = MongoClient("mongodb://user:password@host:port/")
    except pymongo.errors as e:
        print(e)
        return
    print('connected\n')
    for iter in range(100):
        db=client.test
        test=db.test
        #Step 2: Find data
        for test in test.find():
            print(test)
    client.close()
    print('disconnected\n')
threadCount = 2
threads = []
for a in range(threadCount):
    t = threading.Thread(target=test)
    threads.append(t)
    t.start()
for b in threads:
    b.join()
print('Completed\n')