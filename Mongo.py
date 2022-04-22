from pymongo import MongoClient
from random import randint
#Step 1: Connect to MongoDB - Note: Change connection string as needed where "mongodb://youruser:password@instanceip:port/"
client = MongoClient("mongodb://root:8421@192.168.10.180:27017/")
db=client.sampledata
#Step 2: Create sample data
names = ['Kitchen','Animal','State', 'Tastey', 'Big','City','Fish', 'Pizza','Goat', 'Salty','Sandwich','Lazy', 'Fun']
company_type = ['LLC','Inc','Company','Corporation', 'Brotherhood', 'Conclave', 'Commonwealth']
company_cuisine = ['Pizza', 'Bar Food', 'Fast Food', 'Italian', 'Soy', 'Mexican', 'American', 'Sushi Bar', 'Vegetarian']
for x in range(1, 5001):
    reviews = {
        'name' : names[randint(0, (len(names)-1))] + ' ' + names[randint(0, (len(names)-1))]  + ' ' + company_type[randint(0, (len(company_type)-1))],
        'rating' : randint(1, 15),
        'cuisine' : company_cuisine[randint(0, (len(company_cuisine)-1))]
    }
    #Step 3: Insert business object directly into MongoDB via insert_one
    result=db.reviews.insert_one(reviews)
    #Step 4: Print to the console the ObjectID of the new document
    print('Created {0} of 5000 as {1}'.format(x,result.inserted_id))
#Step 5: Tell us that you are done
print('finished creating 5000 business reviews')
