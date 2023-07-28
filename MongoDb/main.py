
import pymongo
import json
import logging
import pandas as pd
import certifi
myclient = pymongo.MongoClient("mongodb+srv://sairam:sairam8662@cluster0.lyahcgb.mongodb.net/?retryWrites=true&w=majority",tlsCAFile=certifi.where())

logging.basicConfig(filename="app.log",level=logging.DEBUG)
mydb = myclient["Spam_Detection"]
mycol = mydb["sms_spam_detection"]



def inserting_records():
    with open('sms_spam.json') as f:
        data=json.load(f)
    mycol.insert_many(data)
    if mycol.find( )==None:
        logging.info("no data inserted")
    else:
        logging.info('data inserted successfully')

inserting_records()
'''
def retriving_data( ):
    etxracted_data=mycol.find()
    #heart_disease=list(etxracted_data)
    #heart_df=pd.DataFrame.from_dict(heart_disease)
    #heart_df=pd.DataFrame(list(mycol.find()))
    #heart_df.to_csv('heartdiseasedata.csv',index=False,header=True)
    print(etxracted_data)
retriving_data()
'''

