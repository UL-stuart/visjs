import pandas as pd
import datetime
#from pandasql import sqldf


#session = input("Please enter session ID as an integer")

def filterSession(session):

    data = pd.read_csv("raw_loki_data_new_prod.csv")

    
    data = data.filter(items=['id', 
                        'datetime', 
                        'player', 
                        'scenario', 
                        'level', 
                        'session_id', 
                        'message'])
    
    data['date'] = pd.to_datetime(data['datetime']).dt.strftime('%Y-%m-%d')

    data['datetime'] = pd.to_datetime(data['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')


    if str(session) != "null":
        sessionData = data.query("session_id == " + str(session))
    else: sessionData = data


    json = sessionData.to_json(orient = "records")
    with open('sessions.json', 'w', encoding='utf-8') as jsonf:
        jsonf.write(json)


