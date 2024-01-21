import pandas as pd
import emoji
import json


def parseJSON():

    with open('sessions.json', 'r') as f:
        input_json = json.load(f)

    # Serializing json
    json_object = json.dumps(input_json)
    
    # Writing to sample.json
    with open("session.js", "w") as outfile:
        outfile.write(json_object)

def structureJSON():

    with open('sessions.json', 'r') as f:
        input_json = json.load(f)

    #get unique session_ids from data set
    sessions = set(map(lambda m:
            m['session_id'], input_json))

    #filter any null sessions
    sessions = [id for id in sessions if id != None]

    #create an array of dicts. 1 for each session
    sessions = list(map(lambda s:
                    {'session_id' : s}, sessions))

    #place session data within each session dict
    for session in sessions:
        #get messages for the session
        messages = list(filter(lambda m:
                        m['session_id'] == session['session_id'] , input_json))
        
        #place messages within the session dict
        session['messages'] = messages
        
        #elevate level and date to the root of the dict
        session['level'] = messages[0]['level']
        session['date'] = messages[0]['date']
        
        #remove unnecessary keys from the message dict now they've been moved to the dict root
        session['messages'] = list(map(lambda m: {
            'id': m['id'],
            'player': m['player'],    
            'message': m['message'],
            'datetime': m['datetime']
        }, session['messages']))
        
        #determine message type
        for message in session['messages']:
            if message['player'] == "UptimeLabs":
                if ":star:" in message['message']:
                    message_type = "star"
                    print("found star")
                elif ":bulb:" in message['message']:
                    message_type = "hint"
                elif ":alarm_clock:" in message['message']:
                    message_type = "timer"
                else:
                    message_type = "message"
            else:
                message_type = "message"
            message['type'] = message_type
            message['message'] = emoji.emojize(message['message'], language='alias')
        
        #get all the message times to create min and max times for the session
        message_times = list(map(lambda m:
                            m['datetime'],session['messages']))
        session['start'] = min(message_times)
        session['end'] = max(message_times)
        
        #add a convenient list of unique players to the session for use in vis groupings
        session['players'] = list(set(map(lambda m:
                            m['player'],session['messages'])))

        #add mentions to each message
        for message in session['messages']:
            mentions = []
            for player in session['players']:
                if player.lower() in message['message'].lower():
                    mentions.append(player);
            message['mentions'] = mentions


    json_sessions = json.dumps(sessions)
    
    # Writing to sample.json
    with open("session_restructured.js", "w") as outfile:
        outfile.write("var sessions = " + json_sessions)
