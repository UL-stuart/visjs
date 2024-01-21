import requests
import json
import logging
import datetime
from decimal import Decimal
import pandas as pd

logging.captureWarnings(True)


# Functions to convert datetime format to nano and visa versa
def dateTimeToNano(dateTime):
    unix_timestamp = datetime.datetime.timestamp(dateTime) * 1000000000
    return int(Decimal(unix_timestamp))


def nanoToDateTime(nano):
    dt = datetime.datetime.fromtimestamp(nano // 1000000000)
    s = dt.strftime("%Y-%m-%d %H:%M:%S")
    return s


today = datetime.date.today()
today = today.strftime("%Y%m%d")

# obtain a new OAuth 2.0 token from the authentication server
server_prod = "id.prod.uptimelabs.io"
auth_server_url = (
    "https://" + server_prod + "/auth/realms/tenants/protocol/openid-connect/token"
)
client_id = "loki"
client_secret = "!(KKZGT?p#W1ir:<"

token_req_payload = {"grant_type": "client_credentials"}

token_response = requests.post(
    auth_server_url,
    data=token_req_payload,
    verify=False,
    allow_redirects=False,
    auth=(client_id, client_secret),
)

tokens = json.loads(token_response.text)
token = tokens["access_token"]


# Request loki api data and save in list

labels = [
    "datetime",
    "player",
    "scenario",
    "level",
    "tenant",
    "session_id",
    "message",
    "neutral",
    "negative",
    "positive",
    "mixed",
    "sentiment",
]
data = []

# Query Function

#nDays = input("How many days ago was the session? (Max 30). Please enter an integer number of days: ")

def grabData(nDays = 10):
    end_date = dateTimeToNano(
        datetime.datetime.now()
    )

    start_date = dateTimeToNano(
        datetime.datetime.now() - datetime.timedelta(days = int(nDays))
    )

    print(f"Start: {nanoToDateTime(start_date)}")
    print(f"End: {nanoToDateTime(end_date)}")

    loki_prod_url = f'https://loki.prod.uptimelabs.io/loki/api/v1/query_range?direction=BACKWARD&limit=50000&query={{app%3D"slack"}}&start={start_date}&end={end_date}'#&step=300'
    api_call_headers = {"Authorization": "Bearer " + token}
    api_call_response = requests.get(
        loki_prod_url, headers=api_call_headers, verify=False
    )
    data_dict = json.loads(api_call_response.text)

    results = data_dict["data"]["result"]

    for result in results:
        for i in range(len(result["values"])):
            record = [None] * len(labels)
            value = result["values"][i]
            message = value[1]

            for j in range(len(labels)):
                if j == 0:
                    # For datetime, get nano epox time and assign to same index
                    record[j] = int(value[0])
                elif j == 6:
                    # For message, set index corresponding to 'message' to the message
                    record[j] = message
                else:
                    # For all other values, the key is the same as the label
                    attribute = labels[j]
                    if attribute not in result["stream"]:
                        record[j] = None
                    else:
                        record[j] = result["stream"][attribute]

            data.append(record)

    # Store loki data in dataframe and save as csv
    df = pd.DataFrame(data, columns=labels)
    df = df.sort_values(by=["session_id", "datetime"])
    df = df.reset_index(drop=True)
    df.to_csv("raw_loki_data_new_prod.csv", index=False)
