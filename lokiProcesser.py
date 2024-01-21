import pandas as pd
import datetime
import argparse

parser = argparse.ArgumentParser(
    description="Process raw loki data",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--convert-names",
    action="store_true",
    help="convert player mentions to the names of the players",
)
parser.add_argument(
    "--convert-dates", action="store_true", help="convert nano time to datetime format"
)
args = vars(parser.parse_args())

CONVERT_NAMES = args["convert_names"]
CONVERT_DATES = args["convert_dates"]

def processRawLokiData():
    # ***Process raw loki data***
    df = pd.read_csv("raw_loki_data_new_prod.csv")

    if CONVERT_DATES:
        # Convert nano time to yyyy-mm-dd hh:mm:ss.ttt format
        df["datetime"] = df["datetime"] // 1000000
        df["datetime"] = pd.to_datetime(df["datetime"], unit="ms").astype(str)

    # Symbols and character mentions must be mapped to their correct strings
    # For player mentions, these still remain coded as different for every player
    symbol_map = {
        "&#39;": "'",
        "&#x60;": "`",
        "&#x2F;": "/",
        "&#x3D;": "=",
        "&lt;": "<",
        "&gt;": ">",
        "&amp;": "&",
        "&quot;": '"',
    }
    player_map = {
        "<@U047N6MGH19>": "@Shay",
        "<@U047N6LHU0K>": "@Daniel",
        "<@U047N90R8KU>": "@Bez",
        "<@U047KAE5JLV>": "@Tinus",
        "<@U047N6LUYJX>": "@Tanya",
        "<@U047N91SRH8>": "@Bob",
        "<@U0480TP9ZA5>": "@Hamed",
        "<@U047QNHDG6Q>": "@Jane",
    }

    messages = df["message"].tolist()
    lex_intents = []
    lex_scores = []
    formatted_messages = []
    length_of_messages = []
    questions = []
    for msg in messages:

        # Handle "Clicked Start Button" or "Clicked Start_feedback Button" with no lex score
        if "Clicked" in msg:
            if "button" in msg:
                print("Forcing Lex Intent")
                print(msg)
                msg = msg + " [n/a - 0]"

        print(msg)
        # Message contains message, lex intent and lex score so extract each
        lex_intent = msg.rsplit(" [", 1)[1].rsplit(" - ", 1)[0]
        lex_intents.append(lex_intent)

        lex_score = msg.rsplit(" [", 1)[1].rsplit(" - ", 1)[1].rsplit("]", 1)[0]
        lex_scores.append(lex_score)

        message = msg.split(": ", 1)[1].rsplit(" [", 1)[0]
        # Replace symbols and character mentions with their correct strings
        for code, character in symbol_map.items():
            message = message.replace(code, character)

        if CONVERT_NAMES:
            for code, character in player_map.items():
                message = message.replace(code, character)

            # Any unmapped character mentions must be player mentions, and replaced with '@Player'
            if "<@U0" in message:
                index1 = message.find("<@U0")
                # Player code is 14 characters long
                index2 = index1 + 14
                player_code = message[index1:index2]
                message = message.replace(player_code, "@Player")

        formatted_messages.append(message)

        message_length = len(message)
        length_of_messages.append(message_length)

        # Question only identified through presence of '?' - needs to be changed in future
        question = False
        if "?" in message:
            question = True
        questions.append(question)

    df["message"] = formatted_messages
    df.insert(7, "lex_intent", lex_intents)
    df.insert(8, "lex_score", lex_scores)
    df["length_of_message"] = length_of_messages
    df["question"] = questions
    df.index.name = "id"
    df.to_csv("raw_loki_data_new_prod.csv", index=True)