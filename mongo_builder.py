def mongo_database_builder():
    import pandas as pd
    from pymongo import MongoClient

    # MongoDB connection details
    mongo_connection_string = 'mongodb://localhost:27017/'
    database_name = 'WorldCupDB'

    # Connect to MongoDB
    client = MongoClient(mongo_connection_string)
    db = client[database_name]

    # Read the CSV file
    worldcups = pd.read_csv('World Cup Files/WorldCups.csv')
    matches = pd.read_csv('World Cup Files/WorldCupMatches.csv')
    playerperformances = pd.read_csv('World Cup Files/WorldCupPlayers.csv')

    # Cleaning the data
    matches = matches[:852]
    matches = matches.drop_duplicates(subset=['MatchID'], keep='first')
    playerperformances = playerperformances.drop_duplicates(keep='first')


    client.drop_database(database_name)

    # List to hold the documents for MongoDB
    worldCups = []

    # Iterate through each row in the DataFrame and construct the document
    for index, row in worldcups.iterrows():
        worldCup_document = {
            "_id": row["Year"],
            "Year": row["Year"],
            "Country": row["Country"],
            "GoalsScored": row["GoalsScored"],
            "QualifiedTeams": row["QualifiedTeams"],
            "MatchesPlayed": row["MatchesPlayed"],
            "Attendance": row["Attendance"],
            "placements": {
                "Winner": row["Winner"],
                "Runners-Up": row["Runners-Up"],
                "Third": row["Third"],
                "Fourth": row["Fourth"]
            }
        }
        worldCups.append(worldCup_document)

    db.WorldCups.insert_many(worldCups)


    # List to hold the documents for MongoDB
    Matches = []
    for index, row in matches.iterrows():
        Match_document = {
            "_id": row["MatchID"],
            "MatchID": row["MatchID"],
            "RoundID": row["RoundID"],
            "Stage": row["Stage"],
            "Year": row["Year"],
            "Datetime": row["Datetime"],
            "Stadium": row["Stadium"],
            "City": row["City"],
            "Attendance": row["Attendance"],

            "Referee": row["Referee"],
            "Assistant 1": row["Assistant 1"],
            "Assistant 2": row["Assistant 2"],

            "Home Team Initials": row["Home Team Initials"],
            "Away Team Initials": row["Away Team Initials"],
            "Score_data": {
            "Home Team Goals": row["Home Team Goals"],
            "Away Team Goals": row["Away Team Goals"],
            "Half-time Home Goals": row["Half-time Home Goals"],
            "Half-time Away Goals": row["Half-time Away Goals"],
                "Win conditions": row["Win conditions"]
            }

        }
        Matches.append(Match_document)


    db.Matches.insert_many(Matches)



    # Initialize an ordered list for unique referees and assistants
    unique_Staff_member = []

    # Go through each column and add unique names to the list in the order they appear
    for column in ['Referee', 'Assistant 1', 'Assistant 2']:
        for name in matches[column]:
            if name not in unique_Staff_member:
                unique_Staff_member.append(name)

    # Initialize the list of referees documents
    referees_list = [{'_id': referee_Staff_member } for referee_Staff_member in unique_Staff_member]

    # Insert the documents into the RefereeStaff collection
    db.RefereeStaff.insert_many(referees_list)




    # Combine home and away team names and initials, and remove duplicates
    unique_teams, unique_initials = (pd.concat([matches[col1], matches[col2]]).unique().tolist() for col1, col2 in [('Home Team Name', 'Away Team Name'), ('Home Team Initials', 'Away Team Initials')])

    # Prepare the documents for MongoDB insertion
    Teams_data = [{"_id": team, "initials": initials} for team, initials in zip(unique_teams, unique_initials)]

    # This is where you would insert into MongoDB
    db.Teams.insert_many(Teams_data)



    #work for playerperformance
    playerperformance_data = []
    for index, row in playerperformances.iterrows():

        # Create a unique ID by combining Player Name, Shirt Number, and MatchID
        unique_id = f"{row['Player Name']}_{row['Shirt Number']}_{row['MatchID']}"

        document = {
            "_id": unique_id,
            "RoundID": row["RoundID"],
            "MatchID": row["MatchID"],
            "Team Initials": row["Team Initials"],
            "Coach Name": row["Coach Name"],
            "Line-up": row["Line-up"],
            "Shirt Number": row["Shirt Number"],
            "Player Name": row["Player Name"],
            "Position": row["Position"],
            "Event": row["Event"]
        }
        playerperformance_data.append(document)


    db.PlayerPerformance.insert_many(playerperformance_data)








