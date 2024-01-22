#import mongo modules
from pymongo import MongoClient

#connect to the database
client = MongoClient()
db = client.WorldCupDB

def query1():
    pipeline = [
        {
            "$project": {
                "halfTimeHomeGoals": "$Score_data.Half-time Home Goals",
                "halfTimeAwayGoals": "$Score_data.Half-time Away Goals",
                "fullTimeHomeGoals": "$Score_data.Home Team Goals",
                "fullTimeAwayGoals": "$Score_data.Away Team Goals"
            }
        },
        {
            "$addFields": {
                "halfTimeResult": {
                    "$cond": {
                        "if": {"$gt": ["$halfTimeHomeGoals", "$halfTimeAwayGoals"]},
                        "then": "Home team winning",
                        "else": {
                            "$cond": {
                                "if": {"$eq": ["$halfTimeHomeGoals", "$halfTimeAwayGoals"]},
                                "then": "Drawing",
                                "else": "Away team winning"
                            }
                        }
                    }
                },
                "fullTimeResult": {
                    "$cond": {
                        "if": {"$gt": ["$fullTimeHomeGoals", "$fullTimeAwayGoals"]},
                        "then": "Home team win",
                        "else": {
                            "$cond": {
                                "if": {"$eq": ["$fullTimeHomeGoals", "$fullTimeAwayGoals"]},
                                "then": "Draw at full time",
                                "else": "Away team win"
                            }
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "halfTimeResult": "$halfTimeResult",
                    "fullTimeResult": "$fullTimeResult"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "halfTimeResult": "$_id.halfTimeResult",
                "fullTimeResult": "$_id.fullTimeResult",
                "count": 1,
                "_id": 0
            }
        }
    ]

    results = list(db.Matches.aggregate(pipeline))

    # Calculating percentages
    total_matches = sum([result['count'] for result in results])
    for result in results:
        result['percentage'] = f"{(result['count'] / total_matches) * 100:.2f}%"

    return results





def query2(team, year):
    # if year is not an int return not valid year
    if not isinstance(year, int):
        return "Not a valid year"
    # if team is not a string return not valid team
    if not isinstance(team, str):
        return "Not a valid team"
    team_initials = db.Teams.find_one({"_id": team}, {"initials": 1})["initials"]

    pipeline = [
        {
            "$match": {
                "$and": [
                    {"$or": [
                        {"Home Team Initials": team_initials}, 
                        {"Away Team Initials": team_initials}
                    ]}, 
                    {"Year": year}
                ]
            }
        },
        {
            "$lookup": {
                "from": "PlayerPerformance",
                "localField": "MatchID",
                "foreignField": "MatchID",
                "as": "captains"
            }
        },
        {
            "$unwind": "$captains"
        },
        {
            "$match": {
                "captains.Team Initials": team_initials,
                "captains.Position": {"$in": ["C", "GKC"]}
            }
        },
        {
            "$group": {
                "_id": "$captains.Player Name"
            }
        }
    ]

    result = db.Matches.aggregate(pipeline)

    captains = [doc['_id'] for doc in result]

    #check if the list is at least long 2: if not return a formatted string like "in the year the captains for team was captain"
    if len(captains) < 2:
        return f"In the year {year} the captain for {team} was {captains[0]}"
    #if the list is at least long 2 return a formatted string like "in the year the captains for team were captain1 and captain2"
    else:
        captains = " and ".join(captains)
        return f"In the year {year} the captains for {team} were {captains}"






def query3_1(team):

    # Get team initials
    team_initials_doc = db.Teams.find_one({"_id": team}, {"initials": 1})
    if not team_initials_doc:
        return f"No team found with ID {team}"

    team_initials = team_initials_doc["initials"]

    # Aggregation pipeline
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"Home Team Initials": team_initials},
                    {"Away Team Initials": team_initials}
                ]
            }
        },
        {
            "$sort": {"Attendance": 1}  # Sort by attendance in ascending order
        },
        {
            "$limit": 1
        },
        {
            "$lookup": {
                "from": "Teams",
                "localField": "Home Team Initials",
                "foreignField": "initials",
                "as": "home_team"
            }
        },
        {
            "$lookup": {
                "from": "Teams",
                "localField": "Away Team Initials",
                "foreignField": "initials",
                "as": "away_team"
            }
        },
        {
            "$addFields": {
                "opponent_team": {
                    "$cond": {
                        "if": {"$eq": ["$Home Team Initials", team_initials]},
                        "then": "$away_team",
                        "else": "$home_team"
                    }
                }
            }
        },
        {
            "$unwind": "$opponent_team"
        },
        {
            "$project": {
                "MatchID": 1,
                "Attendance": 1,
                "Stadium": 1,
                "City": 1,
                "Year": 1,
                "Stage": 1,
                "opponent_team_name": "$opponent_team._id"
            }
        }
    ]

    # Execute the aggregation pipeline
    result = list(db.Matches.aggregate(pipeline))

    # Format the output
    if result:
        match = result[0]  # Since the pipeline returns only one match

        year = str(int(match['Year']))
        stadium = match['Stadium']
        city = match['City'].rstrip()
        attendance = str(int(match['Attendance']))
        stage = match['Stage']
        opponent_team = match['opponent_team_name']

        output = f"{team} was in {year} in {stadium}({city}) with {attendance} people present at the stadium against {opponent_team} in the {stage} stage"
    else:
        output = "No matches found for the team."

    return output




def query3_2(team):



    # Get team initials
    team_initials_doc = db.Teams.find_one({"_id": team}, {"initials": 1})
    if not team_initials_doc:
        return f"No team found with ID {team}"

    team_initials = team_initials_doc["initials"]

    # Aggregation pipeline
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"Home Team Initials": team_initials},
                    {"Away Team Initials": team_initials}
                ]
            }
        },
        {
            "$sort": {"Attendance": -1}
        },
        {
            "$limit": 1
        },
        {
            "$lookup": {
                "from": "Teams",
                "localField": "Home Team Initials",
                "foreignField": "initials",
                "as": "home_team"
            }
        },
        {
            "$lookup": {
                "from": "Teams",
                "localField": "Away Team Initials",
                "foreignField": "initials",
                "as": "away_team"
            }
        },
        {
            "$addFields": {
                "opponent_team": {
                    "$cond": {
                        "if": {"$eq": ["$Home Team Initials", team_initials]},
                        "then": "$away_team",
                        "else": "$home_team"
                    }
                }
            }
        },
        {
            "$unwind": "$opponent_team"
        },
        {
            "$project": {
                "MatchID": 1,
                "Attendance": 1,
                "Stadium": 1,
                "City": 1,
                "Year": 1,
                "Stage": 1,
                "opponent_team_name": "$opponent_team._id"
            }
        }
    ]

    # Execute the aggregation pipeline
    result = list(db.Matches.aggregate(pipeline))

    # Format the output
    if result:
        match = result[0]  # Since the pipeline returns only one match

        year = str(int(match['Year']))
        stadium = match['Stadium']
        city = match['City'].rstrip()
        attendance = str(int(match['Attendance']))
        stage = match['Stage']
        opponent_team = match['opponent_team_name']

        output = f"{team} was in {year} in {stadium}({city}) with {attendance} people present at the stadium against {opponent_team} in the {stage} stage"
    else:
        output = "No matches found for the team."

    return output









def query4(year):
    pipeline = [
        {
            "$match": {
                "Year": year
            }
        },
        {
            "$lookup": {
                "from": "PlayerPerformance",
                "localField": "MatchID",
                "foreignField": "MatchID",
                "as": "player_performances"
            }
        },
        {
            "$unwind": "$player_performances"
        },
        {
            "$match": {
                "player_performances.Event": {"$type": "string"}
            }
        },
        {
            "$project": {
                "MatchID": 1,
                "Player Name": "$player_performances.Player Name",
                "Event": "$player_performances.Event"
            }
        },
        {
            "$addFields": {
                "Goals": {
                    "$add": [
                        { "$size": { "$regexFindAll": { "input": "$Event", "regex": "G" } } },
                        { "$size": { "$regexFindAll": { "input": "$Event", "regex": "P" } } }
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$Player Name",
                "Total Goals": {"$sum": "$Goals"}
            }
        },
        {
            "$sort": {"Total Goals": -1}
        },
        {
            "$limit": 3
        },
        {
            "$project": {
                "_id": 0,
                "Player Name": "$_id",
                "Total Goals": 1
            }
        }
    ]


    players = list(db.Matches.aggregate(pipeline))

    if not players == None:
        return players
    else:
        return "No data available for the given year."
    

