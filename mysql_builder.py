def create_and_fill_database(psw):

    from rich.progress import Progress
    import mysql.connector as mysql
    from mysql.connector import Error
    import time
    import pandas as pd 

    db_name = 'WorldCupDB'

    with Progress() as progress:
        # Tasks for each step
        task_connect = progress.add_task("[yellow]Connecting to MySQL...", total=1)
        task_check_db = progress.add_task("[magenta]Checking for existing database...", total=1)
        task_create_db = progress.add_task("[green]Creating Database...", total=1)
        task_filling_db = progress.add_task("[blue]Filling Database...", total=6)  # Total 6 for 6 tables

        try:
            # Connect to MySQL
            mydb = mysql.connect(host='localhost', user='root', password=psw)
            progress.update(task_connect, advance=1)
            time.sleep(0.5)

            if mydb.is_connected():
                mycursor = mydb.cursor()

                # Check for existing database
                mycursor.execute('SHOW DATABASES')
                result = mycursor.fetchall()
                database_exists = False
                for x in result:
                    if db_name.lower() == x[0].lower():
                        database_exists = True
                        break
                progress.update(task_check_db, advance=1)
                time.sleep(0.5)

                if database_exists:
                    # Drop existing database
                    mycursor.execute('DROP DATABASE IF EXISTS ' + db_name)
                    mydb.commit()

                # Create new database
                mycursor.execute("CREATE DATABASE " + db_name)
                mycursor.execute("USE " + db_name)  # Select the newly created database
                progress.update(task_create_db, advance=1)
                time.sleep(0.5)

                # *** Start Inserting Your Tables and Data Here ***

                worldcups = pd.read_csv('World Cup Files/WorldCups.csv')
                matches = pd.read_csv('World Cup Files/WorldCupMatches.csv')
                playerperformances = pd.read_csv('World Cup Files/WorldCupPlayers.csv')

                # Assume worldcups, matches, playerperformances dataframes are defined
                # You should load your dataframes here before proceeding

                # CREATING WORLDCUP TABLE
                
                mycursor.execute(
                    '''
                        CREATE TABLE WorldCup (
                        Year INT PRIMARY KEY,
                        Country VARCHAR(255),
                        QualifiedTeams INT,
                        MatchesPlayed INT,
                        GoalsScored INT,
                        Attendance INT
                        );
                    '''
                )
                for i, row in worldcups.iterrows():
                    # Convert 'Attendance' to an integer format by removing periods
                    attendance = int(row['Attendance'].replace('.', ''))
                    mycursor.execute(
                        '''
                        INSERT INTO WorldCup (Year, Country, QualifiedTeams, MatchesPlayed, GoalsScored, Attendance)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ''',
                        (row['Year'], row['Country'], row['QualifiedTeams'], row['MatchesPlayed'], row['GoalsScored'], attendance)
                    )
                progress.update(task_filling_db, advance=1)
                time.sleep(0.5)

                # CREATING PLACEMENTS RECORD TABLE

                mycursor.execute(
                    '''
                        CREATE TABLE PlacementsRecord (
                        Year INT PRIMARY KEY,
                        Winner VARCHAR(255),
                        RunnersUp VARCHAR(255),
                        Third VARCHAR(255),
                        Fourth VARCHAR(255)
                        );
                    '''
                )

                for i, row in worldcups.iterrows():
                    mycursor.execute(
                        '''
                        INSERT INTO PlacementsRecord (Year, Winner, RunnersUp, Third, Fourth)
                        VALUES (%s, %s, %s, %s, %s)
                        ''',
                        (row['Year'], row['Winner'], row['Runners-Up'], row['Third'], row['Fourth'])
                    )

                progress.update(task_filling_db, advance=1)
                time.sleep(0.5)


                # CREATING MATCHES TABLE

                mycursor.execute(
                    '''
                        CREATE TABLE Matches (
                        Year INT,
                        Datetime VARCHAR(255),
                        Stage VARCHAR(255),
                        Stadium VARCHAR(255),
                        City VARCHAR(255),
                        Attendance INT,
                        Referee VARCHAR(255),
                        Assistant1 VARCHAR(255),
                        Assistant2 VARCHAR(255),
                        RoundID INT,
                        MatchID INT PRIMARY KEY,
                        HomeTeam CHAR(3),
                        AwayTeam CHAR(3)
                        );
                    '''
                )

                for i, row in matches.iterrows():
                    mycursor.execute(
                        '''
                        INSERT IGNORE INTO Matches (Year, Datetime, Stage, Stadium, City, Attendance, Referee, Assistant1, Assistant2, RoundID, MatchID, HomeTeam, AwayTeam)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s)
                        ''',
                        (row['Year'], row['Datetime'], row['Stage'], row['Stadium'], row['City'], row['Attendance'], row['Referee'], row['Assistant 1'], row['Assistant 2'], row['RoundID'], row['MatchID'], row['Home Team Initials'], row['Away Team Initials'])
                    )

                mycursor.execute(
                    '''
                    DELETE FROM Matches WHERE MatchID = 0
                    '''
                )

                progress.update(task_filling_db, advance=1)
                time.sleep(0.5)




                # CREATING SCORE TABLE

                mycursor.execute(
                    '''
                        CREATE TABLE Score (
                        MatchID INT,
                        HomeTeamGoals INT,
                        AwayTeamGoals INT,
                        HalfTimeHomeGoals INT,
                        HalfTimeAwayGoals INT,
                        WinConditions VARCHAR(255),
                        FOREIGN KEY (MatchID) REFERENCES Matches(MatchID)
                        );
                    '''
                )

                for i, row in matches.iterrows():
                    mycursor.execute(
                        '''
                        INSERT IGNORE INTO Score (MatchID, HomeTeamGoals, AwayTeamGoals, HalfTimeHomeGoals, HalfTimeAwayGoals, WinConditions)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ''',
                        (row['MatchID'], row['Home Team Goals'], row['Away Team Goals'], row['Half-time Home Goals'], row['Half-time Away Goals'], row['Win conditions'])
                    )

                progress.update(task_filling_db, advance=1)
                time.sleep(0.5)



                # CREATING PLAYERPERFORMANCE TABLE

                mycursor.execute(
                    '''
                        CREATE TABLE PlayerPerformance (
                        RoundID INT,
                        MatchID INT,
                        TeamInitials VARCHAR(255),
                        CoachName VARCHAR(255),
                        LineUp VARCHAR(255),
                        ShirtNumber INT,
                        PlayerName VARCHAR(255),
                        Position VARCHAR(255),
                        Event VARCHAR(255),
                        PRIMARY KEY (MatchID, PlayerName, ShirtNumber),
                        FOREIGN KEY (MatchID) REFERENCES Matches(MatchID)
                        
                        );
                    '''
                )

                for i, row in playerperformances.iterrows():
                    mycursor.execute(
                        '''
                        INSERT IGNORE INTO PlayerPerformance (RoundID, MatchID, TeamInitials, CoachName, LineUp, ShirtNumber, PlayerName, Position, Event)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''',
                        (row['RoundID'], row['MatchID'], row['Team Initials'], row['Coach Name'], row['Line-up'], row['Shirt Number'], row['Player Name'], row['Position'], row['Event'])
                    )

                progress.update(task_filling_db, advance=1)
                time.sleep(0.5)



                # CREATING REFEREESTAFF TABLE

                mycursor.execute(
                    '''
                        CREATE TABLE RefereeStaff (
                        Name VARCHAR(255) PRIMARY KEY
                        );
                    '''
                )

                mycursor.execute(
                    '''
                    INSERT INTO RefereeStaff (Name)
                    SELECT DISTINCT Referee
                    FROM Matches
                    WHERE NOT EXISTS (
                        SELECT 1 FROM RefereeStaff WHERE Name = Matches.Referee
                    );
                    '''
                )

                mycursor.execute(
                    '''
                    INSERT INTO RefereeStaff (Name)
                    SELECT DISTINCT Assistant1
                    FROM Matches
                    WHERE NOT EXISTS (
                        SELECT 1 FROM RefereeStaff WHERE Name = Matches.Assistant1
                    );
                    '''
                )

                mycursor.execute(
                    '''
                    INSERT INTO RefereeStaff (Name)
                    SELECT DISTINCT Assistant2
                    FROM Matches
                    WHERE NOT EXISTS (
                        SELECT 1 FROM RefereeStaff WHERE Name = Matches.Assistant2
                    );
                    '''
                )

                progress.update(task_filling_db, advance=1)
                time.sleep(0.5)



                # CREATING TEAMS TABLE

                mycursor.execute(
                    '''
                        CREATE TABLE Teams (
                        TeamName VARCHAR(255) PRIMARY KEY,
                        TeamInitials VARCHAR(255)
                        );
                    '''
                )

                for i, row in matches.iterrows():
                    mycursor.execute(
                        '''
                        INSERT IGNORE INTO Teams (TeamName, TeamInitials)
                        VALUES (%s, %s)
                        ''',
                        (row['Home Team Name'], row['Home Team Initials'])
                    )
                
                for i, row in matches.iterrows():
                    mycursor.execute(
                        '''
                        INSERT IGNORE INTO Teams (TeamName, TeamInitials)
                        VALUES (%s, %s)
                        ''',
                        (row['Away Team Name'], row['Away Team Initials'])
                    )

                progress.update(task_filling_db, advance=1)
                time.sleep(0.5)

                # CREATING TEST TABLE

                mycursor.execute(
                    ''' 
                        CREATE TABLE Test (
                        TestName VARCHAR(255) PRIMARY KEY  
                        );
                    '''
                )

                progress.update(task_filling_db, advance=1)
                time.sleep(0.5)


        except Error as e:
            print("Error while connecting to MySQL", e)







