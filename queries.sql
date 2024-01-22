-- GOT OUTSIDE OF THE COMPETITION

SELECT IF( (
            EXISTS (
                SELECT 1
                FROM
                    worldcupdb.placementsrecord
                WHERE
                    Year = %s
                    AND Winner = %s
            )
        ),
        'World Cup Winner', (
            SELECT Stage
            FROM
                worldcupdb.matches
            WHERE
                Year = %s
                AND (
                    HomeTeam = (
                        SELECT
                            TeamInitials
                        FROM
                            worldcupdb.teams
                        WHERE
                            TeamName = %s
                    )
                    OR AwayTeam = (
                        SELECT
                            TeamInitials
                        FROM
                            worldcupdb.teams
                        WHERE
                            TeamName = %s
                    )
                )
            ORDER BY
                STR_TO_DATE(Datetime, '%d %b %Y - %H:%i') DESC
            LIMIT 1
        )
    ) AS Result;

-- FINALS WINNING PERCENTAGE OF THE NATIONAL TEAMS THAT PLAYED AT LEAST ONE FINAL

SELECT
    Team,
    ROUND( (
            SUM(WinnerCount) / (
                SUM(WinnerCount) + SUM(RunnerUpCount)
            )
        ) * 100,
        2
    ) AS FinalsWinningPercentage,
    SUM(WinnerCount) + SUM(RunnerUpCount) AS FinalsPlayed
FROM (
        SELECT
            Winner AS Team,
            COUNT(*) AS WinnerCount,
            0 AS RunnerUpCount
        FROM
            worldcupdb.placementsrecord
        GROUP BY Winner
        UNION ALL
        SELECT
            `RunnersUp` AS Team,
            0 AS WinnerCount,
            COUNT(*) AS RunnerUpCount
        FROM
            worldcupdb.placementsrecord
        GROUP BY
            `RunnersUp`
    ) AS FinalResults
GROUP BY Team
HAVING
    SUM(RunnerUpCount) > 0
    OR SUM(WinnerCount) > 0
ORDER BY
    FinalsWinningPercentage DESC;

-- RETURN A TABLE WITH HOW MANY MATCHES OF A TEAM AGAINST ANOTHER TEAM WERE WON, DRAWN OR LOST

SELECT
    CASE
        WHEN s.HomeTeamGoals > s.AwayTeamGoals THEN CONCAT(teamHome.TeamName , ' wins')
        WHEN s.HomeTeamGoals < s.AwayTeamGoals THEN CONCAT(teamAway.TeamName , ' wins')
        ELSE 'Draw'
    END AS Outcome,
    COUNT(*) AS NumberOfMatches
FROM worldcupdb.matches m
JOIN worldcupdb.score s ON m.MatchID = s.MatchID
JOIN worldcupdb.teams teamHome ON m.HomeTeam = teamHome.TeamInitials
JOIN worldcupdb.teams teamAway ON m.AwayTeam = teamAway.TeamInitials
WHERE (
    teamHome.TeamName = %s AND teamAway.TeamName = %s
) OR (
    teamHome.TeamName = %s AND teamAway.TeamName = %s
)
GROUP BY Outcome;

-- RETURN A TABLE THAT SHOWS THE TOP TEN REFEREES WITH THE MOST CARDS GIVEN IN THE HISTORY OF THE WORLD CUP

SELECT
    worldcupdb.matches.Referee,
    SUM(TotalCards) AS TotalCardsbyReferee
FROM (
        SELECT
            y.MatchID,
            COALESCE(YellowCards, 0) + COALESCE(SecondYellowCards, 0) + COALESCE(RedCards, 0) AS TotalCards
        FROM (
                SELECT
                    MatchID,
                    COUNT(*) AS YellowCards
                FROM
                    worldcupdb.playerperformance
                WHERE
                    Event IS NOT NULL
                    AND Event LIKE '%Y%'
                    AND Event NOT LIKE '%RSY%'
                GROUP BY
                    MatchID
            ) y
            LEFT JOIN (
                SELECT
                    MatchID,
                    COUNT(*) * 2 AS SecondYellowCards
                FROM
                    worldcupdb.playerperformance
                WHERE
                    Event IS NOT NULL
                    AND Event LIKE '%RSY%'
                GROUP BY
                    MatchID
            ) sy ON y.MatchID = sy.MatchID
            LEFT JOIN (
                SELECT
                    MatchID,
                    COUNT(*) * 2 AS RedCards
                FROM
                    worldcupdb.playerperformance
                WHERE
                    Event IS NOT NULL
                    AND Event LIKE '%R%'
                    AND Event NOT LIKE '%RSY%'
                GROUP BY
                    MatchID
            ) r ON y.MatchID = r.MatchID
            OR sy.MatchID = r.MatchID
    ) Cards
    JOIN worldcupdb.matches ON Cards.MatchID = worldcupdb.matches.MatchID
GROUP BY
    worldcupdb.matches.Referee
ORDER BY
    TotalCardsbyReferee DESC
LIMIT 10;