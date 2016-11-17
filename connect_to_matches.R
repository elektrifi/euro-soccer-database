library("RSQLite")

dbfile = "./euro-soccer-database/database.sqlite";

sqlite = dbDriver("SQLite");

con = dbConnect(sqlite, dbfile, cache_size = 50000, synchronous = "full")

alltables = dbListTables(con)
alltables

#results = dbSendQuery(con, 'select * from Match limit 10' )
#matchData = fetch(results)
#matchData
#dbClearResult(results)

######
# Create playerMerged = player + player_attributes tables
######
playerData = dbGetQuery(con, "select * from Player order by id asc")
playerAttributeData = dbGetQuery(con, "select * from Player_Attributes")
playerMerged = merge(playerData, playerAttributeData, by="player_fifa_api_id")
head(playerMerged)
playerMerged$date = as.Date(playerMerged$date)

######
# Create teamMerged = team + team_attributes tables
######
teamData = dbGetQuery(con, "select * from Team order by id asc")
teamAttributeData = dbGetQuery(con, "select * from Team_Attributes order by id asc")
teamMerged = merge(teamData, teamAttributeData, by="team_fifa_api_id")
head(teamMerged)
teamMerged$date = as.Date(teamMerged$date)

######
# Store other attributes
######
countryData = dbGetQuery(con, "select * from Country order by id asc")
leagueData = dbGetQuery(con, "select * from League order by id asc")
leagueMerged = merge(leagueData, countryData, by="id")
matchData = dbGetQuery(con, "select * from Match order by id asc")
#head(matchData)
matchData$date = as.Date(matchData$date)
# Check match data
dim(matchData)
matchData[1:10,c(10,11)]

#
library(plyr)
matchMerged = merge(matchData, leagueMerged, by="country_id", all=TRUE)
head(matchMerged)
rename(matchMerged, c("name.x"="league", "name.y"="country"))
colnames(matchMerged)
dim(matchMerged)
matchMerged[1:20,c(10,11)]
matchMerged$result = ""
# Add simple match result column
for (i in 1:nrow(matchMerged)) {
  if ( (matchMerged$home_team_goal[i]>0) & (matchMerged$home_team_goal[i]>matchMerged$away_team_goal[i]) ) {
    matchMerged$result[i] = "HOME_WIN"
  } else if ( (matchMerged$away_team_goal[i]>0) & (matchMerged$home_team_goal[i]<matchMerged$away_team_goal[i]) ) {
    matchMerged$result[i] = "AWAY_WIN"
  } else if ( (matchMerged$home_team_goal[i]==0) & (matchMerged$away_team_goal[i]==0) ) {
    matchMerged$result[i] = "NOSCORE_DRAW"
  } else if ( (matchMerged$home_team_goal[i]>0 & matchMerged$away_team_goal[i]>0) & (matchMerged$home_team_goal[i]=matchMerged$away_team_goal[i]) ) {
    matchMerged$result[i] = "SCORE_DRAW"
  }
}

dim(matchMerged)
matchMerged[1:20,c(10,11,119)]

# Add a total_goals_scored column
matchMerged$total_goals_scored = 0
for (i in 1:nrow(matchMerged)) {
  matchMerged$total_goals_scored[i] = matchMerged$home_team_goal[i] + matchMerged$away_team_goal[i]
}
dim(matchMerged)
matchMerged[1:20,c(10,11,119,120)]

# Export matchMerged data frame
colnames(matchMerged)

# Free up connection resources
dbDisconnect(con)

# Join the player data
