install.packages("DBI")
install.packages("odbc")
library(DBI)
library(odbc)

con <- DBI::dbConnect(odbc::odbc(),
                      .connection_string = "Driver={Cloudera ODBC Driver for Apache Hive};",
                      Host   = "sandbox-hdp.hortonworks.com",
                      UID    = "hive",
                      PWD    = "",
                      Port   = 10000)

hive_query <- "DROP Table DimNeighbourhood"
dbGetQuery(con, hive_query)


dbSendQuery(con, "CREATE DATABASE Trip_database")
dbSendQuery(con, "USE Trip_database")

# Create Hive schema for DimTime
dbExecute(con, "CREATE TABLE DimTime (
                   TimeID INT,
		   DayID INT, 
		   SessionID INT,
                   Year INT,
                   Month INT, 
		   DayName STRING,
		   SessionName STRING
                 )")

# Create Hive schema for DimBorough
dbExecute(con, "CREATE TABLE DimBorough (
                   BoroughID INT,
                   BoroughName STRING
                 )")

# Create Hive schema for Neighbourhood
dbExecute(con, "CREATE TABLE DimNeighbourhood (
                   NeighbourhoodID INT,
                   NeighbourhoodName STRING
                 )")

# Create Hive schema for FactTrip
dbExecute(con, "CREATE TABLE FactTrip (
                   TimeID INT,
                   BoroughID INT,
                   NeighbourhoodID INT,
                   TotalIncome DOUBLE,
                   TotalNoOfTrips INT,
                   TotalDIstCovered DOUBLE,
                   TotalTripDuration DOUBLE
                 )")


# Data Validation 
# Check for missing value 
hive_query <- "SELECT COUNT(*) AS num_missing_rows
FROM DimNeighbourhood
WHERE NeighbourhoodName IS NULL"

dbGetQuery(con, hive_query)




