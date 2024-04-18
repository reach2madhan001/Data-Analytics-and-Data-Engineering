library(arrow)
library(dplyr)

for (x in 2013:2022) {

  year = x
  
  df = read_parquet(paste("D:/trip-data/program/yellow-data/yearly-combined/combined_", year, ".parquet", sep=""))
  sum_total_amounts <- sum(df$total_amount)
  sum_fare_amounts <- sum(df$fare_amount)
  sum_inc <- sum(df$inc)
  print(year)
  print(sum_total_amounts)
  print(sum_fare_amounts)
  print(sum_inc)
  
}
