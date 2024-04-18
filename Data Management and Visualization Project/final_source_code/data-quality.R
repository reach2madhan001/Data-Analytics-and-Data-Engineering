library(arrow)
library(dplyr)
library(lubridate)
total_missing = 0

for (y in 2013:2022) {
  
  year = y
  
  for (x in 1:12) {
    
    month = ""
    print("m")
    
    if(x < 10) {
      month = paste("0", x, sep="")
    } else {
      month = x
    }
    
    file_path = paste("D:/trip-data/program/yellow-data/yellow_tripdata_",year ,"-", month, ".parquet",sep="")
    df = read_parquet(file_path)
    # View(df)
    
    print(dim(df))
    
    df <- df[
      (is.na(df$fare_amount))
      | (is.na(df$total_amount))
      | (is.na(df$tpep_pickup_datetime))
      | (is.na(df$tpep_dropoff_datetime))
      | (is.na(df$extra))
      | (is.na(df$tip_amount))
      | (is.na(df$PULocationID))
      | (df$fare_amount < 0)
      | (df$PULocationID > 263)
      | (df$extra < 0)
      | (df$tip_amount < 0)
      | (!(df$extra == 0 | df$extra == 0.5 | df$extra == 1))
      | (df$trip_distance > 10000)
      | (df$fare_amount >7000)
      | (df$total_amount >7000)
      | (df$trip_distance < 0)
      | abs(difftime(df$tpep_pickup_datetime, df$tpep_dropoff_datetime, units = "hours")) > 24
      | (df$tpep_pickup_datetime > df$tpep_dropoff_datetime)
      | year(df$tpep_pickup_datetime) != year
      | month(df$tpep_pickup_datetime) != x
      | (df$total_amount < 0)
      , ]
    
    # total_missing = total_missing + nrow(df)
    
    df = df[, columns_to_extract]
    print(head(df))
  }
}
 
print(paste("invalid count", total_missing))