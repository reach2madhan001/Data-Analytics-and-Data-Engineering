library(arrow)
library(dplyr)
library(lubridate)

for (x in 2020:2022) {
  
  year = x
  columns_to_extract = c("tpep_pickup_datetime",
                         "tpep_dropoff_datetime",
                         "trip_distance",
                         "PULocationID",
                         "fare_amount",
                         "extra",
                         "tip_amount",
                         "total_amount")
  
  ##extract all files
  for (x in 1:12) {
    
    month = ""
    
    if(x < 10) {
      month = paste("0", x, sep="")
    } else {
      month = x
    }
    
    file_path = paste("D:/trip-data/program/yellow-data/yellow_tripdata_",year ,"-", month, ".parquet",sep="")
    df = read_parquet(file_path)
    # View(df)
    
    print(dim(df))
    
    #remove invalid data
    df <- df[
      (!is.na(df$fare_amount))
      & (!is.na(df$total_amount))
      & (!is.na(df$tpep_pickup_datetime))
      & (!is.na(df$PULocationID))
      & (df$fare_amount > 0)
      & (df$extra == 0 | df$extra == 0.5 | df$extra == 1)
      & (df$trip_distance < 3380)
      & (df$trip_distance > 0)
      & (df$tpep_pickup_datetime < df$tpep_dropoff_datetime)
      & (df$total_amount > 0), ]
    
    df = df[, columns_to_extract]
    
    df$tpep_pickup_datetime <- as.POSIXct(df$tpep_pickup_datetime)
    df$tpep_dropoff_datetime <- as.POSIXct(df$tpep_dropoff_datetime)
    #filter taxi rides beyond 24 hours
    df <- df %>%
      filter(abs(difftime(tpep_pickup_datetime, tpep_dropoff_datetime, units = "hours")) <= 24)
    
    df <- df %>%
      filter(year(tpep_pickup_datetime) == year, month(tpep_pickup_datetime) == x)
     df <- df %>%
      mutate(income = fare_amount + extra + tip_amount)
    
    df$tpep_pickup_datetime <- as.POSIXct(df$tpep_pickup_datetime, format = "%Y-%m-%d %H:%M:%S")
    # Create the 'dateID' column in the format 'yyyymmdd'
    df$day_0f_week  <- wday(df$tpep_pickup_datetime)
    df$dateID <- as.integer(format(df$tpep_pickup_datetime, "%Y%m"))
    df$time_id <- as.integer(paste(df$dateID, df$day_0f_week, sep = ""))
    
    print(head(df$time_id))
    # Create the 'test' column based on 'pickupdatetime' 
    df$timeid <- with(df, {
      hour_of_day <- hour(tpep_pickup_datetime)
      ifelse(hour_of_day >= 0 & hour_of_day < 12, time_id * 10 + 1,
             ifelse(hour_of_day >= 12 & hour_of_day < 17, time_id * 10 + 2,
                    ifelse(hour_of_day >= 17 & hour_of_day < 20, time_id * 10 + 3,
                           time_id * 10 + 4)))
    })

    df <- df %>%
      mutate(trip_duration = as.numeric(difftime(tpep_dropoff_datetime, tpep_pickup_datetime, units = "mins")))
    
    df <- df[, c("timeid", "income", "PULocationID", "trip_distance", "trip_duration")]
    
    print(head(df))
    print(summary(df))
    print(dim(df))
    
    new_path = paste("D:/trip-data/program/yellow-data/", year, "/ext_yellow_tripdata_",year,"-", month, ".parquet",sep="")
    write_parquet(df, new_path)
  }
  
  combined_path = paste("D:/trip-data/program/yellow-data/", year, "/combined_", year ,".parquet", sep="")
  setwd(paste("D:/trip-data/program/yellow-data/", year, sep=""))
  
  file_list <- list.files(pattern = "\\.parquet$")
  combined_data <- data.frame()
  
  for (file in file_list) {
    # Read the data from the Parquet file
    data <- arrow::read_parquet(file)
    
    # Append the data to the combined_data data frame
    combined_data <- bind_rows(combined_data, data)
  }
    arrow::write_parquet(combined_data, combined_path)
}

combined_path = paste("D:/trip-data/program/yellow-data/", "2013-2015", "/combined_", "2013-2015" ,".parquet", sep="")
setwd(paste("D:/trip-data/program/yellow-data/", "2013-2015", sep=""))

file_list <- list.files(pattern = "\\.parquet$")
combined_data <- data.frame()

for (file in file_list) {
  # Read the data from the Parquet file
  data <- arrow::read_parquet(file)
  
  # Append the data to the combined_data data frame
  combined_data <- bind_rows(combined_data, data)
}

arrow::write_parquet(combined_data, combined_path)
head(combined_data)