install.packages("arrow")
install.packages("dplyr")

library(arrow)
library(dplyr)

# set directory and read the file
parquet_file <- "D:/SHU Class/ADMP-Assignment/NYC Taxi - Yellow_trip_Combined/yellowtrip_2013_combined.parquet"
df <- arrow::read_parquet(parquet_file)

# Print the names of all columns in the data frame
print(names(df))

# Summary statistics
summary(df)

#print dim
print(dim(df))

#remove invalid data
df <- df[
  (!is.na(df$fare_amount))
  & (!is.na(df$tpep_pickup_datetime))
  & (!is.na(df$tpep_dropoff_datetime))
  & (!is.na(df$PULocationID))
  & (df$fare_amount > 0)
  & (df$trip_distance < 3300)
  & (df$fare_amount <9000)
  & (df$trip_distance > 0)
  & (df$tpep_pickup_datetime< df$tpep_dropoff_datetime), ]

#df = df[, columns_to_extract]

#filter taxi rides beyond 24 hours
df <- df %>%
  filter(abs(difftime(tpep_pickup_datetime, tpep_dropoff_datetime, units = "hours")) <= 24)

df <- df %>%
  mutate(income = fare_amount + tips + extra )

new_parquet_file <- "D:/SHU Class/ADMP-Assignment/Nyc_Taxi_cleaned_Data(Hive)/yellowtrip_2013_cleaned.parquet"
arrow::write_parquet(df, new_parquet_file)

# Print the names of all columns in the data frame
print(names(df))

print(summary(df))
print(dim(df))


