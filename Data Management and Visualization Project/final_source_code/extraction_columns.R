library(arrow)

# extract and read data from trip record files
parquet_file <- "D:/SHU Class/ADMP-Assignment/NYC Taxi - Yellow_trip_Combined/yellowtrip_2018_combined.parquet"
df <- arrow::read_parquet(parquet_file)

# Print the names of all columns in the data frame
print(names(df))

# Columns to be extracted
selected_columns <- df[c("tpep_pickup_datetime", "tpep_dropoff_datetime","trip_distance", "PULocationID","fare_amount","extra","tip_amount")]

# set file directory
new_parquet_file <- "D:/SHU Class/ADMP-Assignment/Nyc Taxi_cleaned_data_1/yellowtrip_2018_cleaned"
arrow::write_parquet(selected_columns, new_parquet_file)

print(colnames(selected_columns))