library(arrow)

# extract and read data from trip record files
parquet_file <- "D:/SHU Class/ADMP-Assignment/Nyc_Taxi_cleaned_Data(Hive)/NYC_Taxi_Zones.parquet"
df <- arrow::read_parquet(parquet_file)

print(names(df))

# Columns to be extracted
selected_columns <- df[c("the_geom", "zone","LocationID", "borough")]

# Rename the "zone" column to "neighbourhood"
selected_columns$neighbourhood <- selected_columns$zone
selected_columns$zone <- NULL  #  remove the old "zone" column


# set file directory
new_parquet_file <- "D:/SHU Class/ADMP-Assignment/Nyc_Taxi_cleaned_Data(Hive)/NYC_Taxi_Zones_cleaned.parquet"
arrow::write_parquet(selected_columns, new_parquet_file)

# extract and read data from trip record files
cleaned_parquet_file <- "D:/SHU Class/ADMP-Assignment/Nyc_Taxi_cleaned_Data(Hive)/NYC_Taxi_Zones_cleaned.parquet"
new_df <- arrow::read_parquet(cleaned_parquet_file)

# Print the names of all columns in the data frame
print(names(new_df))
