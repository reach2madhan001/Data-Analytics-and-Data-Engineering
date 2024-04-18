# Install and load required packages
install.packages("arrow")
library(arrow)

# Read the Parquet file
parquet_file <- arrow::read_parquet("D:/SHU Class/ADMP-Assignment/Nyc_Taxi_cleaned_Data(Hive)/NYC_Taxi_Zones_cleaned.parquet")

# Specify the col name
column_name <- "borough"

# Get unique values and assign IDs
unique_values <- unique(parquet_file[[column_name]])
id_mapping <- data.frame(value = unique_values, boroughid = seq_along(unique_values))

# To Merge the IDs into the original data
result <- merge(parquet_file, id_mapping, by.x = column_name, by.y = "value", all.x = TRUE)

# Save the result
arrow::write_parquet(result, "D:/SHU Class/ADMP-Assignment/Nyc_Taxi_cleaned_Data(Hive)/NYC_Taxi_Zones_cleaned1.parquet")

# Read the saved file
df <- arrow::read_parquet("D:/SHU Class/ADMP-Assignment/Nyc_Taxi_cleaned_Data(Hive)/NYC_Taxi_Zones_cleaned1.parquet")

print(colnames(df))
print(df)
