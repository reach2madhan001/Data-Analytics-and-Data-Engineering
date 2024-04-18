# Load necessary libraries
library(arrow)
library(openxlsx)

# set directory
parquet_files <- list.files(path = "D:/SHU Class/ADMP-Assignment/NYC Taxi - Yellow_trip_Combined/yellowtrip_2013_combined.parquet", pattern = "\\.parquet$", full.names = TRUE)

all_columns <- list()

# Read the column names from each Parquet file 
for (file in parquet_files) {
  dataset <- arrow::read_parquet(file)
  all_columns[[basename(file)]] <- colnames(dataset)
}

# Find the maximum number of columns among all Parquet files
max_cols <- max(lengths(all_columns))

# Create a data frame to hold the file names and column names
columns_df <- data.frame(File_Name = character(0), stringsAsFactors = FALSE)

# Arrange the file names and column names horizontally in the data frame
for (file in names(all_columns)) {
  col_names <- all_columns[[file]]
  col_diff <- max_cols - length(col_names)
  col_names <- c(col_names, rep(NA, col_diff))
  columns_df <- rbind(columns_df, c(file, col_names))
}

# Export the data frame to a single Excel sheet
write.xlsx(columns_df, "D:\SHU Class\ADMP-Assignment\Nyc_Taxi_cleaned_Data(Hive)\yellowtrip_2013_cleaned.parquet", rowNames = FALSE)

