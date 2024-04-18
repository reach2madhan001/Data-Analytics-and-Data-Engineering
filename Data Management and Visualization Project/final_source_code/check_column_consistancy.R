library(dplyr)
library(arrow)

# Create a list to store column names
column_names_list <- list()

# set directory
data_frames_directory <- "D:/SHU Class/ADMP-Assignment/NYC Taxi - Datasets/Yellow Taxi Trip Records/all-data"

# List of data frame file names
data_frame_files <- list.files(path = data_frames_directory, pattern = "\\.parquet$", full.names = TRUE)  # Change pattern if needed

# Read each data frame and store its column names
for (file_path in data_frame_files) {
  df <- read_parquet(file_path)  # Change read function if your files are in a different format
  column_names_list[[file_path]] <- colnames(df)
}

# Check if all data frames have the same column names
print('check column names')
consistent_column_names <- length(unique(column_names_list)) == 1
print('received result')

# Print result
if (consistent_column_names) {
  print("All data frames have consistent column names.\n")
} else {
  print("Data frames have inconsistent column names.\n")
}
