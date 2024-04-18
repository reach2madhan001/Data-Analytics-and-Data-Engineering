# Load the required libraries
library(arrow)

# Set the directory 
parquet_directory <- "D:/SHU Class/ADMP-Assignment/NYC Taxi - Datasets/Yellow Taxi Trip Records/2013"

# List all the Parquet files
parquet_files <- list.files(path = parquet_directory, pattern = "\\.parquet$", full.names = TRUE)

# Function to get the number of rows and columns in a Parquet file
get_rows_cols_count <- function(file_path) {
  
  parquet_data <- read_parquet(file_path)
  
  # Get the number of rows and columns
  num_rows <- nrow(parquet_data)
  num_cols <- ncol(parquet_data)
  
  return(list(rows = num_rows, columns = num_cols))
}

# List to store the results
results <- list()

# Loop through each Parquet file 
for (file in parquet_files) {
  file_name <- basename(file)
  counts <- get_rows_cols_count(file)
  results[[file_name]] <- counts
}

# Print the results
for (file_name in names(results)) {
  counts <- results[[file_name]]
  cat("File:", file_name, "\n")
  cat("Number of rows:", counts$rows, "\n")
  cat("Number of columns:", counts$columns, "\n\n")
}
