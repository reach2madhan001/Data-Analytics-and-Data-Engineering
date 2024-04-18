install.packages("arrow")
install.packages("dplyr")

library(arrow)
library(dplyr)

setwd("D:/SHU Class/ADMP-Assignment/NYC Taxi - Datasets/Yellow Taxi Trip Records/2013")
file_list <- list.files(pattern = "\\.parquet$")
combined_data <- data.frame()

for (file in file_list) {
  # Read the data from the Parquet file
  data <- arrow::read_parquet(file)
  
  # Append the data to the combined_data data frame
  combined_data <- bind_rows(combined_data, data)
}

arrow::write_parquet(combined_data, "yellowtrip_2013_combined.parquet")
