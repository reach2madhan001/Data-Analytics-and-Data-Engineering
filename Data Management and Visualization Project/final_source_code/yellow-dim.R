library(arrow)

for (x in 2013:2022) {
  year = x
  total_rows = 0
  total_size = 0
  total_cols = 0
  
  for (x in 1:12) {
    
    month = ""
    
    if(x < 10) {
      month = paste("0", x, sep="")
    } else {
      month = x
    }
    
    file_path = paste("D:/trip-data/program/yellow-data/yellow_tripdata_",year ,"-", month, ".parquet",sep="")
    df = read_parquet(file_path)
    file_info <- file.info(file_path)
    # Calculate file size in megabytes
    file_size_mb <- file_info$size / (1024 * 1024)
    rows = nrow(df)
    cols = ncol(df)
    total_rows = total_rows + rows
    total_size = total_size + file_size_mb
  }
  print(paste("rows, columns, size in MB for year", year, "is", total_rows, " " , cols, " " , total_size))
}

for (x in 2013:2022) {

    
    file_path = paste("D:/trip-data/program/yellow-data/yearly-combined/combined_",x,".parquet",sep="")
    df = read_parquet(file_path)
    total_rows = nrow(df)
    
  print(paste("rows for year", x, "is", total_rows))
}



