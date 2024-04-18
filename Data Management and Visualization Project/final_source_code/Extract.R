for (y in 2013:2022) {

  year = y
  
  for (x in 1:12) {
    
      month = ""
      
      if(x < 10) {
        month = paste("0", x, sep="")
      } else {
        month = x
      }
    url <- paste("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_",year ,"-", month, ".parquet",sep="")
    
    # Directory where you want to save the downloaded file
    download_dir <- "D:/trip-data/"
    
    # Create the full path to the downloaded file
    download_path <- paste("D:/trip-data/program/yellow-data/yellow_tripdata_",year ,"-", month, ".parquet",sep="")
    
    # Download the file
    download.file(url, destfile = download_path, method = "auto")
  
  }
}