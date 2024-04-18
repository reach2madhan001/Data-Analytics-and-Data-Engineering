library(arrow)
library(ISLR)
library(dplyr)

df = read_parquet("D:/trip-data/program/yellow-data/2013/combined_2013.parquet")
colnames(df)
print(head(df))

colnames(df)

nrow(df[df['trip_distance'] == 0])



total_total_amount = sum(df$total_amount)
print(paste("Total total amounts of trips", total_total_amount))

total_fare_amount = sum(df$fare_amount)
print(paste("Total fare amounts of trips", total_fare_amount))

total_rows <- nrow(df)
print(paste("Total trips", total_rows))
summary(df)
