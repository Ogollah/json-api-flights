# Load required libraries
install.packages("nycflights13")
install.packages("data.table")
library(nycflights13)
library(data.table)

# Optimize threading
setDTthreads(0)

# Load flights dataset
flights_dt <- as.data.table(flights)

# Compute average departure delay for each airline
avg_dep_delay <- flights_dt[, .(avg_delay = mean(dep_delay, na.rm = TRUE)), by = carrier]

# Find the top 5 destinations with the most flights
top_destinations <- flights_dt[, .N, by = dest][order(-N)][.SD[1:5]]

# Add flight_id and delayed column
flights_dt[, flight_id := .I]
flights_dt[, delayed := dep_delay > 15]

# Save processed data as CSV
fwrite(flights_dt, "processed_flights.csv", nThread = getDTthreads())

