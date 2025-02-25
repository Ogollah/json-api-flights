# Load required libraries
install.packages("ambiorix")
install.packages("jsonlite")
library(ambiorix)
library(jsonlite)
library(data.table)

# Initialize API
app <- Ambiorix$new()

define_routes <- function(app) {
  
  # Load processed data with optimized threading
  flights_dt <- fread("processed_flights.csv", nThread = getDTthreads())
  
  # POST /flight - Add a Flight
  app$post("/flight", function(req, res) {
    new_flight <- as.data.table(fromJSON(req$body))
    new_flight[, flight_id := max(flights_dt$flight_id, na.rm = TRUE) + 1]
    flights_dt <<- rbindlist(list(flights_dt, new_flight), use.names = TRUE, fill = TRUE)
    fwrite(flights_dt, "processed_flights.csv", nThread = getDTthreads())
    res$json(list(message = "Flight added successfully"))
  })
  
  # GET /flight/:id - Retrieve Flight by ID
  app$get("/flight/:id", function(req, res) {
    flight <- flights_dt[flight_id == as.integer(req$params$id)]
    if (nrow(flight) == 0) {
      res$status(404)
      res$json(list(error = "Flight not found"))
    } else {
      res$json(flight)
    }
  })
  
  # GET /check-delay/:id - Check if Flight was Delayed
  app$get("/check-delay/:id", function(req, res) {
    delay_status <- flights_dt[flight_id == as.integer(req$params$id), .(delayed)]
    if (nrow(delay_status) == 0) {
      res$status(404)
      res$json(list(error = "Flight not found"))
    } else {
      res$json(list(delayed = delay_status$delayed[1]))
    }
  })
  
  # GET /avg-dep-delay - Get Airline’s Average Delay
  app$get("/avg-dep-delay", function(req, res) {
    airline <- req$query$id
    if (!is.null(airline)) {
      avg_delay <- flights_dt[carrier == airline, .(avg_delay = mean(dep_delay, na.rm = TRUE))]
    } else {
      avg_delay <- flights_dt[, .(avg_delay = mean(dep_delay, na.rm = TRUE)), by = carrier]
    }
    res$json(avg_delay)
  })
  
  # GET /top-destinations/:n - Get Top n Destinations
  app$get("/top-destinations/:n", function(req, res) {
    n <- as.numeric(req$params$n)
    top_dest <- flights_dt[, .N, by = dest][order(-N)][.SD[1:n]]
    res$json(top_dest)
  })
  
  # PUT /flights/:id - Update a Flight
  app$put("/flights/:id", function(req, res) {
    updated_flight <- fromJSON(req$body)
    idx <- flights_dt[flight_id == as.integer(req$params$id), which = TRUE]
    if (length(idx) == 0) {
      res$status(404)
      res$json(list(error = "Flight not found"))
    } else {
      set(flights_dt, idx, names(updated_flight), unlist(updated_flight))
      fwrite(flights_dt, "processed_flights.csv", nThread = getDTthreads())
      res$json(list(message = "Flight updated successfully"))
    }
  })
  
  # DELETE /:id - Delete Flight
  app$delete("/:id", function(req, res) {
    idx <- flights_dt[flight_id == as.integer(req$params$id), which = TRUE]
    if (length(idx) == 0) {
      res$status(404)
      res$json(list(error = "Flight not found"))
    } else {
      flights_dt <<- flights_dt[-idx]
      fwrite(flights_dt, "processed_flights.csv", nThread = getDTthreads())
      res$json(list(message = "Flight deleted successfully"))
    }
  })
}

define_routes(app)

# Run API
app$run(port = 9000)