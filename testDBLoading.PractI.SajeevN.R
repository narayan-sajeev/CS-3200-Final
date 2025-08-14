# testDBLoading.PractI.SajeevN.R
# Narayan Sajeev
# Summer 2 2025

# Import modules
library(RMySQL)
library(sqldf)

# Define settings
db_host_aiven = "mysql-cs3200-final-northeastern-526e.l.aivencloud.com"
db_port_aiven = 28849
db_name_aiven = "defaultdb"
db_user_aiven = "avnadmin"
db_pwd_aiven = "AVNS_6TJXHpMjE7TJa78fR1w"

# Embedded SSL certificate
db_cert = "
-----BEGIN CERTIFICATE-----
MIIEUDCCArigAwIBAgIUO1pRHbx3609yHTsZVnB7CpJy500wDQYJKoZIhvcNAQEM
BQAwQDE+MDwGA1UEAww1MWQ1MGYxYzktODE2OC00YzdmLWIxMDgtMjkzZTdkZGM2
NmMxIEdFTiAxIFByb2plY3QgQ0EwHhcNMjUwODEyMDAxMDQzWhcNMzUwODEwMDAx
MDQzWjBAMT4wPAYDVQQDDDUxZDUwZjFjOS04MTY4LTRjN2YtYjEwOC0yOTNlN2Rk
YzY2YzEgR0VOIDEgUHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCC
AYoCggGBAMnXr1R7Wf6oMySyX9rQtPrPpSBGJpINf0aDMpO6a81xaqbAGEwD++Ju
rrOWKGpps3ZjXQIfTdO0WzFAvgQ11bej1qJKL5liJxWfs+QGW4OSSYWgE8mBNR9I
wZJfvA9eIhCKZUnFjMUrvXeggVaH+2xYh4g2LV0BnuI7cU65Mnz1QCC0GB2Acqyl
XLveWXidDEFTTdzLg+HBMHTNgjUgV6PQcD8egVUl5ePyr+CZxl6/nqfjcB2Cjbya
vT4yW+LwRxZv/vILBNXn8qv5EemHuQIrYyzWYszu0aZeQIH/KTwKt2WAb7IKKYQ5
ff0DdSfx0QsySrFZuTQ3/Sd04Ch3a+rrAmAUE0rVvzqtFT8Vb510p2A377HyGPd5
dSddnNKOQlf8mvzMBPyODhG8HYV0SJ7xPY7aCjzdmhr82mVlFGQJ7B42vM0lfGIH
k/wPUT1q/espSoKYIM2vivkrg2KFFTKfKdnJVf2i1HX4KUd8IVcxWNIa+VXJ1+sO
4qDqGSgqrQIDAQABo0IwQDAdBgNVHQ4EFgQU7GNjNV9068HkGF+ANoLFEiXHMtkw
EgYDVR0TAQH/BAgwBgEB/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQAD
ggGBAC8Q2f5ULd7miSx6Ph5l6HwsfAVqtMUsef5jTYMPKU2y/k8IzdqFuYLNBvb0
SEVbj5PbCTBIAAHI4T4oRsz5M7xrN/PpbwiNoaq+iqn0v04SS0nK7NqB0riRq08+
8ubJxp5xe/fgxYxpmgwjwisxfyaRKINj+mJMomJhwSThghsf0fCWq3cB8PHIi6R2
T5EB3mVsxv8WAaLdkrdiuW8k5wLIRTpVKj3wU+hzN6fnDzD+8/ClkVbowthc9rbY
BHBCmpnCf0wPwa33XOWOUdLXqJaZVmE8ZKgQ7BcYOf8FLi5LHQXN4FUxUbIZnKXa
L328hjdsdWCX3iD50T7ZhCdrMZtyrQvxf8H2g0zuZJiuTNC7BXPrqftvwe3FwYk2
Ni6PNPJTJQO8nG/dW7Qxw9ap0Y+0AJ8gd9dzEFyCIYKk2Tger281uY6h2XyWYQtz
o/NCZh5uMaEQdeEXIjR5aIEoxPTU7vPWu2OPSSyQmZREAKX8wYkGTI0bPTxg9gTG
/TABQw==
-----END CERTIFICATE-----
"

# Use SQLite when needed
options(sqldf.driver = "SQLite")

# Connect to MySQL server
con =  dbConnect(RMySQL::MySQL(), user = db_user_aiven,
                 password = db_pwd_aiven, dbname = db_name_aiven,
                 host = db_host_aiven, port = db_port_aiven,
                 sslmode = "require", sslcert = db_cert)

# Read csv
csv = read.csv("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/incidents-v2.csv")

# Count unique values
countUnique = function(db_col, db_table, csv_col, col_name) {
  
  # Create database SQL query
  db_sql = paste("SELECT COUNT(DISTINCT", db_col, ") FROM", db_table)
  # Get first column from database
  db_val = dbGetQuery(con, db_sql)[, 1]
  
  # Create CSV SQL query
  csv_sql = paste("SELECT COUNT(DISTINCT", csv_col, ") FROM csv")
  # Get first column from csv
  csv_val = sqldf(csv_sql)[, 1]
  
  # Pluralize column name
  plural = paste0(col_name, "s")
  
  # Check if they're equal
  if (db_val == csv_val) {
    
    # Format output string if they're equal
    str = paste("Passed! Number of unique", plural, "in database equals number of unique", plural, "in CSV:", db_val)
    
    # Print results
    cat(str)
    
  } else {
    
    # Format database string
    db_str = paste("Failed! Number of unique", col_name, "in database:", db_val)
    
    # Format CSV string
    csv_str = paste("Failed! Number of unique", col_name, "in CSV:", csv_val)
    
    # Print results if they're different
    cat(db_str)
    cat(csv_str)
    
  }
}

# Count unique flights
countUniqueFlights = function() {
  
  # Create database SQL query
  db_sql = "SELECT COUNT(*) FROM (
  SELECT airlineCode, flightNumber, date
  FROM Flight
  GROUP BY airlineCode, flightNumber, date)
  AS t"
  
  # Get first column from database
  db_val = dbGetQuery(con, db_sql)[, 1]
  
  # Create CSV SQL query
  csv_sql = "SELECT COUNT(*) FROM (
  SELECT airline, flightNumber, date
  FROM csv
  GROUP BY airline, flightNumber, date)
  AS t"
  
  # Get first column from csv
  csv_val = sqldf(csv_sql)[, 1]
  
  # Check if they're equal
  if (db_val == csv_val) {
    
    # Print results
    cat("Passed! Number of unique flights in database equals number of unique flights in CSV:", db_val)
    
  } else {
    
    # Print results if they're different
    cat("Failed! Number of unique flights in database:", db_val)
    cat("Failed! Number of unique flights in CSV:", csv_val)
    
  }
  
}

# Count unique airlines
countUnique("airlineCode", "Airline", "airline", "airline")

# Count unique flights
countUniqueFlights()

# Count unique incidents
countUnique("incidentName", "Incident", "incidentType", "incident")

# Compare dates across database & CSV
compareDates = function() {
  # Get oldest database date
  db_old_date = dbGetQuery(con, "SELECT date FROM Incident ORDER BY date LIMIT 1")[, 1]
  # Get newest database date
  db_new_date = dbGetQuery(con, "SELECT date FROM Incident ORDER BY date DESC LIMIT 1")[, 1]
  
  # Format CSV dates
  csv$date = as.Date(csv$date, format="%d.%m.%Y")
  
  # Get oldest CSV date
  csv_old_date = sqldf("SELECT date FROM csv ORDER BY date LIMIT 1")[, 1]
  # Get newest CSV date
  csv_new_date = sqldf("SELECT date FROM csv ORDER BY date DESC LIMIT 1")[, 1]
  
  # Check if oldest dates match
  if (db_old_date == csv_old_date) {
    print(paste("Passed! Oldest dates match:", db_old_date))
  } else {
    print(paste("Failed! Database oldest date:", db_old_date))
    print(paste("Failed! CSV oldest date:", csv_old_date))
  }
  
  # Check if newest dates match
  if (db_new_date == csv_new_date) {
    print(paste("Passed! Newest dates match:", db_new_date))
  } else {
    print(paste("Failed! Database newest date:", db_new_date))
    print(paste("Failed! CSV newest date:", csv_new_date))
  }
  
}

# Compare first and last dates
compareDates()

# Check numeric columns
checkNumCol = function(db_col, csv_col) {
  # Define SQL query for database
  db_sql = paste("SELECT", db_col, "FROM Incident")
  # Retrieve database values from first column
  db_vals = dbGetQuery(con, db_sql)[, 1]
  
  # Define SQL query for CSV
  csv_sql = paste("SELECT", csv_col, "FROM csv")
  # Retrieve CSV values
  csv_vals = sqldf(csv_sql)[, 1]
  
  # Find sums
  db_sum = sum(db_vals)
  csv_sum = sum(csv_vals)
  
  # Check if sums match
  if (db_sum == csv_sum) {
    # Define output string
    str = paste0("Passed! Sums match for column '", db_col, "': ", db_sum)
    # Print string
    print(str)
                
  
  } else {
    # Define database string
    db_str = paste0("Failed! Database sum for column '", db_col, "': ", db_sum)
    
    # Define CSV string
    csv_str = paste0("Failed! CSV sum for column '", csv_col, "': ", csv_sum)
    
    # Print strings
    print(db_str)
    print(csv_str)
  }
  
  # Find averages
  db_avg = mean(db_vals)
  csv_avg = mean(csv_vals)
  
  # Check if averages match
  if (db_avg == csv_avg) {
    # Define output string
    str = paste0("Passed! Averages match for column '", db_col, "': ", db_avg)
    # Print string
    print(str)
    
    
  } else {
    # Define database string
    db_str = paste0("Failed! Database average for column '", db_col, "': ", db_avg)
    
    # Define CSV string
    csv_str = paste0("Failed! CSV average for column '", csv_col, "': ", csv_avg)
    
    # Print strings
    print(db_str)
    print(csv_str)
  }
  
  
}

# Check flight number column
checkNumCol("flightNumber", "flightNumber")

# Check delay column
checkNumCol("delay", "delay")

# Check num injuries column
checkNumCol("numInjuries", "`num.injuries`")

# Disconnect from database
dbDisconnect(con)