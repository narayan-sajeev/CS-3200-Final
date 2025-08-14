# loadDB.PractI.SajeevN.R
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

# Connect to MySQL server
con =  dbConnect(RMySQL::MySQL(), user = db_user_aiven,
                 password = db_pwd_aiven, dbname = db_name_aiven,
                 host = db_host_aiven, port = db_port_aiven,
                 sslmode = "require", sslcert = db_cert)

# Read data
data = read.csv("https://s3.us-east-2.amazonaws.com/artificium.us/datasets/incidents-v2.csv")

# Get unique values
getUnique = function(v) {
  # Create empty vector
  uniq = c()
  # Loop through vector
  for (i in v) {
    # Check if item already exists
    if (!is.na(i) && !any(uniq == i)) {
      # Add to vector
      uniq = c(uniq, i)
    }
  }
  
  # Return unique values
  return (uniq)
  
}

# Begin transaction
dbExecute(con, "BEGIN")

# Insert data if it's not already in the table
insert = function(val, table) {

  # Create insert SQL command
  insertSQL = paste0("INSERT INTO ", table, " VALUES ('", val, "')")
  
  # Insert it
  dbExecute(con, insertSQL)
  
}

# Insert data into airlines table
for (airline in getUnique(data$airline)) {
  insert(airline, "Airline")
}

# Insert data into airport table
for (airport in getUnique(data$dep.airport)) {
  insert(airport, "Airport")
}

# Insert data into aircraft table
for (aircraft in getUnique(data$aircraft)) {
  insert(aircraft, "Aircraft")
}

# Insert data into incident type table
for (incidentType in getUnique(data$incidentType)) {
  insert(incidentType, "IncidentType")
}

# Insert data into severity table
for (severity in getUnique(data$severity)) {
  insert(severity, "Severity")
}

# Insert data into reporter table
for (reporter in getUnique(data$reported.by)) {
  insert(reporter, "Reporter")
}

# Find minimum of 2 values
getMin = function(v1, v2) {
  if (v1 > v2) {
    return (v2)
  }
  return (v1)
}

# Create batches to insert into database
runBatches = function(values, table, cols) {
  # Define batch size
  batchSize = 500
  # Find max length
  len = length(values)
  # Generate sequences of indices
  range = seq(1, len, batchSize)
  # Loop through indices
  for (i in range) {
    # Find upper bound of batch
    upper = getMin(i + batchSize - 1, len)
    # Retrieve batch of values
    batch = values[i:upper]
    # Combine values together
    combined_vals = paste(batch, collapse="'),('")
    # Create SQL statement
    sql = paste0("INSERT INTO ", table, " (", cols, ") VALUES ('", combined_vals, "')")
    # Execute SQL
    dbExecute(con, sql)
  }
}

# Retrieve values
airlineCode = data$airline
flightNumber = data$flightNumber
rawDate = data$date
depAirport = data$dep.airport
aircraftName = data$aircraft

# Reformat date
date = as.Date(rawDate, format="%d.%m.%Y")

# Compile flight values
flightValues = paste0(airlineCode, "', ", flightNumber, ", '", date, "', '", depAirport, "', '", aircraftName)

# Insert values into flight table
runBatches(flightValues, "Flight", "airlineCode, flightNumber, date, depAirport, aircraftName")

# Retrieve incident values
iid = data$iid
incidentName = data$incidentType
severityType = data$severity
delay = data$delay
numInjuries = data$num.injuries
reporterName = data$reported.by

# Add default values to vectors
addDefault = function(vals) {
  # Define substitute
  sub = 0
  # Loop through values
  for (i in seq(1, length(vals))) {
    # Get current value
    v = vals[i]
    # Check if value is null
    if (is.na(v)) {
      # If so, substitute it
      vals[i] = sub
    }
  }
  
  # Return updated list
  return (vals)
  
}

# Give default values if needed
delay = addDefault(delay)
numInjuries = addDefault(numInjuries)

# Compile incident values
incidentValues = paste0(iid, "', '", airlineCode, "', ", flightNumber, ", '", date, "', '",
     incidentName, "', '", severityType, "', ", delay, ", ", numInjuries, ", '", reporterName)

# Insert values into incident table
runBatches(incidentValues, "Incident", "iid, airlineCode, flightNumber, date,
           incidentName, severityType, delay, numInjuries, reporterName")

# Commit transaction
dbExecute(con, "COMMIT")

# Disconnect from database
dbDisconnect(con)