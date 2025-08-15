# configBusinessLogic.PractI.SajeevN.R
# Narayan Sajeev
# Summer 2 2025

# Import modules
library(RMySQL)

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

# Drop procedure if it exists
dbExecute(con, "DROP PROCEDURE IF EXISTS storeIncident")

# Create stored procedure
dbExecute(con, "CREATE PROCEDURE storeIncident(
          IN iid VARCHAR(6),
          IN airlineCode CHAR(2),
          IN flightNumber INTEGER,
          IN date DATE,
          IN incidentName VARCHAR(10),
          IN severityType VARCHAR(8),
          IN delay INTEGER,
          IN numInjuries INTEGER,
          IN reporterName VARCHAR(13)
          )
          
          BEGIN
          
          INSERT INTO Incident VALUES (
          iid, airlineCode, flightNumber,
          date, incidentName, severityType,
          delay, numInjuries, reporterName
          );
          
          END
          ")

# Get a random airline, flight number & date
sample1 = dbGetQuery(con, "SELECT airlineCode, flightNumber, date
                    FROM Flight
                    ORDER BY airlineCode
                    LIMIT 1")

# Retrieve airline
airlineCode = sample1[, 1]
# Retrieve flight number
flightNumber = sample1[, 2]
# Retrieve date
date = sample1[, 3]

# Get an incident name, severity type, delay, number of injuries & reporter name
sample2 = dbGetQuery(con, "SELECT incidentName, severityType,
                    delay, numInjuries, reporterName
                    FROM Incident
                    ORDER BY delay
                    LIMIT 1")

# Define incident attributes
iid = "i20000"
incidentName = sample2[, 1]
severityType = sample2[, 2]
delay = sample2[, 3]
numInjuries = sample2[, 4]
reporterName = sample2[, 5]

# Define SQL
sql = paste0("CALL storeIncident('", 
          iid, "', '", airlineCode,  "', ", flightNumber,  ", '", 
          date,  "', '", incidentName,  "', '", severityType,  "', ",
          delay,  ", ", numInjuries,  ", '", reporterName,
          "')")

# Add incident
newIncident1 = dbExecute(con, sql)

# Define SQL query for printing
sql1 = paste0("SELECT * FROM Incident WHERE iid = '", iid, "'")

# Print new incident
dbGetQuery(con, sql1)

# Disconnect from database
dbDisconnect(con)