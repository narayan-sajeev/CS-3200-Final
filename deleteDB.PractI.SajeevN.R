# deleteDB.PractI.SajeevN.R
# Narayan Sajeev
# Summer 2 2025

# Load required library
library(RMySQL)

# Define settings
db_host_aiven <- "mysql-cs3200-final-northeastern-526e.l.aivencloud.com"
db_port_aiven <- 28849
db_name_aiven <- "defaultdb"
db_user_aiven <- "avnadmin"
db_pwd_aiven <- "AVNS_6TJXHpMjE7TJa78fR1w"

# Embedded SSL certificate
db_cert <- "
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
con <-  dbConnect(RMySQL::MySQL(), 
                  user = db_user_aiven, 
                  password = db_pwd_aiven,
                  dbname = db_name_aiven, 
                  host = db_host_aiven, 
                  port = db_port_aiven,
                  sslmode = "require",
                  sslcert = db_cert)

# Drop tables
dbExecute(con, "DROP TABLE IF EXISTS Incident")
dbExecute(con, "DROP TABLE IF EXISTS Flight")
dbExecute(con, "DROP TABLE IF EXISTS Reporter")
dbExecute(con, "DROP TABLE IF EXISTS Severity")
dbExecute(con, "DROP TABLE IF EXISTS IncidentType")
dbExecute(con, "DROP TABLE IF EXISTS Aircraft")
dbExecute(con, "DROP TABLE IF EXISTS Airport")
dbExecute(con, "DROP TABLE IF EXISTS Airline")

# Disconnect from database
dbDisconnect(con)