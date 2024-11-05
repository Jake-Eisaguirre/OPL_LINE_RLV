print("Script is starting...")


if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, here, DBI, odbc)

### Database Connection: Connect to `ENTERPRISE` database using Snowflake
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),
                                     Driver = "SnowflakeDSIIDriver",
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                     WAREHOUSE = "DATA_LAKE_READER",
                                     Database = "PLAYGROUND",
                                     UID = "jacob.eisaguirre@hawaiianair.com",  # Replace Sys.getenv("UID") with your email
                                     authenticator = "externalbrowser")
  print("Database Connected!")  # Success message
}, error = function(cond) {
  print("Unable to connect to Database.")  # Error handling
})

# Set the schema for the session
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")

line_rlv_q <- "select * from AA_RESERVE_LINEHOLDER"

raw_line_rlv <- dbGetQuery(db_connection_pg, line_rlv_q)

clean_line_rlv <- raw_line_rlv %>% 
  mutate(#BASE = if_else(BASE == "HAL", "HNL", BASE),
          CREW_TYPE = if_else(PAIRING_POSITION %in% c("CA", "FO"), "P", "FA")) %>% 
  rename(DATE=PAIRING_DATE,
         SEAT = PAIRING_POSITION,
         FLEET = EQUIPMENT) %>% 
  relocate(CREW_TYPE, .before = SEAT) %>% 
  mutate(AIRLINE = "HA", .before = CREW_TYPE) %>% 
  distinct()

write_csv(clean_line_rlv, "F:/INFLIGHT_RESERVE_LINE.csv")

print("Data Uploaded")
Sys.sleep(10)