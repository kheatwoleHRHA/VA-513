
# APR CUSTOM LONGITUDINAL COMBINING
# Kaitlin Heatwole, kheatwole@harrisonburgrha.com
# August 2022

# ABOUT #######
# This script can be used to create a custom longitudinal time series.
# it is NOT NEEDED for ongoing monthly updates. Those can be completed entirely in APR_datastream.R


# SETUP ##########
library(tidyverse)
library(lubridate)
library(readxl)

# set working directory and local path for file saving
localpath <- "C:/Users/kheatwole/Documents/HMIS/VA-513/data/"
setwd(localpath)

# create desired date duration of historic data

start_dates <- seq(as.Date("2020-01-01"), as.Date("2020-12-31"), by="months")

end_dates <- ceiling_date(start_dates, "month") - days(1)

# read in dictionary and extract vector of filenames
dictionary <- read_xlsx(paste0(localpath, "APR_dictionary.xlsx"))
names <- as.vector(dictionary$question)

filenames <- c(paste0(names, "_clean.csv"))


# loop each question through each date  #######
for (i in 1:length(names)){
  name <- names[i]
  filename <- filenames[i]


  Q <- NULL
  for (d in 1:length(start_dates)){

    startdate <- gsub("-", "", start_dates[d])
    enddate <- gsub("-", "", end_dates[d])
  
    report <- paste("APR", startdate, enddate, sep = "_") # create name of zipped file 
  
    temp <- read_csv(paste0(localpath, "clean/monthly/", report, "/", filename))
  
    Q <- rbind(Q, temp)
  
     assign(name, Q)
  
  }# end of loop through dates for each Q

 }# end of loop through all Qs


# join relevant files #####

# gender identity
Q10 <- full_join(Q10a, Q10b)
Q10 <- full_join(Q10, Q10c)

# physical/mental health conditions
Q13_1 <- full_join(Q13a1, Q13b1)
Q13_1 <- full_join(Q13_1, Q13c1)

# number of conditions
Q13_2 <- full_join(Q13a2, Q13b2)
Q13_2 <- full_join(Q13_2, Q13c2)

names <- c(names, "Q10", "Q13_1", "Q13_2") # add new ones to list for saving


# save longitudinal files ######
for (i in 1:length(names)){
  name <- names[i] # name of question
  Q <- get(name)# actual data object
  dates <- c(as.character(range(Q$start_date))) # date range in object
  
  new_filename <- paste0("APR_", #report
                         name, "_",  # question
                         substring(dates[1], 1,4), substring(dates[1], 6,7), "_", #starting month
                         substring(dates[2], 1,4), substring(dates[2], 6,7),  # ending month
                         ".csv"
  )
  write_csv(Q, paste0(localpath, "clean/over_time/", new_filename))
}



