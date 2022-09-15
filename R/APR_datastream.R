
# APR DATA CLEANING AND JOINING
# Kaitlin Heatwole, kheatwole@harrisonburgrha.com
# August 2022

# ABOUT #######
# TODO: Before running this code: 
# download, unzip, appropriately name, and save all monthly folders in ./VA-513/data/raw/

# This code is designed to automatically clean and join any missing months
## to the existing time series dataset that runs from November 2014 to present.


# PROCESS TO PARSE EACH Q: 
# DONE: clean column names (still need to do 9a and 9b)
# DONE: remove quotation marks in first column with gsub
# DONE: assign subheadings to new column where relevant #see Q15 code
# DONE: remove subtotal/summary rows
# DONE: create time variables
# DONE: create question variables
# DONE: create population and additional identifier variables
# DONE: pivot wide by disaggregation (household type /status)
# DONE: join what makes sense 
# DONE: export clean CSVs
# DONE: build system to stitch historic time periods together (2018-2022)
# DONE: fix heading errors for 5a, 9a, and 9b
# DONE: build system to add one month to existing dataset (see below)
# DONE: clean and migrate code to github
# DONE: add more historic data (2014-2017)

# PROCESS TO UPDATE TIME SERIES########
# DONE: define most recent end of month date 
# DONE: Read file names in data/clean/over_time, read in data
# DONE: extract last start date using max(date)
# DONE: create strings with all missing start/end dates, if any
# IF there were any missing dates:
# DONE: (MANUAL) download and unzip missing months from HMIS
# DONE: clean new months
# DONE: join existing with new months IF there were any missing dates
# DONE: write joined csvs with new end dates 
# DONE: delete old end date files from over_time directory


# SETUP ##########
library(readxl)
library(tidyverse)
library(snakecase)
library(stringr)
library(lubridate)
library(janitor)

# set working directory and local path for file saving
localpath <- "C:/Users/kheatwole/Documents/HMIS/VA-513/data/"
setwd(localpath)
dictionary <- as_tibble(read_xlsx(paste0(localpath, "APR_dictionary.xlsx")))


# GET EXISTING LONGITUDINAL DATA #####

# read in all file names in the over_time directory (should be 72)
filenames_timeseries <- list.files(paste0(localpath, "/clean/over_time"))

# loop through and create unique filepaths for each time series file
filepaths_timeseries <- NULL
for (i in 1:length(filenames_timeseries)){
  temp <- paste0(localpath, "/clean/over_time/", filenames_timeseries[i])
  filepaths_timeseries <- c(filepaths_timeseries, temp)
}

# creates list of names to apply to time series objects
names_timeseries <- str_match(filenames_timeseries,"APR_(.*?)_20")
names_timeseries <- paste0(names_timeseries[,2], "_timeseries") # extracts result (second column) as a vector


# loop through to read in each time series file and assign its name
for (i in 1:length(filepaths_timeseries)) {
  temp <- read_csv(filepaths_timeseries[i],
                   col_names=T)
  name <- names_timeseries[i]
  assign(name, temp)
  rm(temp, name)
  
}


# IDENTIFY MISSING MONTHS ########
# identify max date recorded in the time series and derive the next month to clean and import
max_start <- as.Date(max(Q15_timeseries$start_date))
next_start <- floor_date(max_start+months(1))
most_recent <- floor_date(Sys.Date(), "month") - months(1) #  first day of previous month


if(next_start>=most_recent){
  stop("Time series is already up to date")
}

#begin conditional section only if updates are needed ######
if (next_start<most_recent){ #this won't run if the files are up to date
# create strings with all missing start/end dates (if any)

  missing_starts <- seq(as.Date(next_start),as.Date(most_recent),by="month") # create series of missing starts
  missing_ends <- ceiling_date(missing_starts, "month")- days(1) #derive series of missing ends

# create desired date duration of data to clean
start_dates <- missing_starts
end_dates <- missing_ends

# Manual alternative to create custom date range:
#start_dates <- seq(as.Date("2014-11-01"), as.Date("2022-08-31"), by="months")
#end_dates <- ceiling_date(start_dates, "month") - days(1)



# FUNCTIONS ########

# GENERAL FUNCTION TO CLEAN AND STANDARDIZE EACH Q COLUMN

clean_Qs <- function(Q, name, start, end){
  #remove problematic characters
  Q <- as_data_frame(lapply(Q, function(y) gsub('%', 'percent', y)))
  Q <- as_data_frame(lapply(Q, function(y) gsub('#', 'number', y)))
  Q <- as_data_frame(lapply(Q, function(y) gsub('"', '', y)))
  
  #CLEAN COLUMN NAMES
  # change first row to names for all but the three exceptions
  exceptions <- c("Q5a", "Q9a", "Q9b")
  
  if ((name %in% exceptions)==FALSE)(
    Q <- row_to_names(Q, row_number=1)
    )
  
  # hard coding for the three exceptions
  cols_Q5a <- c("report_validations", "count")
  cols_Q9b <- c("number_of_persons_engaged",
                "all_persons_contacted", 
                "first_contact_not_staying_on_the_streets_ES_or_SH", 
                "first_contact_was_staying_on_the_streets_ES_or_SH", 
                "first_contact_worker_unable_to_determine",
                "drop1",
                "drop2",
                "drop3",
                "drop4")
  cols_Q9a<- gsub("engaged", "contacted",cols_Q9b)
  
  #applying hard coded column names
  if ((name %in% exceptions) == TRUE){
    cols <- switch(name,
                   "Q5a" = cols_Q5a,
                   "Q9a" = cols_Q9a, 
                   "Q9b" = cols_Q9b)
    names(Q) <- cols
    
    # drop columns with "drop" in them
    Q <- Q %>% select(-starts_with("drop"))
    
    #drop first row of 9a and 9b
    if (name != "Q5a")(Q <- Q[-1,])
  }
  
  #supply missing first column name from data dictionary table
  label <- dictionary %>% filter(question == name)%>% select(X1_label)
  colnames(Q)[is.na(colnames(Q))] <- label
  colnames(Q)[colnames(Q)==''] <- label # janky fix for Q27e
  names(Q) <- to_snake_case(names(Q))
  
  # reconcile "with children only" and "with only children"
  if("with_children_only" %in% names(Q))(
    Q <- Q %>% rename("with_only_children" = with_children_only))
  
  #REMOVE EMBEDDED SUBTOTAL AND OTHER LINES
  to_remove <- list("Subtotal" , "Total" , "Percentage")
  Q$delete <- apply(Q, 1, function(r) (any(r %in% to_remove)))  # there has to be a better way
  Q <- Q %>% filter(delete ==FALSE)%>% select(-delete)
  

  # CREATE NEW VARIABLES
  #supply population value from data dictionary table
  population <- dictionary %>% filter(dictionary$question == name)%>% select(population)
  
  Q$population <- population$population
  
  # assign date based on third/fourth variable inputs
  Q$start_date <- as.Date(paste(substring(start, 1,4), substring(start,5,6), substring(start,7,8), sep="-"))
  Q$end_date <- as.Date(paste(substring(end, 1,4), substring(end,5,6), substring(end,7,8), sep="-"))
  
  #assign question column
  Q$question <- name
  
  # RESULTING OUTPUT
  return(Q)
  
}


# FUNCTION TO IDENTIFY SUBHEADING ROWS AS SEPARATE COLUMN

# reassign subheadings to a distinct "category" column (see Q15, Q23c)
fix_subheadings <- function(Q){
  cats <- NULL
  for (r in 1:nrow(Q)){
    line <- as.data.frame(Q[r,])
    cat <- if_else(is.na(line[,2]), line[,1], NULL) # subheadings indicated by NAs in second column, with label in first
    cats <- c(cats, cat)
    
  }
  
  # create  column of categories to populate with duplicates
  cats <- as_data_frame(cats)%>% mutate(
    category = value)
  
  # assign previous value for every empty NA row 
  #for (i in 1:nrow(cats)){
    cats <- cats %>% mutate(
      category = case_when(!is.na(category) ~ category,
                           TRUE ~ lag(category)))
    
  #}
  
  #join categories to original data
  Q$category <- cats$category
  
  #remove subheading rows
  Q <- Q %>% filter(!is.na(Q[,2]))
  
  return(Q)
  
}


# FUNCTION TO PIVOT LONGER BASED ON TWO CROSSTAB TYPES

pivot_crosstabs <- function(Q, name){
  
  # create column names for the two crosstab types
  household_cols <- c("without_children",
                      "with_children_and_adults", 
                      "unknown_household_type",
                      "with_only_children")
  status_match <- c("_start", "_entry", "stayers", "leavers")
  
  status_cols <- names(Q)[str_detect(names(Q), paste(status_match, collapse = "|"))]
  
  # identify which type is relevant for the Q
  ID$disaggregation <- dictionary %>% filter(question == name)%>% select(disaggregation) %>% as.character()
  
  #assign column names
  crosstab_cols <- switch(ID$disaggregation,
                          "household_type" = household_cols,
                          "status" = status_cols)
  
  cols <- names(Q)[names(Q) %in% crosstab_cols]
  
  # apply pivot only to those Qs with disaggregations
  if(!is.na(ID$disaggregation)){
    Q <- Q %>% pivot_longer(cols = all_of(cols),
                            names_to = as.character(ID$disaggregation), 
                            values_to="count")
    # make the new variable numeric, not string
    Q <- Q %>% mutate(count=as.numeric(count))
  }
  
  return(Q)
  
}


# DATA INGEST #########

# TODO: automate file unzipping and file naming if possible
# for now, do it manually, with standard naming convention for zipped folders (APR_startdate_enddate)

# start of loop through each month ####

for (d in 1:length(start_dates)){
  
  # create name of zipped file 
  startdate <- gsub("-", "", start_dates[d])
  enddate <- gsub("-", "", end_dates[d])
  report <- paste("APR", startdate, enddate, sep = "_") 

  # read in all file names in the month directory
  filenames <- list.files(paste0(localpath, "raw/", report))
  
  # loop through and create unique filepaths for each question file
  filepaths <- NULL
  for (i in 1:length(names)){ 
    temp <- paste0(localpath, "raw/", report, "/", filenames[i])
    filepaths <- c(filepaths, temp)
  }
  
  # read in raw data ######
  names <- gsub(".csv", "", filenames) # creates list of names to apply to objects

  # loop through to read in each file and assign its name
  for (i in 1:length(filepaths)) {
    temp <- read_csv(filepaths[i],
                     col_names=F)
    name <- names[i]
    assign(name, temp)
    rm(temp, name)
  }

  # CLEANING #########

  # clean all Qs using the three functions 

  # start of loop through questions ######
  for (i in 1:length(names)){
  
    Q <- get(names[i])
    name <- names[i]
    
    #basic cleaning
    Q <- clean_Qs(Q, name, startdate, enddate)
    
    #fix subheadings for relevant Qs  
    ID <- dictionary %>% filter(question == name)%>% select(subheadings)
      if(ID$subheadings == TRUE) (
        Q <- fix_subheadings(Q))
    
    # pivot longer for relevant Qs
    Q <- pivot_crosstabs(Q, names[i])
  
    # save each unique Q
     assign(name, Q)
  
  } # end of loop through questions #####


  # EXPORT #########

  #save clean individual datasets ####

    # create new date folder inside clean monthly data directory
  for (i in 1:length(names)){
  
    #check if folder name exists, then write new one if it doesn't
    if(!dir.exists(file.path(paste0(localpath, "clean/monthly/", report)))) dir.create(file.path(paste0(localpath, "clean/monthly/"), report))
  
    #make new "clean" filename
    filename <- paste(names[i], "clean.csv", sep="_")
  
    # get and write each clean monthly file
    file <- get(names[i])
    write_csv(file, paste0(localpath, "clean/monthly/", report, "/", filename))
    
  
    }
  
  
# read in as csv (to correct column type errors) and join to time series objects
  for (i in 1:length(names)){
    filename <- paste(names[i], "clean.csv", sep="_")
    file <- read_csv(paste0(localpath, "clean/monthly/", report, "/", filename))
    
    # pull correct longitudinal data
    file_long <- get(paste0(names[i], "_timeseries"))
    
    if(nrow(file)!=0){ # avoids error of joining empty Q4a months
      # join new month to existing longitudinal dataset
      file_long <- full_join(file_long, file)
    
      #assign it to its durable object in order to be available for the next month's loop
      name_timeseries <- paste0(names[i], "_timeseries")
      assign(name_timeseries, file_long)
    }
  }
  
} # end of loop for each month ######


  # join relevant files #####
  
  # gender identity
  Q10_timeseries <- full_join(Q10a_timeseries, Q10b_timeseries)
  Q10_timeseries <- full_join(Q10_timeseries, Q10c_timeseries)
  
  # physical/mental health conditions
  Q13_1_timeseries <- full_join(Q13a1_timeseries, Q13b1_timeseries)
  Q13_1_timeseries <- full_join(Q13_1, Q13c1)
  
  # number of conditions
  Q13_2_timeseries <- full_join(Q13a2_timeseries, Q13b2_timeseries)
  Q13_2_timeseries <- full_join(Q13_2_timeseries, Q13c2_timeseries)
  
  
  # export timeseries as new files
  
  
  # save longitudinal files ######
  for (i in 1:length(names_timeseries)){
    name_timeseries <- names_timeseries[i] # name of question
    Q <- get(name_timeseries)# actual data object
    dates <- c(as.character(range(Q$start_date))) # date range in object
    
    new_filename <- paste0("APR_", #report
                           name, "_",  # question
                           substring(dates[1], 1,4), substring(dates[1], 6,7), "_", #starting month
                           substring(dates[2], 1,4), substring(dates[2], 6,7),  # ending month
                           ".csv"
    )
    write_csv(Q, paste0(localpath, "clean/over_time/", new_filename))
  }
  
  # TODO: confirm that it worked before deleting old files!
  # delete old files #########
 # all_files <- list.files(paste0(localpath, "/clean/over_time"))# new and old listed in directory
#  old_files <- all_files[!(all_files %in% filenames_timeseries)] # select all those not in the new time series
#  
#  for (i in 1:length(old_files)){
#    old_filepaths <- paste0(localpath, "clean/over_time/", old_files[i])
#      unlink(old_filepaths[i], recursive=T)
#  }
  
}# end of conditional section to ingest, clean, join, and export missing months ######