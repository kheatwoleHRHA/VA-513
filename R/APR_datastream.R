
# APR DATA CLEANING 
# Kaitlin Heatwole, kheatwole@harrisonburgrha.com
# August 2022


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
# TODO: build system to add one month to existing dataset
# TODO: clean and migrate code to github
# TODO: add more historic data (2014-2017)


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

# create desired date duration of historic data
start_dates <- seq(as.Date("2014-01-01"), as.Date("2017-12-31"), by="months")
end_dates <- ceiling_date(start_dates, "month") - days(1)


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
  
  #REMOVE SUBTOTAL AND OTHER LINES
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
  
  # loop through and create unique filepaths for each file
  filepaths <- NULL
  for (i in 1:length(filenames)){
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

    # create "clean" folder inside unzipped one
  for (i in 1:length(names)){
  
    #check if folder name exists, then write new one if it doesn't
    if(!dir.exists(file.path(paste0(localpath, "clean/monthly/", report)))) dir.create(file.path(paste0(localpath, "clean/monthly/"), report))
  
    #make new "clean" filename
    filename <- paste(names[i], "clean.csv", sep="_")
  
    # get and write each clean monthly file
    file <- get(names[i])
    write_csv(file, paste0(localpath, "clean/monthly/", report, "/", filename))
    }

} # end of loop for each month ######


