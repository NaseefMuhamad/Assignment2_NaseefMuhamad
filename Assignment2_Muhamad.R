# ALMUZAHIM NASEEF MUHAMAD S24B38/006 B30296

# Install required packages
# Uncomment if not installed
# install.packages("fs")
# install.packages("DBI")
# install.packages("duckdb")
# install.packages("readr")
# install.packages("readxl")
# install.packages("dplyr")
# install.packages("tidyr")
# install.packages("summarytools")
# install.packages("rmarkdown")
# install.packages("knitr")
# install.packages("httr")
# install.packages("curl")
# install.packages("stringr")
# install.packages("WDI")

# Load packages
suppressPackageStartupMessages({
  library(fs)
  library(DBI)
  library(duckdb)
  library(readr)
  library(dplyr)
  library(tidyr)
  library(summarytools)
  library(rmarkdown)
  library(knitr)
  library(WDI)
})

# Search indicators related to digital
indicators <- WDIsearch('digital')
head(indicators, 20)

# Create clean folder structure
if (!dir_exists("raw_data")) dir_create("raw_data")
if (!dir_exists("db")) dir_create("db")

# Define selected indicators
indicators <- list(
  internet_users = "IT.NET.USER.ZS",
  mobile_subs    = "IT.CEL.SETS.P2",
  fixed_broad    = "IT.NET.BBND.P2",
  secure_servers = "IT.NET.SECR.P6",
  mobile_broad   = "IT.CEL.BBND.P2",
  fixed_phone    = "IT.TEL.FIX.P2",
  ict_exports    = "TX.VAL.ICTG.ZS.UN",
  ict_imports    = "TM.VAL.ICTG.ZS.UN",
  hi_tech_exp    = "TX.VAL.TECH.MF.ZS",
  edu_spend      = "SE.XPD.TOTL.GD.ZS"
)

# Download and save datasets
for (name in names(indicators)) {
  code <- indicators[[name]]
  
  df <- tryCatch(
    WDI(country = 'UGA', indicator = code, start = 2000, end = 2023),
    error = function(e) {
      cat('Error:', name, '\n')
      data.frame()
    }
  )
  
  path <- file.path('raw_data', paste0(name, '.csv'))
  write_csv(df, path)
  cat('Saved:', path, "with", nrow(df), 'rows\n')
}

# Set working directory
setwd("C:/Users/nasee/Desktop/Assignment2/Assignment2_NaseefMuhamad")

# List files
list.files("raw_data")
fs::dir_ls("raw_data")

# Preview sample datasets
df <- read.csv("raw_data/internet_users.csv")
head(df)
str(df)
summary(df)

df <- read.csv("raw_data/fixed_broad.csv")
head(df)
str(df)
summary(df)

# Connect to DuckDB
con <- dbConnect(duckdb::duckdb(), dbdir = 'db/assignment2.duckdb')

# Import CSVs into DuckDB
csv_files <- list.files('raw_data', pattern = '\\.csv$', full.names = TRUE)

for (file in csv_files) {
  table_name <- tools::file_path_sans_ext(basename(file))
  query <- sprintf("CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s')", table_name, file)
  dbExecute(con, query)
  cat("Imported:", table_name, "\n")
}

# Check for missing values
tables <- dbListTables(con)

for (table in tables) {
  cat("\nMissing values in:", table, "\n")
  df <- dbReadTable(con, table)
  print(colSums(is.na(df)))
}

# Descriptive statistics for one variable per dataset
indicator_columns <- list(
  internet_users = "IT.NET.USER.ZS",
  mobile_subs    = "IT.CEL.SETS.P2",
  fixed_broad    = "IT.NET.BBND.P2",
  secure_servers = "IT.NET.SECR.P6",
  mobile_broad   = "IT.CEL.BBND.P2",
  fixed_phone    = "IT.TEL.FIX.P2",
  ict_exports    = "TX.VAL.ICTG.ZS.UN",
  ict_imports    = "TM.VAL.ICTG.ZS.UN",
  hi_tech_exp    = "TX.VAL.TECH.MF.ZS",
  edu_spend      = "SE.XPD.TOTL.GD.ZS"
)

for (table in names(indicator_columns)) {
  col <- indicator_columns[[table]]
  df <- dbReadTable(con, table)
  
  if (col %in% names(df)) {
    values <- df[[col]]
    
    if (!is.numeric(values)) {
      values <- suppressWarnings(as.numeric(values))
    }
    
    values <- values[!is.na(values)]
    
    if (length(values) > 0) {
      cat("\nStats for:", table, "| Column:", col, "\n")
      cat("Count:", length(values), "\n")
      cat("Mean:", round(mean(values), 2), "\n")
      cat("Median:", round(median(values), 2), "\n")
      cat("Min:", round(min(values), 2), "\n")
      cat("Max:", round(max(values), 2), "\n")
      cat("SD:", round(sd(values), 2), "\n")
    } else {
      cat("\nColumn", col, "has no numeric values after coercion.\n")
    }
  } else {
    cat("\nColumn", col, "not found in table", table, "\n")
  }
}