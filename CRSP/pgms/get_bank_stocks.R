#---------------------------------------------------------------------
# Downloads daily stock prices for permcos
# listed in the NY Fed's PERMCO-RSSD link
# Available at:
# https://www.newyorkfed.org/research/banking_research/datasets.html
#---------------------------------------------------------------------

rm(list=ls())

library(data.table)
library(sqldf)
library(RPostgres)
library(zoo)
library(feather)

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds')


setwd("~/CRSP/")

#-------------------------------
# Read NY Fed listed stock file
#-------------------------------

## read and cleanup file
nyfed = fread("./data/crsp_20200930.csv", sep=",", stringsAsFactors=F)
nyfed[,notice := NULL]
nyfed = nyfed[!is.na(permco),]

## convert dates
nyfed[,dt_start := as.Date(as.character(dt_start), "%Y%m%d")]
nyfed[,dt_end := as.Date(as.character(dt_end), "%Y%m%d")]

## flag for in NYFED data
nyfed[,nyfed := 1]

print(paste("Duplicate permcos?:", any(duplicated(nyfed[,list(permco, dt_start)]))))
saveRDS(nyfed, "./data/nyfed_crsp_20200930.rds")
write_feather(nyfed, "./data/nyfed_crsp_20200930.feather")

#--------------
# CCAR permcos
#--------------
CCAR_permcos = c(53687, 90, 3151, 20265, 30513,
                 20483, 55006, 52396, 1741, 35048,
                 2093, 20436, 2535, 1689, 21224,
                 3275, 3685, 1620, 4260, 1645,
                 21305, 4163, 21691, 20269,
                 29146, 42125, 42291, 35175, 22107,
                 29151, 29152, 55100, 20260)
				 
#-----------------------------
# List of unique bank permcos
#-----------------------------
permcos = paste(unique(c(nyfed$permco, CCAR_permcos)), sep=' ', collapse=',')
length(unique(c(nyfed$permco, CCAR_permcos)))

#----------------------------------------
# Get all stock prices for NYFED permcos
#----------------------------------------

## daily stock prices
q = paste("SELECT a.*, b.htick, b.hcomnam, c.gvkey
           FROM crspq.dsf62 as a
            LEFT JOIN crspq.dsfhdr62 as b
                   ON  a.permco = b.permco
                   AND a.permno = b.permno
                   AND a.date between b.begdat and b.enddat
            LEFT JOIN (SELECT gvkey, lpermno, linkdt, linkenddt
                       FROM crspq.ccmxpf_linktable
                       WHERE linktype IN ('LU', 'LC')
                         AND linkprim in ('P', 'C')) as c
                   ON a.permno = c.lpermno
                  AND a.date between c.linkdt AND coalesce(c.linkenddt, CAST('9999-12-31' AS DATE))
           WHERE a.date >= CAST('1986-01-01' AS DATE)
             AND a.permco IN (",  permcos, ")", sep="")

q = dbSendQuery(wrds, q)
bank_stocks = dbFetch(q)
dbClearResult(q)

## set as data table
setDT(bank_stocks)

## check duplicates
any(duplicated(bank_stocks[,list(date, permco, permno)]))


#---------------------
# Merge in NYFed data
#---------------------

bank_stocks = sqldf("SELECT a.*, b.entity as ID_RSSD, b.name as bank_name, b.inst_type, b.nyfed
                     FROM bank_stocks as a
                       LEFT JOIN nyfed as b
                              ON a.permco = b.permco
                             AND a.date between b.dt_start and b.dt_end")
setDT(bank_stocks)

#-----------------
# Flag CCAR banks
#-----------------
bank_stocks[,CCAR := ifelse(permco %in% CCAR_permcos, 1, 0)]
bank_stocks[,CCAR_FBO := ifelse(permco %in% c(20260, 20269, 22107, 29146, 29151, 29152, 35175, 42125, 42291, 55100), 1, 0)]

#-----------------------------------------
# Drop if not in NYFED and not a CCAR FBO
#-----------------------------------------
bank_stocks = bank_stocks[which(!is.na(nyfed) | CCAR_FBO == 1),]

#---------------------
# Set quarterly date
#--------------------
bank_stocks[,dt_qtr := fifelse(as.Date(as.yearqtr(date)+0.25)-1 == date, as.Date(as.yearqtr(date)+0.25)-1, as.Date(as.yearqtr(date))-1)]

#------------------
# Set monthly date
#------------------
bank_stocks[,dt_mnth := as.Date(as.yearmon(date)+(1/12))-1]

#------------------
# Check duplicates
#------------------
print(paste("Duplicates by permnos, date, inst_type, id_rssd:", any(duplicated(bank_stocks[,list(permno, date, inst_type, ID_RSSD)]))))

#-----------------------
# count unique permcos
#-----------------------
print(paste("Number of unique permcos in NYFED data:", length(unique(nyfed$permco))))
print(paste("Number of unique permcos from CRSP data:", length(unique(bank_stocks$permco))))

#----------------------
# list CCAR_banks
# BNP ADR is excluded
#---------------------

## All CCAR banks
CCAR_banks = unique(bank_stocks[which(dt_qtr == as.Date('2020-06-30') & CCAR == 1),list(hcomnam, permco, permno, htick)])
CCAR_banks[order(hcomnam),]

## CCAR FBOs
CCAR_FBOs = unique(bank_stocks[which(dt_qtr == as.Date('2020-06-30') & CCAR_FBO == 1),list(hcomnam, permco, permno, htick)])
CCAR_FBOs[order(hcomnam),]

#-------------
# Export data
#-------------
write.table(bank_stocks, "./data/bank_stocks.psv", sep="|", row.names=F)
saveRDS(bank_stocks, "./data/bank_stocks.rds")
write_feather(bank_stocks, "./data/bank_stocks.feather")


