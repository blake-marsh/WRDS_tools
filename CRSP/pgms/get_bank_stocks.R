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
nyfed = fread("./data/nyfed_listed_banks_20200930.csv", sep=",", stringsAsFactors=F)
nyfed[,dt_start := as.Date(as.character(dt_start), "%Y%m%d")]
nyfed[,dt_end := as.Date(as.character(dt_end), "%Y%m%d")]

nyfed = nyfed[!is.na(permco),]
print("Duplicate permcos?:", any(duplicated(nyfed[,list(permco)])))

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
           WHERE a.date >= CAST('1960-01-01' AS DATE) 
             AND a.permco IN (",  paste(nyfed$permco, sep=' ', collapse=','), ")", sep="")

q = dbSendQuery(wrds, q)
bank_stocks = dbFetch(q)
dbClearResult(q)

## set as data table
setDT(bank_stocks)

## check duplicates
any(duplicated(bank_stocks[,list(date, permco, permno)]))


#-----------------------------
# limit to NYFED active dates
#-----------------------------

bank_stocks = sqldf("SELECT a.*, b.entity as ID_RSSD, b.name as bank_name
                     FROM bank_stocks as a,
                          nyfed as b
                     WHERE a.permco = b.permco 
                       AND a.date between b.dt_start and b.dt_end")
setDT(bank_stocks)

#-----------------
# Flag CCAR banks
#-----------------
CCAR_permcos = c(53687, 90, 3151, 20265, 30513,
                 20483, 55006, 52396, 1741, 35048,
                 2093, 20436, 2535, 1689, 21224, 
                 3275, 3685, 1620, 4260, 1645, 
                 21305, 4163, 21691, 57031, 20269,
                 29146, 42125, 42291, 35175, 22107,
                 29151, 29152, 55100, 20260)

bank_stocks[,CCAR := ifelse(permco %in% CCAR_permcos, 1, 0)]

#-----------------------
# count unique permcos
#-----------------------
print(paste("Number of unique permcos in NYFED data:", length(unique(nyfed$permco))))
print(paste("Number of unique permcos from CRSP data:", length(unique(bank_stocks$permco))))

#-------------
# Export data
#-------------
write.table(bank_stocks, "./data/bank_stocks.psv", sep="|", row.names=F)
saveRDS(bank_stocks, "./data/bank_stocks.rds")
write_feather(bank_stocks, "./data/bank_stocks.feather")


