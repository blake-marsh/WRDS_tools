#---------------------------------------------
# Download common stock prices and dividends 
# over a given date range
#---------------------------------------------


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

#--------------------------
# Get common stock prices
#--------------------------
## daily stock prices
q = "SELECT a.*, b.htick, b.hcomnam, b.hshrcd, b.hnaics, c.gvkey,
            abs(a.prc)*a.shrout as mktcap,
            a.prc/a.cfacpr as price_adj,
            a.shrout*a.cfacshr as shares_adj 
     FROM crspq.dsf62 as a
       LEFT JOIN crspq.dsfhdr62 as b
              ON  a.permco = b.permco
              AND a.permno = b.permno
              AND a.date between b.begdat and b.enddat
       LEFT JOIN (SELECT gvkey, lpermno, linkdt, linkenddt
                  FROM crspq.ccmxpf_linktable
                  WHERE linktype IN ('LU', 'LC')
                    AND linkprim in ('P', 'C')) as c
              ON  a.permno = c.lpermno
              AND a.date between c.linkdt AND coalesce(c.linkenddt, CAST('9999-12-31' AS DATE))
     WHERE a.date between CAST('2020-03-01' AS DATE) AND CAST('2020-03-31' AS DATE)
       AND b.hshrcd IN (10,11)
       AND a.prc IS NOT NULL"

q = dbSendQuery(wrds, q)
stocks = dbFetch(q)
dbClearResult(q)

## set as data table
setDT(stocks)

## check duplicates
any(duplicated(stocks[,list(date, permco, permno)]))

#-----------------------
# Create a monthly date
#-----------------------
stocks[,dt_mnth := as.Date(as.yearmon(date)+(1/12))-1]

#-------------------------
# Create a quarterly date
#-------------------------
stocks[,dt_qtr := fifelse(as.Date(as.yearqtr(date)+0.25)-1 == date, as.Date(as.yearqtr(date)+0.25)-1, as.Date(as.yearqtr(date))-1)]

#--------------------
# Get compustat data
#--------------------

## get the raw data
q = "SELECT gvkey, datadate, fyearq, fqtr, fyr, datacqtr, datafqtr,
            dvy, dvpy, cshoq, cshopq, iby, piy
     FROM comp.fundq
     WHERE indfmt = 'INDL' AND datafmt = 'STD' AND consol = 'C' AND popsrc = 'D'
       AND datacqtr IS NOT NULL
	   AND datafqtr IS NOT NULL"
q = dbSendQuery(wrds, q)
cs = dbFetch(q)
dbClearResult(q)

setDT(cs)

## create a firm id
cs[,firm_id := .GRP, by=list(gvkey,fyr)]

## convert calendar quarter
cs[,datacqtr := as.Date(as.yearqtr(datacqtr, "%YQ%q")+0.25)-1]

## convert fiscal qtr
cs[,datafqtr := as.yearqtr(datafqtr)]

## check duplicates
any(duplicated(cs[,list(gvkey, datadate, fyr)]))
any(duplicated(cs[,list(firm_id, datafqtr)]))

#---------------------
# Take compustat lags
#---------------------
ytd_vars = c("dvy", "dvpy", "iby", "piy")
ytd_flow_vars = paste(ytd_vars, "_q", sep="")

## order dataset
cs = cs[order(firm_id, datafqtr),]

cs[,firm_id_L1 := shift(firm_id, 1)]
cs[,datafqtr_L1 := shift(datafqtr,1)]

## take lags
cs[,(ytd_flow_vars) := lapply(.SD, function(x) ifelse(fqtr==1, x, x - shift(x,1))), .SDcols=ytd_vars]

## set NAs
cs[,(ytd_flow_vars) := lapply(.SD, function(x) ifelse((fqtr == 1 ) | (firm_id == firm_id_L1 & datafqtr-datafqtr_L1 == 0.25), x, NA)), .SDcols=ytd_flow_vars]

#------------------------------
# Generate compustat variables
#------------------------------
cs[,dividends_per_share := (dvy_q-dvpy_q)/cshoq]
cs[,earnings_per_share := (iby_q-dvpy_q)/cshopq]

#----------------------
# Deal with duplicates
#----------------------
cs[,row_count := .N, by=list(gvkey, datacqtr)]
cs = cs[which(row_count == 1),]
cs[,row_count := NULL]

## check duplicates
any(duplicated(cs[,list(gvkey, datacqtr)]))

#----------------------------------------
# Merge CRSP with Compustat fundamentals
#-----------------------------------------
stocks = merge(stocks, cs, by.x=c("gvkey", "dt_qtr"), by.y=c("gvkey", "datacqtr"))
any(duplicated(stocks[,list(date, permco, permno)]))

#--------------------
# Define repurchases
#--------------------
stocks[,prc_qtr_avg := mean(prc, na.rm=T),by=list(dt_qtr, permco, permno)]
stocks[,common_repurchased := ifelse(!is.na(cshopq*prc), cshopq*prc_qtr_avg,0)]

#-------------
# Export data
#-------------
write.table(stocks, "./data/stocks.psv", sep="|", row.names=F)
saveRDS(stocks, "./data/stocks.rds")
write_feather(stocks, "./data/stocks.feather")


