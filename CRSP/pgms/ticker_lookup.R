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

q = "SELECT a.*, b.htick, b.hcomnam
     FROM crspq.dsf62 as a
	   LEFT JOIN crspq.dsfhdr62 as b
	     ON  a.permco = b.permco 
		 AND a.permno = b.permno 
		 AND a.date between b.begdat and b.enddat
	WHERE htick IN ('BCS', 'BMO', 'BNPQY', 'CS', 'DB', 'HSBC', 'MUFG', 'RY', 'TD', 'TFC', 'UBS', 'SAN')
	  AND date between CAST('2020-03-01' AS DATE) AND CAST('2020-03-31' AS DATE) "
	
q = dbSendQuery(wrds, q)
df = dbFetch(q)
dbClearResult(q)

## set as data table
setDT(df)

## check duplicates
any(duplicated(df[,list(date, permco, permno)]))
		   
