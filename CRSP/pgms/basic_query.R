rm(list=ls())

library(data.table)
library(RPostgres)

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds')

#-------------------------------
# Tables names in CRSPQ schema
#-------------------------------

q = dbSendQuery(wrds, "SELECT table_schema, table_name
                       FROM INFORMATION_SCHEMA.TABLES
                       WHERE table_schema = 'crspq' ")

schemas = dbFetch(q)
dbClearResult(q)

setDT(schemas)

#---------------
# Table queries
#---------------

## daily stock prices
q = dbSendQuery(wrds, "SELECT * 
                       FROM crspq.dsf62
                       LIMIT 100")


daily = dbFetch(q)
dbClearResult(q)

setDT(daily)

## monthly stock prices
q = dbSendQuery(wrds, "SELECT *
                       FROM crspq.msf62
                       LIMIT 100")
monthly = dbFetch(q)
dbClearResult(q)

# header info
q = dbSendQuery(wrds, "SELECT *
                       FROM crspq.dsfhdr62
                       LIMIT 100")
header_info = dbFetch(q)
dbClearResult(q)

# event info
q = dbSendQuery(wrds, "SELECT *
                       FROM crspq.dse62
                       LIMIT 100")
events = dbFetch(q)
dbClearResult(q)










