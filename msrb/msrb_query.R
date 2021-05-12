rm(list=ls())

library(data.table)
library(RPostgres)
library(feather)

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
                       WHERE table_schema IN ('msrb', 'msrb_all') ")

schemas = dbFetch(q)
dbClearResult(q)

setDT(schemas)

#---------------
# Table queries
#---------------

## MSRB trades
q = dbSendQuery(wrds, "SELECT *
                       FROM msrb_all.msrb
                       WHERE trade_date >= CAST('2019-01-01' AS DATE) ")

msrb = dbFetch(q)
dbClearResult(q)

setDT(msrb)

#-----------------
# Export the data
#-----------------
write_feather(msrb, "msrb_trade_data_wrds.feather")
saveRDS(msrb, "msrb_trade_data_wrds.rds")
write.table(msrb, "msrb_trade_data_wrds.psv", sep="|", row.names=F)


