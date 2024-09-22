rm(list = ls())

library(RPostgres)
library(data.table)
library(zoo)

setwd("~/WRDS_tools/")

#---------------------------
# Connect to WRDS database
#---------------------------
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds')

#-------------------
# query trace data
#  since 2021:Q1
#-------------------

q = "SELECT bond_sym_id, msg_seq_nb, orig_msg_seq_nb, trc_st, asof_cd,
            CAST(trd_exctn_dt AS DATE) as trd_exctn_dt, trd_exctn_tm,
            cusip_id, substring(cusip_id, 1, 6) as cusip6, company_symbol, 
            bloomberg_identifier, yld_pt as yield, rptd_pr as price,
            entrd_vol_qt as quantity, sub_prdct, rpt_side_cd, cntra_mp_id
     FROM trace.trace_enhanced
     WHERE trd_exctn_dt >= CAST('2021-01-01' AS DATE)"
q <- dbSendQuery(wrds, q)
df <- dbFetch(q)
setDT(df)
dbClearResult(q)

## close wrds connection
dbDisconnect(wrds)

## Create a date time
df[,trd_exctn_dt_tm_gmt := as.POSIXct(trd_exctn_tm, origin=trd_exctn_dt, tz="GMT",format="%H:%M:%S")]

## raw obs counts
print(paste("Raw data count: ", nrow(df)))

#---------------------------------------------
# Keep non-reversal and non-cancelled records
#----------------------------------------------
df_TR = df[which(trc_st %in% c('T', 'R')),]

#--------------------------------------------------------
# Step 1: Remove same-day cancellations and corrections
#--------------------------------------------------------

## get all the cancellation records
cancellations = df[which(trc_st %in% c('X', 'C')),list(trd_exctn_dt, msg_seq_nb, cusip_id, quantity, price, trd_exctn_tm, rpt_side_cd, cntra_mp_id)]
cancellations[,cancellation := 1]
any(duplicated(cancellations[,list(cusip_id, trd_exctn_dt, trd_exctn_tm, price, quantity, rpt_side_cd, cntra_mp_id, msg_seq_nb)]))

## merge into the database
df_TR = merge(df_TR, cancellations, by=c('cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'price', 'quantity', 'rpt_side_cd', 'cntra_mp_id', 'msg_seq_nb'), all.x=T)

## drop cancellations
df_TR = df_TR[is.na(cancellation),]
df_TR[,cancellation := NULL]

## check remaining obs
print(paste("Count after removing cancellations:", nrow(df_TR)))

#--------------------------
# Step 2: Remove reversals
#--------------------------

reversals = df[which(trc_st == 'Y'),list(trd_exctn_dt, msg_seq_nb, cusip_id, quantity, price, trd_exctn_tm, rpt_side_cd, cntra_mp_id)]

reversals[,reversal := 1]
any(duplicated(reversals[,list(cusip_id, trd_exctn_dt, trd_exctn_tm, price, quantity, rpt_side_cd, cntra_mp_id, msg_seq_nb)]))

## merge into the database
df_TR = merge(df_TR, reversals, by=c('cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'price', 'quantity', 'rpt_side_cd', 'cntra_mp_id', 'msg_seq_nb'), all.x=T)

## drop cancelations
df_TR = df_TR[is.na(reversal),]
df_TR[,reversal := NULL]

## check remaining obs
print(paste("Count after removing reversals:", nrow(df_TR)))

#-----------------------------------------
# Step 3: Remove duplicated agency records
#------------------------------------------

## agency sales
agency_s = df_TR[which(rpt_side_cd == 'S' & cntra_mp_id == 'D'),]
setnames(agency_s, old=c('rpt_side_cd'), new=c('rpt_side_cd_s'))

## agency buys
agency_b = df_TR[which(rpt_side_cd == 'B' & cntra_mp_id == 'D'),]

## non-duplicated
agency_bnodup = merge(agency_b, 
		      agency_s[,list(cusip_id, trd_exctn_dt, price, quantity, rpt_side_cd_s)], 
		      by=c('cusip_id', 'trd_exctn_dt', 'price', 'quantity'), all.x=T, allow.cartesian=T)
agency_bnodup = agency_bnodup[is.na(rpt_side_cd_s),]
agency_bnodup[,rpt_side_cd_s := NULL]

## clean up agency_s
setnames(agency_s, old=c("rpt_side_cd_s"), new=c("rpt_side_cd"))

## stack datasets
df_clean = rbindlist(list(df_TR[which(cntra_mp_id == 'C')], agency_s, agency_bnodup), use.names=T, fill=T)

## count
print(paste("After deduping agency trades:", nrow(df_clean)))

#-------------------------------
# Print final clean sample size
#-------------------------------
print(paste("Final trade count:", nrow(df_clean)))

#--------------------
# Export the dataset
#--------------------
#saveRDS(df_clean, "./data/trace_enhanced_clean.rds")
#write.table(df_clean, "./data/trace_enhanced_clean.psv", row.names=F, sep="|", na = ".")


