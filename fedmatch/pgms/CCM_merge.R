rm(list=ls())

library(data.table)
library(maps)
library(RPostgres)

wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds')

#--------------------------------------
# CRSP COMPHIST data (post-April 2007)
#--------------------------------------

q = dbSendQuery(wrds, "SELECT CAST(gvkey as varchar(6)) as gvkey, 
                              CAST(hchgdt AS DATE) as hchgdt, 
                              CAST(hchgenddt AS DATE) as hchgenddt, 
                              hconm, hconml, hein,  HSIC, HNAICS, HCITY, HSTATE, HADDZIP, HFIC, HINCORP
                       FROM crsp_q_ccm.comphist")

comphist = dbFetch(q)
dbClearResult(q)

setDT(comphist)
comphist[,(c("hchgdt", "hchgenddt")) := lapply(.SD, function(x) fifelse(is.na(x), as.Date("9999-12-31"), x)), .SDcols=c("hchgdt", "hchgenddt")]
comphist[,crsp_table := "comphist"]


#-------------------------------------
# CRSP CST_HIST data (pre-April 2007)
#-------------------------------------
q = dbSendQuery(wrds, "SELECT CAST(gvkey as int) as gvkey, 
                                     CAST(chgdt as varchar(8)) as hchgdt, 
                                     CAST(chgenddt as varchar(8)) as hchgenddt, 
                                     coname as hconm, EIN as hEIN, 
                                     CAST(dnum as varchar(4)) as HSIC, 
                                     CAST(naics as varchar(6)) as hnaics, 
                                     state as FIPS, cnum, FINC, stinc, smbl, zlist 
                       FROM crsp_q_ccm.cst_hist")
cst_hist = dbFetch(q)
dbClearResult(q)
setDT(cst_hist)

cst_hist[,gvkey := as.character(sprintf("%06.0f", gvkey))]
cst_hist[,(c("hchgdt", "hchgenddt")) := lapply(.SD, function(x) fifelse(is.na(x) | x == "99999999", "20070413", x)), .SDcols=c("hchgdt", "hchgenddt")]
cst_hist[,(c("hchgdt", "hchgenddt")) := lapply(.SD, function(x) as.Date(x, format="%Y%m%d")), .SDcols=c("hchgdt", "hchgenddt")]
cst_hist[,crsp_table := "cst_hist"]
cst_hist[,hnaics := trimws(hnaics)]
cst_hist[,(c("fips","stinc")) := lapply(.SD, function(x) as.character(sprintf("%02.0f", x))), .SDcols=c("fips", "stinc")]

# merge in the FIPS codes for hstate;
fips = state.fips[,c("fips", "abb")]
fips = unique(fips)
setDT(fips)
setnames(fips, old="abb", "hstate")
fips[,fips := as.character(sprintf('%02.0f', fips))]
cst_hist = merge(cst_hist, fips, by="fips", all.x=T)

# merge in the FIPS codes for HINCORP;
setnames(fips, old=c("fips", "hstate"), new=c("stinc", "hincorp"))
cst_hist = merge(cst_hist, fips, by="stinc", all.x=T)

## merge in the country codes;
cs_country_codes = fread("~/CS_country_codes.csv", sep=",", stringsAsFactors=F)
cs_country_codes = cs_country_codes[,list(cntry_numeric_cd, cntry_alpha_cd)]
setnames(cs_country_codes, old=c("cntry_numeric_cd", "cntry_alpha_cd"), new=c("finc", "hfic"))
cst_hist = merge(cst_hist, cs_country_codes, by="finc", all.x=T)

## cleanup 
cst_hist[,c("stinc", "fips", "finc") := NULL]

#---------------------
# stack the datasets
#---------------------
crsp_hist = rbindlist(list(cst_hist, comphist), use.names=T, fill=T)

#-------------------------------------------------------------------
# create unique intervals for each gvkey, EIN, and name combination
#-------------------------------------------------------------------
crsp_hist = crsp_hist[,list(d_dt_start = min(hchgdt), 
                            crsp_dt_end = max(hchgenddt)), 
                       by=list(gvkey, hconm, hconml, 
                               smbl, zlist, hein, cnum, hnaics, hsic, hfic, 
                               hincorp, hstate, hcity, haddzip, crsp_table)]

#---------------------------
# fix overlapping intervals
#---------------------------
 
## find overlapping intervals
crsp_hist = crsp_hist[order(gvkey, -d_dt_start),]
crsp_hist[,ldt_start := shift(d_dt_start,1), by=list(gvkey)]
crsp_hist[,diff_dt_start := ifelse(is.na(crsp_dt_end) | crsp_dt_end == as.Date("9999-12-31"), NA, crsp_dt_end - ldt_start)]

## correct the overlaps 
crsp_hist[,d_dt_end := fifelse(diff_dt_start > 0 & !is.na(diff_dt_start), ldt_start -1, crsp_dt_end)]

## clean up
crsp_hist[,c("ldt_start", "diff_dt_start") := NULL]

#----------------
# Backfill items
#----------------

## copy items to backfill;
char_cols = c("hein_fill", "hstate_fill", "hfic_fill", "zlist_fill")
crsp_hist[,(char_cols) := .SD, .SDcols=c("hein", "hstate", "hfic", "zlist")]

# back fill;
crsp_hist = crsp_hist[order(gvkey, -d_dt_start),]
crsp_hist[ , (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
lev = sapply(char_cols, function(x) levels(crsp_hist[[x]]))
crsp_hist[ , (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
crsp_hist[ , (char_cols) := lapply(.SD, nafill, 'locf'), by = "gvkey", .SDcols = char_cols]
for (col in char_cols) set(crsp_hist, NULL, col, lev[[col]][crsp_hist[[col]]])

#--------------------
# Forward fill items
#--------------------

## copy items to forward fill;
char_cols = c("hein_fill", "hstate_fill", "hfic_fill", "zlist_fill")

# forward fill;
crsp_hist = crsp_hist[order(gvkey, d_dt_start),]
crsp_hist[ , (char_cols) := lapply(.SD, factor), .SDcols = char_cols]
lev = sapply(char_cols, function(x) levels(crsp_hist[[x]]))
crsp_hist[ , (char_cols) := lapply(.SD, as.integer), .SDcols = char_cols]
crsp_hist[ , (char_cols) := lapply(.SD, nafill, 'locf'), by = "gvkey", .SDcols = char_cols]
for (col in char_cols) set(crsp_hist, NULL, col, lev[[col]][crsp_hist[[col]]])

#---------------
# Final cleanup
#---------------
crsp_hist = crsp_hist[,list(gvkey, d_dt_start, d_dt_end, crsp_dt_end, 
                            hconml, hconm, smbl, zlist, hein, cnum, hnaics, hsic, 
                            hfic, hincorp, hstate, hcity, haddzip, crsp_table)]
crsp_hist = crsp_hist[order(gvkey, d_dt_start, d_dt_end),]

#------------------
# Check duplicates
#------------------
print(paste("Duplicates: ", any(duplicated(crsp_hist[,list(gvkey, d_dt_start)]))))

#-------------
# Export data
#-------------
write.table(crsp_hist, "crsp_hist.psv", sep="|", row.names = F)
saveRDS(crsp_hist, "crsp_hist.rds")
