#!/bin/bash


#$ -cwd

#$ -N CRSP_bank_stocks
#$ -o ~/CRSP/logfiles/get_bank_stocks.o.log
#$ -e ~/CRSP/logfiles/get_bank_stocks.e.log


cd ~/CRSP/
R CMD BATCH --no-save --no-restore ./pgms/get_bank_stocks.R ./logfiles/get_bank_stocks.Rout


