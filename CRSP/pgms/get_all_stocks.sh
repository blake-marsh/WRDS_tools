#!/bin/bash


#$ -cwd

#$ -N CRSP_stocks
#$ -o ~/CRSP/logfiles/get_all_stocks.o.log
#$ -e ~/CRSP/logfiles/get_all_stocks.e.log


cd ~/CRSP/
R CMD BATCH --no-save --no-restore ./pgms/get_all_stocks.R ./logfiles/get_all_stocks.Rout



