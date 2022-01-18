#!/bin/bash
#path setting
PATH=/usr/bin:/bin:/usr/sbin:/usr/local/bin

#run cron
cron -f

python3 /home/asc/LP_review_analysis/exe.py
