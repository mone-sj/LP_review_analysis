#!/bin/bash
#path setting
PATH=/usr/bin:/bin:/usr/sbin:/usr/local/bin

export LANG=ko_KR.utf8

chmod +x /home/LP_review_analysis/cron/analy.sh

service cron start
