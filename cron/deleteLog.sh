#!/bin/bash
cd /var/log
find ./*.log -mtime +7 -exec rm {} \;