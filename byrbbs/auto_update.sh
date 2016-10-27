#!/bin/bash

export PATH=/usr/local/bin:$PATH

spider='updatespider'
user_spider='userupdate'

cd /root/byrbbs
python starter.py $spider
python starter.py $user_spider
#python starter.py allspider
#python starter.py userspider
