#!/bin/bash

export PATH=/usr/local/bin:$PATH

user_spider='userspider'

cd /root/byrbbs
python starter.py $user_spider
