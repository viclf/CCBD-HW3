#!/usr/bin/env python
import os
import sys
import boto
from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
import time
from datetime import datetime, timedelta


#take tweets from DB and write them into a file

#db connection
keyid='...'
secretkey='...'
db = connect_to_region('us-east-1', aws_access_key_id=keyid, aws_secret_access_key=secretkey)
tweetsTable = Table('TweetsTable',connection=db)

#get tweets
print 'scanning'

TweetsNov=[]
TweetsOct=[]
result = tweetsTable.scan()
for x in result:
    if 'Oct' in x['time'] and '#' in x['text']:
        TweetsOct.append([x['text']])
    if 'Nov' in x['time'] and '#' in x['text']:
        TweetsNov.append([x['text']])


#write into two file
print 'writing'

fileOct = open('Octinput.txt','w')
fileNov = open('Novinput.txt','w')

for item in TweetsOct:
    fileOct.write("%s\n" % item[0].encode('utf8'))

for item in TweetsNov:
    fileNov.write("%s\n" % item[0].encode('utf8'))