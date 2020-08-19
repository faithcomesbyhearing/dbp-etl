

#WARNING: plain_text filesetId INZSHL has no verses.
#WARNING: plain_text filesetId SPANVI has no verses.
#WARNING: plain_text filesetId TTQWBT has no verses.
import os
import io
import sys
from Config import *


config = Config("dev")
filename = config.directory_bucket_list + os.sep + "dbp-vid.txt" 
total = 0.0
fp = open(filename, "r")
for line in fp:
	size = float(line.split("\t")[1])
	total += size
fp.close()
print(total)



#2,881,420,648,281 dbp-prod

#11,737,978,482,989 dbp-vid