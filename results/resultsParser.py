import re
import datetime
import csv
import os
import glob

def parseResultsFile(fileName):
    pat = "^(.*)-iterate.*Found"
    res = []
    prev = None
    with open(fileName,'r') as fp:
        for line in fp:
           match = re.match(pat,line)
           if (match):
               if not prev:
                   prev = datetime.datetime.strptime(match.group(1),'%Y-%m-%dT%H:%M:%S.%f')
               else:
                   curr = datetime.datetime.strptime(match.group(1),'%Y-%m-%dT%H:%M:%S.%f')
                   res.append((curr-prev).seconds)
                   prev = curr
    return res

def createCsv(inputDirPattern):
    res = []
    for f in glob.glob(inputDirPattern):
        fileName = os.path.basename(f)
        timesRes = parseResultsFile(f)
        timesRes.insert(0,fileName)
        res.append(timesRes)
    return res

res = createCsv("/home/lev/Documents/teza/dataset/automation_t20/parsed/*.txt")
with open("/home/lev/Documents/teza/dataset/automation_t20/parsed/output_17_1_21.csv", "w") as f:
    writer = csv.writer(f)
    for r in res:
        writer.writerow(r)

