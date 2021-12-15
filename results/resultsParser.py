import re
import datetime
import csv
import os
import glob
import argparse


HEADER = ["FILENAME",1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21]
def parseResultsFile(fileName):
    # Found 579753 at iteration number 0 (1595688,1622591.5,1634928) Partitions: 10
    #pat = "^(.*)-iterate.*Found.*\(([+-]?([0-9]*[.])?[0-9]+,[+-]?([0-9]*[.])?[0-9]+,[+-]?([0-9]*[.])?[0-9]+\)"
    patTime = "^(.*)-iterate.*Found"
    res = []
    prev = None
    with open(fileName,'r') as fp:
        for line in fp:
           match = re.match(patTime,line)
           if (match):
               if not prev:
                   prev = datetime.datetime.strptime(match.group(1),'%Y-%m-%dT%H:%M:%S.%f')
               else:
                   curr = datetime.datetime.strptime(match.group(1),'%Y-%m-%dT%H:%M:%S.%f')
                   res.append((curr-prev).seconds)
                   prev = curr
    return res

def parseStatisticsData(fileName):
    patNodes = ".*([0-9]*[.]?[0-9]+,[0-9]*[.]?[0-9]+,[0-9]*[.]?[0-9]+).* Partitions: (\d+)"
    resMin = []
    resMid = []
    resMax = []
    resPartitionsSize = []
    with open(fileName,'r') as fp:
        for line in fp:
           matchNodes = re.match(patNodes,line)
           if (matchNodes):
                (treeStat, part) = matchNodes.groups()
                (minNodes,midNodes,maxNodes) = treeStat.split(',')
                resMin.append(int(minNodes))
                resMid.append(float(midNodes))
                resMax.append(int(maxNodes))
                resPartitionsSize.append(int(part))
    return (resMin,resMid,resMax,resPartitionsSize)


def createCsv(inputDirPattern):
    resTime = [HEADER]
    resStat = [HEADER]
    for f in glob.glob(inputDirPattern):
        fileName = os.path.basename(f)
        timesRes = parseResultsFile(f)
        timesRes.insert(0, fileName)
        (resMin, resMid, resMax, resPartitionsSize) = parseStatisticsData(f)
        resMin.insert(0,fileName+"_min")
        resMid.insert(0, fileName+"_mid")
        resMax.insert(0, fileName+"_max")
        resPartitionsSize.insert(0, fileName+"_partitions")
        resTime.append(timesRes)
        resStat.append(resMin)
        resStat.append(resMid)
        resStat.append(resMax)
        resStat.append(resPartitionsSize)
    return (resTime,resStat)


def runParsing(resultsDir):
    from time import gmtime, strftime
    resTime,resStat = createCsv(resultsDir+"/*.txt")
    sufix = strftime("_%d_%m_%Y_%H_%M_%S.csv", gmtime())
    resTimeOutput = resultsDir + "/time_output_"+sufix
    resStatOutput = resultsDir + "/stat_output_" + sufix
    with open(resTimeOutput, "w") as f:
        writer = csv.writer(f)
        for r in resTime:
            writer.writerow(r)
    with open(resStatOutput, "w") as f:
        writer = csv.writer(f)
        for r in resStat:
            writer.writerow(r)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run parsing of results file.')
    parser.add_argument('--results-dir', dest='resdir', help='results directory - with ')
    args = parser.parse_args()
    runParsing(args.resdir)
