import os, shlex, subprocess
import argparse
import runTestcases
import json


# runTests(outputDir,
#          log4jBaseDir,
#          master,
#          testFilePath,
#          testCaseName,
#          isFreq=False,
#          minMinSup=0,
#          pfp=False,
#          song=False,
#          setcover=0,
#          minSupport = [0.001],
#          coresExecutorMemNums = [(40,4,'20g')],
#          partitions = [1000]
#          )

def runTestsAutomation(cfgFile):
    with open(cfgFile) as f:
        data = json.load(f)
        for test in data['testcases']:
            for testParams in test['testParams']:
                print((testParams['coresExecutorMemNums'][0],testParams['coresExecutorMemNums'][1],testParams['coresExecutorMemNums'][2],testParams['coresExecutorMemNums'][3]))
                runTestcases.runTests(data['outputdir'],
                                      data['log4jbasedir'],
                                      data['master'],
                                      test['testpath'],
                                      test['testname'],
                                      testParams['freq'],
                                      testParams['minMinSup'],
                                      testParams['pfp'],
                                      testParams['song'],
                                      testParams['set-cover'],
                                      testParams['minSupport'],
                                      [(testParams['coresExecutorMemNums'][0],testParams['coresExecutorMemNums'][1],testParams['coresExecutorMemNums'][2],testParams['coresExecutorMemNums'][3])],
                                      testParams['partitions'],
                                      testParams['collectStatistics']==1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run automation from json file.')
    parser.add_argument('--run-cfg', dest='runcfg', help='logs output dir')
    args = parser.parse_args()
    runTestsAutomation(args.runcfg)
