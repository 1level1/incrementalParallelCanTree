import os, shlex, subprocess
import argparse

def prepareLogFile(logFilePath,errorFilePath,outputFilePath):
    log4jProperties = 'log4j.rootCategory=INFO,FILE,R' + os.linesep + \
                      'log4j.appender.FILE=org.apache.log4j.FileAppender'+ os.linesep + \
                      'log4j.appender.FILE.File={logFilePath}' + os.linesep + \
                      'log4j.appender.FILE.ImmediateFlush=true' + os.linesep + \
                      'log4j.appender.FILE.MaxFileSize=10MB' + os.linesep + \
                      'log4j.appender.FILE.MaxBackupIndex=10' + os.linesep + \
                      'log4j.appender.FILE.layout=org.apache.log4j.PatternLayout' + os.linesep + \
                      'log4j.appender.FILE.layout.conversionPattern=%m%n' + os.linesep + \
                      'log4j.appender.R=org.apache.log4j.RollingFileAppender' + os.linesep + \
                      'log4j.appender.R.File={errorFilePath}' + os.linesep + \
                      'log4j.appender.R.MaxFileSize=10MB' + os.linesep + \
                      'log4j.appender.R.layout=org.apache.log4j.PatternLayout' + os.linesep + \
                      'log4j.appender.R.layout.ConversionPattern=%p %t %c - %m%n' + os.linesep + \
                      'log4j.logger.org.spark-project.jetty=WARN' + os.linesep + \
                      'log4j.logger.org.spark-project.jetty.util.component.AbstractLifeCycle=ERROR' + os.linesep + \
                      'log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO' + os.linesep + \
                      'log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO' + os.linesep + \
                      'log4j.logger.com.vmeg.code=DEBUG'
    solved= log4jProperties.format(logFilePath=logFilePath, errorFilePath=errorFilePath)
    with open(outputFilePath,'w') as of:
        of.write(solved)
    return

def runJob(fileListPath,master,driverMemory,executorMemory,numExecutors,minSupport,numPartitions,cores,log4jFilePath,isPFP=False):
    prefixcmd = 'spark-submit --class \"CanTreeMain\" ' \
          '--master {master} '\
          '--driver-memory {driverMemory} ' \
          '--executor-memory {executorMemory} ' \
          '--num-executors {numExecutors} ' \
          '--executor-cores {cores} ' \
          '--conf \"spark.driver.extraJavaOptions=-Dlog4j.configuration=file:{log4jFilePath} -Xss1G\" ' \
          '--conf \"spark.executor.extraJavaOptions=-Dlog4j.configuration=file:{log4jFilePath} -Xss1G\" '.format(master=master,
                                                                                                        driverMemory=driverMemory,
                                                                                                        executorMemory=executorMemory,
                                                                                                        numExecutors=numExecutors,
                                                                                                        cores=cores,
                                                                                                        log4jFilePath=log4jFilePath)
    jarFile =  'target/scala-2.11/cantree_2.11-0.1.jar'
    postcmd =' --num-partitions {numPartitions} ' \
             '--min-support {minSupport} ' \
             '--in-file-list-path {fileListPath}'.format(numPartitions=numPartitions,
                                                         minSupport=minSupport,
                                                         fileListPath=fileListPath)
    if isPFP:
        postcmd += ' --pfp 1'
    cmdList = shlex.split(prefixcmd)
    cmdList.append(jarFile)
    cmdList+=(shlex.split(postcmd))
    try:
        print(cmdList)
        subprocess.check_output(cmdList)
    except subprocess.CalledProcessError as e:
        print("-Error- \n"+str(e.cmd))
        print(e.output)


def runTests(outputDir,log4jBaseDir,master,testFilePath,testCaseName):
    minSupport = [0.1,0.01,0.001]
    coresExecutorMemNums = [(40,4,'20g')]
    partitions = [1,100,1000,10000]
    log4jPath = 'src/main/resources/log4j_file.properties'
    testCaseFiles = [(testFilePath,testCaseName)]
    for supp in minSupport:
        for cem in coresExecutorMemNums:
            cores,execNums,mem = cem
            for partition in partitions:
                for testCase in testCaseFiles:
                    testFile,testName = testCase
                    testname = '_'.join([testName,str(partition),str(cores),str(execNums),mem,str(supp).replace('.','_')])
                    log4jErrorFileName = os.path.join(outputDir,testname+'_error.txt')
                    log4jFileName = os.path.join(outputDir,testname+'.txt')
                    prepareLogFile(log4jFileName,log4jErrorFileName,os.path.join(log4jBaseDir,log4jPath))
                    runJob(testFile,master,mem,mem,execNums,supp,partition,cores,log4jPath)
                    log4jErrorFileName = os.path.join(outputDir,'PFP_'+testname+'_error.txt')
                    log4jFileName = os.path.join(outputDir,'PFP_'+testname+'.txt')
                    prepareLogFile(log4jFileName,log4jErrorFileName,os.path.join(log4jBaseDir,log4jPath))
                    runJob(testFile,master,mem,mem,execNums,supp,partition,cores,log4jPath,True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run FIS test cases.')
    parser.add_argument('--outputdir', dest='outputdir', help='logs output dir')
    parser.add_argument('--log4jbasedir', dest='log4jBaseDir',help='base dir of cantree dir')
    parser.add_argument('--master', dest='master', help='master host')
    parser.add_argument('--testpath', dest='testpath', help='master host')
    parser.add_argument('--testname', dest='testname', help='master host')
    # outputDir,log4jBaseDir,master
    args = parser.parse_args()
    runTests(args.outputdir,args.log4jBaseDir,args.master)




