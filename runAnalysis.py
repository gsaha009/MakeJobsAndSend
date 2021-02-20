import os
import yaml
import argparse
import logging
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%m/%d/%Y %H:%M:%S')

parser = argparse.ArgumentParser(description='Make Jobs and Send')

parser.add_argument('--configName', action='store', required=True, type=str, help='Name of the config')
parser.add_argument('--useHTcondor', action='store_true', required=False, help='to send jobs to HT-Condor')

args = parser.parse_args()

pwd = os.getcwd()
logging.info('present working dir : {}'.format(pwd))

with open(args.configName, 'r') as config:
    configDict = yaml.safe_load(config)

keyList = [str(key) for key in configDict.keys()]
logging.info('Config yaml keys : {}'.format(keyList))

era            = configDict.get('era')
lumi           = configDict.get('lumi')
tree           = configDict.get('tree')
commonInfoList = configDict.get('commonInfo')
mvaInfoList    = configDict.get('mvaInfo')
endInfoList    = configDict.get('endInfo')
cutLists       = configDict.get('cutLists')

logging.info('era  : {}'.format(era))
logging.info('lumi : {} pb-1'.format(lumi))
logging.info('tree : {}'.format(tree))

samplesDict    = configDict.get('samplesDict')
dataTypes      = [str(sample) for sample in samplesDict.keys()]
logging.info('dataTypes : {}'.format(dataTypes))
mcSamplesDict     = samplesDict.get('MC')
dataSamplesDict   = samplesDict.get('DATA')
signalSamplesDict = samplesDict.get('SIGNAL')

logging.info('MC_Samples : {}'.format([sample for sample in mcSamplesDict.keys()]))
logging.info('Signal_Samples : {}'.format([sample for sample in signalSamplesDict.keys()]))
logging.info('Data_Samples : {}'.format([sample for sample in dataSamplesDict.keys()]))

# mc samples
logging.info('Start making job cards for MC Bkg samples ===>')
for key, val in mcSamplesDict.items():
    logging.info('... sample : {}'.format(key))
    filePathList = val.get('filedirs')
    xsec         = val.get('xsec')
    evtWtSum     = val.get('genEvtWtSum')
    split        = int(val.get('split'))
    files        = []
    for item in filePathList:
        logging.info('\t {}'.format(item))
        files += [os.path.join(item,rfile) for rfile in os.listdir(item) if '.root' in rfile]
    jobFile = str(key)+'.job'
    jobDir  = 'JobCards_'+str(era) 
    if not os.path.isdir(jobDir):
        os.mkdir(jobDir)
    with open(os.path.join(jobDir,jobFile), 'w') as jfile:
        jfile.write('START\n'+'era '+str(era)+'\n'+'dataType mc\n')
        for item in commonInfoList:
            jfile.write(item+'\n') 
        jfile.write('############ MVA Info ###############'+'\n')
        for item in mvaInfoList:
            jfile.write(item+'\n')
        jfile.write('########### xsec,lumi,hist ###########\n')
        jfile.write('lumiWtList xsec='+str(xsec)+' intLumi='+str(lumi)+' nevents=100000'+'\n')
        jfile.write('histFile '+str(key)+'_hist.root'+'\n')
        jfile.write('logFile '+str(key)+'_dump.log'+'\n')
        jfile.write('############ Cut lists ###############'+'\n')
        for item in cutLists:
            jfile.write(item+'\n')
        jfile.write('############ Input Files ##############'+'\n')
        for item in files:
            jfile.write('inputFile '+item+'\n')
        jfile.write('########################################\n')
        for item in endInfoList:
            jfile.write(item+'\n')
        jfile.write('END')

    jfile.close()
    # jobfile production finished
    # Now prepare to send jobs to condor
    conDir = os.path.join(jobDir, str(key)+'_condorJobs')
    inputFile = str(key)+'_infiles.list'
    with open(os.path.join(conDir, inputFile), 'w') as infile:
        for file in files:
            infile.write(file+'\n')
    infile.close()
    if not os.path.isdir(conDir):
        os.mkdir(conDir)
    if not os.path.isdir(os.path.join(conDir,'log')):
        os.mkdir(os.path.join(conDir,'log'))
    if not os.path.isdir(os.path.join(conDir,'output')):
        os.mkdir(os.path.join(conDir,'output'))
    if not os.path.isdir(os.path.join(conDir,'error')):
        os.mkdir(os.path.join(conDir,'error'))

# data samples
logging.info('Start making job cards for data ===>')
for key, val in dataSamplesDict.items():
    logging.info('... sample : {}'.format(key))
    filePathList = val.get('filedirs')
    xsec         = -999.9
    evtWtSum     = 'bla'
    split        = int(val.get('split'))
    files = []
    for item in filePathList:
        logging.info('\t {}'.format(item))
        files += [os.path.join(item,rfile) for rfile in os.listdir(item) if '.root' in rfile]
    inputFile = str(key)+'_infiles.list'
    jobFile = str(key)+'.job'
    jobDir  = 'JobCards_'+str(era) 
    if not os.path.isdir(jobDir):
        os.mkdir(jobDir)
    with open(os.path.join(jobDir, inputFile), 'w') as infile:
        for file in files:
            infile.write(file+'\n')
    infile.close()
    with open(os.path.join(jobDir,jobFile), 'w') as jfile:
        jfile.write('START\n'+'era '+str(era)+'\n'+'dataType data\n')
        for item in commonInfoList:
            jfile.write(item+'\n') 
        jfile.write('############ MVA Info ###############'+'\n')
        for item in mvaInfoList:
            jfile.write(item+'\n')
        jfile.write('########### xsec,lumi,hist ###########\n')
        jfile.write('lumiWtList xsec='+str(xsec)+' intLumi='+str(lumi)+' nevents=100000'+'\n')
        jfile.write('histFile '+str(key)+'_hist.root'+'\n')
        jfile.write('logFile '+str(key)+'_dump.log'+'\n')
        jfile.write('############ Cut lists ###############'+'\n')
        for item in cutLists:
            jfile.write(item+'\n')
        jfile.write('############ Input Files ##############'+'\n')
        for item in files:
            jfile.write('inputFile '+item+'\n')
        jfile.write('########################################\n')
        for item in endInfoList:
            jfile.write(item+'\n')
        jfile.write('END')

    jfile.close()

# signal samples
logging.info('Start making job cards for Signal samples ===>')
for key, val in signalSamplesDict.items():
    logging.info('... sample : {}'.format(key))
    filePathList = val.get('filedirs')
    xsec         = val.get('xsec')
    evtWtSum     = val.get('genEvtWtSum')
    split        = int(val.get('split'))
    files = []
    for item in filePathList:
        logging.info('\t {}'.format(item))
        files += [os.path.join(item,rfile) for rfile in os.listdir(item) if '.root' in rfile]
    inputFile = str(key)+'_infiles.list'
    jobFile = str(key)+'.job'
    jobDir  = 'JobCards_'+str(era) 
    if not os.path.isdir(jobDir):
        os.mkdir(jobDir)
    with open(os.path.join(jobDir, inputFile), 'w') as infile:
        for file in files:
            infile.write(file+'\n')
    infile.close()
    with open(os.path.join(jobDir,jobFile), 'w') as jfile:
        jfile.write('START\n'+'era '+str(era)+'\n'+'dataType mc#signal\n')
        for item in commonInfoList:
            jfile.write(item+'\n') 
        jfile.write('############ MVA Info ###############'+'\n')
        for item in mvaInfoList:
            jfile.write(item+'\n')
        jfile.write('########### xsec,lumi,hist ###########\n')
        jfile.write('lumiWtList xsec='+str(xsec)+' intLumi='+str(lumi)+' nevents=100000'+'\n')
        jfile.write('histFile '+str(key)+'_hist.root'+'\n')
        jfile.write('logFile '+str(key)+'_dump.log'+'\n')
        jfile.write('############ Cut lists ###############'+'\n')
        for item in cutLists:
            jfile.write(item+'\n')
        jfile.write('############ Input Files ##############'+'\n')
        for item in files:
            jfile.write('inputFile '+item+'\n')
        jfile.write('########################################\n')
        for item in endInfoList:
            jfile.write(item+'\n')
        jfile.write('END')

    jfile.close()
