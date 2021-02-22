#################################################
### A script to make the job submissions easy ###
###               Gourab Saha                 ###
#################################################
import os
import yaml
import argparse
import shutil
import fileinput
import sys
import stat
from subprocess import Popen, PIPE
import logging
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%m/%d/%Y %H:%M:%S')

def replaceAll(file,searchExp,replaceExp):
    for line in fileinput.input(file, inplace=1):
        if searchExp in line:
            line = line.replace(searchExp,replaceExp)
        sys.stdout.write(line)

def main():
    parser = argparse.ArgumentParser(description='Make Jobs and Send')
    
    parser.add_argument('--configName', action='store', required=True, type=str, help='Name of the config')
    parser.add_argument('--suffix', action='store', required=True, type=str, help='set jobdir name')
    parser.add_argument('--send', action='store_true', required=False, help='send jobs to HT-Condor')
    
    args = parser.parse_args()
    
    pwd = os.getcwd()
    logging.info('present working dir : {}'.format(pwd))
    
    with open(args.configName, 'r') as config:
        configDict = yaml.safe_load(config)

    keyList = [str(key) for key in configDict.keys()]
    
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
    
    appdir            = configDict.get('appDir')
    jobdir            = configDict.get('jobDir')
    exeToRun          = configDict.get('exeToRun')
    outdir            = configDict.get('outDir')
    samplesDict       = configDict.get('samplesDict')
    dataTypes         = [str(sample) for sample in samplesDict.keys()]
    mcSamplesDict     = samplesDict.get('MC')
    dataSamplesDict   = samplesDict.get('DATA')
    signalSamplesDict = samplesDict.get('SIGNAL')
    
    logging.info('MC_Samples : {}'.format([sample for sample in mcSamplesDict.keys()]))
    logging.info('Signal_Samples : {}'.format([sample for sample in signalSamplesDict.keys()]))
    logging.info('Data_Samples : {}'.format([sample for sample in dataSamplesDict.keys()]))

    jobDir  = os.path.join(pwd,'JobCards_'+str(era)+'_'+args.suffix) 
    if not os.path.isdir(jobDir):
        os.mkdir(jobDir)
        logging.info('{} : job directory created'.format(jobDir))
    else:
        logging.info('job directory exists .')
    histDir = os.path.join(outdir,'JobOutput_'+str(era)+'_'+args.suffix)
    if not os.path.isdir(histDir):
        os.mkdir(histDir)
        logging.info('{} : out directory created'.format(histDir))
    else:
        logging.info('out directory exists .')
    
    # mc samples
    logging.info('Start making job cards for MC Bkg samples ===>')
    for key, val in mcSamplesDict.items():
        logging.info(' Sample : {}'.format(key))
        filePathList = val.get('filedirs')
        xsec         = val.get('xsec')
        evtWtSum     = val.get('genEvtWtSum')
        filesPerJob  = int(val.get('filesPerJob'))
        files        = []
        for item in filePathList:
            logging.info('\t {}'.format(item))
            files += [os.path.join(item,rfile) for rfile in os.listdir(item) if '.root' in rfile]
        jobFile = str(key)+'.job'
        with open(os.path.join(jobDir,jobFile), 'w') as jfile:
            jfile.write('START\n'+'era '+str(era)+'\n'+'dataType mc\n')
            for item in commonInfoList:
                jfile.write(item+'\n') 
            jfile.write('############ MVA Info ###############'+'\n')
            for item in mvaInfoList:
                jfile.write(item+'\n')
            jfile.write('########### xsec,lumi,hist ###########\n')
            jfile.write('lumiWtList xsec='+str(xsec)+' intLumi='+str(lumi)+' nevents=100000'+'\n')
            jfile.write('histFile '+histDir+'/'+str(key)+'_hist.root'+'\n')
            jfile.write('logFile '+histDir+'/'+str(key)+'_dump.log'+'\n')
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
        if not os.path.isdir(conDir):
            os.mkdir(conDir)
        tmplsubFile = os.path.join(conDir,str(key)+'.sub.tmpl')
        tmplshFile  = os.path.join(conDir,str(key)+'.sh.tmpl')
        shutil.copy(os.path.join('CondorTemplateScripts','SAMPLE.sub.tmpl'), tmplsubFile)
        shutil.copy(os.path.join('CondorTemplateScripts','SAMPLE.sh.tmpl'), tmplshFile)
        inputFile = str(key)+'_infiles.list'
        with open(os.path.join(conDir, inputFile), 'w') as infile:
            for file in files:
                infile.write(file+'\n')
        infile.close()
        if not os.path.isdir(os.path.join(conDir,'log')):
            os.mkdir(os.path.join(conDir,'log'))
        if not os.path.isdir(os.path.join(conDir,'output')):
            os.mkdir(os.path.join(conDir,'output'))
        if not os.path.isdir(os.path.join(conDir,'error')):
            os.mkdir(os.path.join(conDir,'error'))
        if not os.path.isdir(os.path.join(conDir,'runlogs')):
            os.mkdir(os.path.join(conDir,'runlogs'))
        infileListPerJob = [files[i:i+filesPerJob] for i in range(0, len(files), filesPerJob)]
        logging.info('\t nFiles : {} || nJobs : {}'.format(len(files),len(infileListPerJob)))
        for i, filesList in enumerate(infileListPerJob):
            jobkey = os.path.join(conDir,str(key)+'_'+str(i)+'.job')
            subkey = os.path.join(conDir,str(key)+'_'+str(i)+'.sub')
            shkey  = os.path.join(conDir,str(key)+'_'+str(i)+'.sh')
            with open(jobkey, 'w') as tmpl:
                tmpl.write('START\n'+'era '+str(era)+'\n'+'dataType mc\n')
                for item in commonInfoList:
                    tmpl.write(item+'\n') 
                tmpl.write('############ MVA Info ###############'+'\n')
                for item in mvaInfoList:
                    tmpl.write(item+'\n')
                tmpl.write('########### xsec,lumi,hist ###########\n')
                tmpl.write('lumiWtList xsec='+str(xsec)+' intLumi='+str(lumi)+' nevents=100000'+'\n')
                tmpl.write('histFile '+histDir+'/'+str(key)+'_'+str(i)+'_hist.root'+'\n')
                tmpl.write('logFile '+histDir+'/'+str(key)+'_'+str(i)+'_dump.log'+'\n')
                tmpl.write('############ Cut lists ###############'+'\n')
                for item in cutLists:
                    tmpl.write(item+'\n')
                tmpl.write('############ Input Files ##############'+'\n')
                for item in filesList:
                    tmpl.write('inputFile '+item+'\n')
                tmpl.write('########################################\n')
                for item in endInfoList:
                    tmpl.write(item+'\n')
                tmpl.write('END')
            tmpl.close()
        
            shutil.copy(tmplsubFile, subkey)
            shutil.copy(tmplshFile, shkey)
            # edit the sub.tmpl file
            replaceAll(subkey, 'executable   = sample_index.sh', 'executable   = '+shkey.split('/')[-1])
            replaceAll(subkey, 'output       = output/sample_INDEX.$(ClusterId).$(ProcId).out', 'output       = output/'+str(key)+'_'+str(i)+'.$(ClusterId).$(ProcId).out')
            replaceAll(subkey, 'error        = error/sample_INDEX.$(ClusterId).$(ProcId).err', 'error        = error/'+str(key)+'_'+str(i)+'.$(ClusterId).$(ProcId).err')
            replaceAll(subkey, 'log          = log/sample_INDEX.$(ClusterId).log', 'log          = log/'+str(key)+'_'+str(i)+'.$(ClusterId).log')
            # edit the sh.tmpl file
            replaceAll(shkey, 'JOBDIR=NameOfJobDirGivenInYaml', 'JOBDIR='+jobdir)
            replaceAll(shkey, 'APPDIR=NameOfAppDirGivenInYaml', 'APPDIR='+appdir)
            #replaceAll(shkey, 'cd $JOBDIR/condor_runlog_dir', 'cd $JOBDIR/'+pwd.split('/')[-1]+'/'+conDir+'/'+'runlogs')
            replaceAll(shkey, 'cd $JOBDIR/condor_runlog_dir', 'cd '+os.path.join(conDir,'runlogs'))
            replaceAll(shkey, 'uname -a > ./sample_INDEX.runlog 2>&1', 'uname -a > ./'+str(key)+'_'+str(i)+'.runlog 2>&1')
            #replaceAll(shkey, '$JOBDIR/EXE $JOBDIR/PathToJobFile/sample_index.job >> ./sample_index.runlog 2>&1','$JOBDIR/'+exeToRun+' $JOBDIR/'+jobkey+' >> ./'+str(key)+'_'+str(i)+'.runlog 2>&1')
            replaceAll(shkey, '$JOBDIR/EXE $JOBDIR/PathToJobFile/sample_index.job >> ./sample_index.runlog 2>&1', exeToRun+' '+jobkey+' >> ./'+str(key)+'_'+str(i)+'.runlog 2>&1')

            # All job files, sub and sh files are ready
            # Now SHOOT !
            if args.send :
                condorJobCommandList = ['condor_submit',subkey]
                st = os.stat(shkey)
                os.chmod(shkey, st.st_mode | stat.S_IEXEC)
                process = Popen(condorJobCommandList, stdout=PIPE, stderr=PIPE)

    # data samples
    logging.info('Start making job cards for data ===>')
    for key, val in dataSamplesDict.items():
        logging.info(' Sample : {}'.format(key))
        filePathList = val.get('filedirs')
        xsec         = -999.9
        evtWtSum     = 'bla'
        filesPerJob  = int(val.get('filesPerJob'))
        files = []
        for item in filePathList:
            logging.info('\t {}'.format(item))
            files += [os.path.join(item,rfile) for rfile in os.listdir(item) if '.root' in rfile]
        jobFile = str(key)+'.job'
        with open(os.path.join(jobDir,jobFile), 'w') as jfile:
            jfile.write('START\n'+'era '+str(era)+'\n'+'dataType data\n')
            for item in commonInfoList:
                jfile.write(item+'\n') 
            jfile.write('############ MVA Info ###############'+'\n')
            for item in mvaInfoList:
                jfile.write(item+'\n')
            jfile.write('########### xsec,lumi,hist ###########\n')
            jfile.write('lumiWtList xsec='+str(xsec)+' intLumi='+str(lumi)+' nevents=100000'+'\n')
            jfile.write('histFile '+histDir+'/'+str(key)+'_hist.root'+'\n')
            jfile.write('logFile '+histDir+'/'+str(key)+'_dump.log'+'\n')
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
        if not os.path.isdir(conDir):
            os.mkdir(conDir)
        tmplsubFile = os.path.join(conDir,str(key)+'.sub.tmpl')
        tmplshFile  = os.path.join(conDir,str(key)+'.sh.tmpl')
        shutil.copy(os.path.join('CondorTemplateScripts','SAMPLE.sub.tmpl'), tmplsubFile)
        shutil.copy(os.path.join('CondorTemplateScripts','SAMPLE.sh.tmpl'), tmplshFile)
        inputFile = str(key)+'_infiles.list'
        with open(os.path.join(conDir, inputFile), 'w') as infile:
            for file in files:
                infile.write(file+'\n')
        infile.close()
        if not os.path.isdir(os.path.join(conDir,'log')):
            os.mkdir(os.path.join(conDir,'log'))
        if not os.path.isdir(os.path.join(conDir,'output')):
            os.mkdir(os.path.join(conDir,'output'))
        if not os.path.isdir(os.path.join(conDir,'error')):
            os.mkdir(os.path.join(conDir,'error'))
        if not os.path.isdir(os.path.join(conDir,'runlogs')):
            os.mkdir(os.path.join(conDir,'runlogs'))
        infileListPerJob = [files[i:i+filesPerJob] for i in range(0, len(files), filesPerJob)]
        logging.info('\t nFiles : {} || nJobs : {}'.format(len(files),len(infileListPerJob)))
        for i, filesList in enumerate(infileListPerJob):
            jobkey = os.path.join(conDir,str(key)+'_'+str(i)+'.job')
            subkey = os.path.join(conDir,str(key)+'_'+str(i)+'.sub')
            shkey  = os.path.join(conDir,str(key)+'_'+str(i)+'.sh')
            with open(jobkey, 'w') as tmpl:
                tmpl.write('START\n'+'era '+str(era)+'\n'+'dataType data\n')
                for item in commonInfoList:
                    tmpl.write(item+'\n') 
                tmpl.write('############ MVA Info ###############'+'\n')
                for item in mvaInfoList:
                    tmpl.write(item+'\n')
                tmpl.write('########### xsec,lumi,hist ###########\n')
                tmpl.write('lumiWtList xsec='+str(xsec)+' intLumi='+str(lumi)+' nevents=100000'+'\n')
                tmpl.write('histFile '+histDir+'/'+str(key)+'_'+str(i)+'_hist.root'+'\n')
                tmpl.write('logFile '+histDir+'/'+str(key)+'_'+str(i)+'_dump.log'+'\n')
                tmpl.write('############ Cut lists ###############'+'\n')
                for item in cutLists:
                    tmpl.write(item+'\n')
                tmpl.write('############ Input Files ##############'+'\n')
                for item in filesList:
                    tmpl.write('inputFile '+item+'\n')
                tmpl.write('########################################\n')
                for item in endInfoList:
                    tmpl.write(item+'\n')
                tmpl.write('END')
            tmpl.close()
        
            shutil.copy(tmplsubFile, subkey)
            shutil.copy(tmplshFile, shkey)
            # edit the sub.tmpl file
            replaceAll(subkey, 'executable   = sample_index.sh', 'executable   = '+shkey.split('/')[-1])
            replaceAll(subkey, 'output       = output/sample_INDEX.$(ClusterId).$(ProcId).out', 'output       = output/'+str(key)+'_'+str(i)+'.$(ClusterId).$(ProcId).out')
            replaceAll(subkey, 'error        = error/sample_INDEX.$(ClusterId).$(ProcId).err', 'error        = error/'+str(key)+'_'+str(i)+'.$(ClusterId).$(ProcId).err')
            replaceAll(subkey, 'log          = log/sample_INDEX.$(ClusterId).log', 'log          = log/'+str(key)+'_'+str(i)+'.$(ClusterId).log')
            # edit the sh.tmpl file
            replaceAll(shkey, 'JOBDIR=NameOfJobDirGivenInYaml', 'JOBDIR='+jobdir)
            replaceAll(shkey, 'APPDIR=NameOfAppDirGivenInYaml', 'APPDIR='+appdir)
            #replaceAll(shkey, 'cd $JOBDIR/condor_runlog_dir', 'cd $JOBDIR/'+pwd.split('/')[-1]+'/'+conDir+'/'+'runlogs')
            replaceAll(shkey, 'cd $JOBDIR/condor_runlog_dir', 'cd '+os.path.join(conDir,'runlogs'))
            replaceAll(shkey, 'uname -a > ./sample_INDEX.runlog 2>&1', 'uname -a > ./'+str(key)+'_'+str(i)+'.runlog 2>&1')
            #replaceAll(shkey, '$JOBDIR/EXE $JOBDIR/PathToJobFile/sample_index.job >> ./sample_index.runlog 2>&1','$JOBDIR/'+exeToRun+' $JOBDIR/'+jobkey+' >> ./'+str(key)+'_'+str(i)+'.runlog 2>&1')
            replaceAll(shkey, '$JOBDIR/EXE $JOBDIR/PathToJobFile/sample_index.job >> ./sample_index.runlog 2>&1', exeToRun+' '+jobkey+' >> ./'+str(key)+'_'+str(i)+'.runlog 2>&1')

            # All job files, sub and sh files are ready
            # Now SHOOT !
            if args.send :
                condorJobCommandList = ['condor_submit',subkey]
                st = os.stat(shkey)
                os.chmod(shkey, st.st_mode | stat.S_IEXEC)
                process = Popen(condorJobCommandList, stdout=PIPE, stderr=PIPE)



    # signal samples
    logging.info('Start making job cards for Signal samples ===>')
    for key, val in signalSamplesDict.items():
        logging.info(' Sample : {}'.format(key))
        filePathList = val.get('filedirs')
        xsec         = val.get('xsec')
        evtWtSum     = val.get('genEvtWtSum')
        filesPerJob        = int(val.get('filesPerJob'))
        files = []
        for item in filePathList:
            logging.info('\t {}'.format(item))
            files += [os.path.join(item,rfile) for rfile in os.listdir(item) if '.root' in rfile]
        jobFile = str(key)+'.job'
        with open(os.path.join(jobDir,jobFile), 'w') as jfile:
            jfile.write('START\n'+'era '+str(era)+'\n'+'dataType mc#signal\n')
            for item in commonInfoList:
                jfile.write(item+'\n') 
            jfile.write('############ MVA Info ###############'+'\n')
            for item in mvaInfoList:
                jfile.write(item+'\n')
            jfile.write('########### xsec,lumi,hist ###########\n')
            jfile.write('lumiWtList xsec='+str(xsec)+' intLumi='+str(lumi)+' nevents=100000'+'\n')
            jfile.write('histFile '+histDir+'/'+str(key)+'_hist.root'+'\n')
            jfile.write('logFile '+histDir+'/'+str(key)+'_dump.log'+'\n')
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
        if not os.path.isdir(conDir):
            os.mkdir(conDir)
        tmplsubFile = os.path.join(conDir,str(key)+'.sub.tmpl')
        tmplshFile  = os.path.join(conDir,str(key)+'.sh.tmpl')
        shutil.copy(os.path.join('CondorTemplateScripts','SAMPLE.sub.tmpl'), tmplsubFile)
        shutil.copy(os.path.join('CondorTemplateScripts','SAMPLE.sh.tmpl'), tmplshFile)
        inputFile = str(key)+'_infiles.list'
        with open(os.path.join(conDir, inputFile), 'w') as infile:
            for file in files:
                infile.write(file+'\n')
        infile.close()
        if not os.path.isdir(os.path.join(conDir,'log')):
            os.mkdir(os.path.join(conDir,'log'))
        if not os.path.isdir(os.path.join(conDir,'output')):
            os.mkdir(os.path.join(conDir,'output'))
        if not os.path.isdir(os.path.join(conDir,'error')):
            os.mkdir(os.path.join(conDir,'error'))
        if not os.path.isdir(os.path.join(conDir,'runlogs')):
            os.mkdir(os.path.join(conDir,'runlogs'))
        infileListPerJob = [files[i:i+filesPerJob] for i in range(0, len(files), filesPerJob)]
        logging.info('\t nFiles : {} || nJobs : {}'.format(len(files),len(infileListPerJob)))
        for i, filesList in enumerate(infileListPerJob):
            jobkey = os.path.join(conDir,str(key)+'_'+str(i)+'.job')
            subkey = os.path.join(conDir,str(key)+'_'+str(i)+'.sub')
            shkey  = os.path.join(conDir,str(key)+'_'+str(i)+'.sh')
            with open(jobkey, 'w') as tmpl:
                tmpl.write('START\n'+'era '+str(era)+'\n'+'dataType mc#signal\n')
                for item in commonInfoList:
                    tmpl.write(item+'\n') 
                tmpl.write('############ MVA Info ###############'+'\n')
                for item in mvaInfoList:
                    tmpl.write(item+'\n')
                tmpl.write('########### xsec,lumi,hist ###########\n')
                tmpl.write('lumiWtList xsec='+str(xsec)+' intLumi='+str(lumi)+' nevents=100000'+'\n')
                tmpl.write('histFile '+histDir+'/'+str(key)+'_'+str(i)+'_hist.root'+'\n')
                tmpl.write('logFile '+histDir+'/'+str(key)+'_'+str(i)+'_dump.log'+'\n')
                tmpl.write('############ Cut lists ###############'+'\n')
                for item in cutLists:
                    tmpl.write(item+'\n')
                tmpl.write('############ Input Files ##############'+'\n')
                for item in filesList:
                    tmpl.write('inputFile '+item+'\n')
                tmpl.write('########################################\n')
                for item in endInfoList:
                    tmpl.write(item+'\n')
                tmpl.write('END')
            tmpl.close()
        
            shutil.copy(tmplsubFile, subkey)
            shutil.copy(tmplshFile, shkey)
            # edit the sub.tmpl file
            replaceAll(subkey, 'executable   = sample_index.sh', 'executable   = '+shkey.split('/')[-1])
            replaceAll(subkey, 'output       = output/sample_INDEX.$(ClusterId).$(ProcId).out', 'output       = output/'+str(key)+'_'+str(i)+'.$(ClusterId).$(ProcId).out')
            replaceAll(subkey, 'error        = error/sample_INDEX.$(ClusterId).$(ProcId).err', 'error        = error/'+str(key)+'_'+str(i)+'.$(ClusterId).$(ProcId).err')
            replaceAll(subkey, 'log          = log/sample_INDEX.$(ClusterId).log', 'log          = log/'+str(key)+'_'+str(i)+'.$(ClusterId).log')
            # edit the sh.tmpl file
            replaceAll(shkey, 'JOBDIR=NameOfJobDirGivenInYaml', 'JOBDIR='+jobdir)
            replaceAll(shkey, 'APPDIR=NameOfAppDirGivenInYaml', 'APPDIR='+appdir)
            #replaceAll(shkey, 'cd $JOBDIR/condor_runlog_dir', 'cd $JOBDIR/'+pwd.split('/')[-1]+'/'+conDir+'/'+'runlogs')
            replaceAll(shkey, 'cd $JOBDIR/condor_runlog_dir', 'cd '+os.path.join(conDir,'runlogs'))
            replaceAll(shkey, 'uname -a > ./sample_INDEX.runlog 2>&1', 'uname -a > ./'+str(key)+'_'+str(i)+'.runlog 2>&1')
            #replaceAll(shkey, '$JOBDIR/EXE $JOBDIR/PathToJobFile/sample_index.job >> ./sample_index.runlog 2>&1','$JOBDIR/'+exeToRun+' $JOBDIR/'+jobkey+' >> ./'+str(key)+'_'+str(i)+'.runlog 2>&1')
            replaceAll(shkey, '$JOBDIR/EXE $JOBDIR/PathToJobFile/sample_index.job >> ./sample_index.runlog 2>&1', exeToRun+' '+jobkey+' >> ./'+str(key)+'_'+str(i)+'.runlog 2>&1')

            # All job files, sub and sh files are ready
            # Now SHOOT !
            if args.send :
                condorJobCommandList = ['condor_submit',subkey]
                st = os.stat(shkey)
                os.chmod(shkey, st.st_mode | stat.S_IEXEC)
                process = Popen(condorJobCommandList, stdout=PIPE, stderr=PIPE)


if __name__ == "__main__":
    main()
