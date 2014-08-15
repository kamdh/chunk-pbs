#!/usr/bin/env python
# -*- coding: utf-8 -*-

# submitChunks.py
# Kameron Decker Harris
#
# usage: submitChunks.py cfg.ini
#  Submits the commands in a given file (line-separated) to a number of PBS 
#  jobs, breaking them into chunks as appropriate. Options are set in cfg.ini

import sys
import os
import time
import subprocess
import numpy as np
import ConfigParser

qsub_command_fn = "qsubber.sh"

## begin helper functions
def parseargs(argv):
    cfg = ConfigParser.ConfigParser()
    cfg.read(argv[1])
    cmdFn = os.path.expandvars(cfg.get("chunks", "commandfile"))
    baseDir = os.path.expandvars(cfg.get("chunks", "qsubdir"))
    runDir = os.path.expandvars(cfg.get("chunks", "rundir"))
    pbsTag = cfg.get("chunks", "jobname")
    qsubFn = os.path.join(baseDir, qsub_command_fn)
    ppn = cfg.getint("chunks", "ppn")
    mem = cfg.getfloat("chunks", "mem")
    cmdspernode = cfg.getint("chunks", "useprocs")
    queue = cfg.get("chunks", "queue")
    ## options to pass to parallel
    ## only run cmdspernode at once
    parallelOpts = "-j" + str(cmdspernode)
    chunksize = cfg.getint("chunks", "chunksize")
    cmdruntime = cfg.getfloat("chunks", "cmdruntime")
    extratime = cfg.getfloat("chunks", "extratime")
    headerExtras = cfg.get("chunks", "headerextras")
    return cmdFn, baseDir, runDir, pbsTag, qsubFn, ppn, mem, cmdspernode, \
        chunksize, cmdruntime, extratime, headerExtras, parallelOpts, queue

def linecount(fn):
    if os.path.isfile(fn):
        # fork a wc process to count the number of commands in fn
        p = subprocess.Popen(['wc', '-l', fn], stdout=subprocess.PIPE, 
                             stderr=subprocess.PIPE)
        out,err = p.communicate()
        numlines = int(out.split()[0])
    else:
        raise Exception(fn + ' does not exist')
    return numlines

def qsubheader(pbsnumber, walltime, pbsTag, ppn, mem, baseDir, \
               runDir, headerExtras, parallelOpts):
    ## setup our header
    S = "#!/bin/bash\n"
    ## job name
    S += "#PBS -N " + pbsTag + "%d\n" % pbsnumber
    ## resources
    S += "#PBS -l nodes=1:ppn=%d,mem=%dgb,feature=%dcore\n" % (ppn,mem,ppn)
    S += "#PBS -l walltime=00:%d:00\n" % walltime
    ## output log
    S += "#PBS -o " + \
         os.path.abspath(os.path.join(baseDir, "log_chunk%d" % pbsnumber)) + "\n"
    S += "#PBS -j oe\n"
    S += "#PBS -d " + runDir + "\n" # set PBS_O_INITDIR
    S += "cd $PBS_O_INITDIR\n" # change to running directory
    S += headerExtras + "\n"
    ## pipe all commands to parallel using a heredoc
    S += "cat << CHUNK_EOF | parallel " + parallelOpts + "\n"
    return S

def qsubcloser():
    S = "CHUNK_EOF\n"
    return S
## end helper functions

########################################
def main(argv=None):
    if argv is None:
        argv = sys.argv
    cmdFn, baseDir, runDir, pbsTag, qsubFn, ppn, mem, cmdspernode, \
        chunksize, cmdruntime, extratime, headerExtras, parallelOpts, queue\
        = parseargs(argv)
    print "Setting up for file " + cmdFn + " in directory " + baseDir
    ## cleanup from last  
    try:
        os.makedirs(baseDir)
    except OSError:
        pass
    try:
        os.remove(qsubFn)
    except OSError:
        pass
    try:
        [ os.remove(os.path.join(baseDir,f)) \
          for f in os.listdir(baseDir) \
          if f.endswith(".pbs") ]
    except OSError:
        pass
    ## setup a few variables
    ## est. computing time per simulation, minutes
    simruntime = np.ceil(float(cmdruntime) * chunksize / cmdspernode)
    walltime = simruntime + extratime
    totalcmds = linecount(cmdFn)
    totalchunks = np.ceil(float(totalcmds) / chunksize)
    print "Breaking %d commands into %d chunks" % (totalcmds, totalchunks)
    qsub = open(qsubFn, 'w') # this file is a script to submit the job chunks
    qsub.write("#!/bin/bash\n")
    cmds = open(cmdFn, 'r') # this is the file with our commands to be chunked
    ## initialize loop
    chunk = 1
    cmdCounter = 1
    pbsFn = pbsTag + "_chunk%d.pbs" % chunk
    pbs = open(os.path.join(baseDir, pbsFn), 'w') # pbs is the current chunk job
    pbs.write(qsubheader(chunk, walltime, pbsTag, ppn, mem, baseDir, \
                          runDir, headerExtras, parallelOpts))
    try:
        for line in cmds: # loop through all the commands
            pbs.write(line)
            if cmdCounter % chunksize == 0:
                ## we've filled the chunk
                pbs.write(qsubcloser())
                pbs.close()
                qsub.write("qsub -q " + queue + " " + pbsFn + "\n")
                if chunk != totalchunks:
                    ## then we didn't finish and can open the next
                    chunk += 1
                    pbsFn = pbsTag + "_chunk%d.pbs" % chunk
                    pbs = open(os.path.join(baseDir, pbsFn), 'w')
                    pbs.write(qsubheader(chunk, walltime, pbsTag, ppn, mem, 
                                          baseDir,runDir, headerExtras, 
                                          parallelOpts))
                    cmdCounter = 0 # incremented outside block
            cmdCounter += 1
    except ValueError as e:
        print "Warning, caught ValueError: " + str(e)
    ## cleanup
    if ((cmdCounter-1) % chunksize) != 0:
        ## close pbs if our commands didn't fill the last chunk
        pbs.write(qsubcloser())
        pbs.close()
        qsub.write("qsub -q "+ queue + " " + pbsFn + "\n")
    qsub.close()
    os.system("chmod +x \"" + qsubFn + "\"")
    print "To run your jobs:\n\t$ cd " + baseDir + '\n\t$ ./' + \
        qsub_command_fn

# run the main stuff
if __name__ == '__main__':
    status = main()
    sys.exit(status)
