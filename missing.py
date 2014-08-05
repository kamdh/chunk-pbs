#!/usr/bin/env python

import os

cmdFn = '../sweep_random_fine_sweepfile'
newCmdFn = 'sweepcleanup'
#runDir = os.path.join(os.environ['HOME'], "prebotc/src/model") # where the jobs ran
fnIdx = 3

try:
    os.remove(newCmdFn)
except OSError:
    pass

cmds = open(cmdFn, 'r')
newf = open(newCmdFn, 'w')
for line in cmds:
    cmdparts = line.split()
    fn = cmdparts[fnIdx]
    #fn = os.path.join(runDir, cmdparts[fnIdx])
    if not os.path.isfile(fn):
        print fn
        newf.write(line)
cmds.close()
newf.close()
