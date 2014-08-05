#!/usr/bin/env python

import os

## following file contains the commands that were run
cmdFn = os.path.join(os.environ['HOME'], "work/prebotc/src/pipeline/",
                     "random_fine_g_sweep_sweepfile")
## output filename's index in the command line (0 based)
fnIdx = 3
## this will contain the commands that should be rerun
newCmdFn = 'sweepcleanup'

try:
    os.remove(newCmdFn)
except OSError:
    pass

cmds = open(cmdFn, 'r')
newf = open(newCmdFn, 'w')
for line in cmds:
    cmdparts = line.split()
    fn = cmdparts[fnIdx]
    if not os.path.isfile(fn):
        print fn
        newf.write(line)
cmds.close()
newf.close()
