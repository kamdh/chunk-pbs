chunk-pbs
=========

Breaks up a file containing a list of commands into chunks of configurable 
size. These are then run submitted as separate jobs to the PBS scheduler.

Contains:
* chunks.py - generate the chunks which are then submitted manually
* testconfig.ini - example configuration file
* sweepfile - example command file
* missing.py - find missing output
