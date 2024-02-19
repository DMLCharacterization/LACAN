#!/usr/bin/env python
# -*- coding: utf-8 -*-
##################################################
## Listing all values considered for all configuration parameters.
##################################################
## LACAN is licensed under the LGPL license
##################################################
## Author: Sara Bouchenak (Sara.Bouchenak@insa-lyon.fr)
## Copyright: Copyright 2019, LACAN
## License: LGPL
## Version: 1.0
## Date: May 29, 2019
## Email: Sara.Bouchenak@insa-lyon.fr
##################################################

##################################################
## Usage:
## python configofeachparam.py ../../../../../results/2019-05-26T11:06:34.518724 
##################################################

import sys
import os

os.system('python configofoneparam.py ' + sys.argv[1] + " spark.executor.cores")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.serializer")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.io.compression.codec")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.reducer.maxSizeInFlight")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.shuffle.io.preferDirectBufs")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.shuffle.spill.compress")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.shuffle.compress")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.locality.wait")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.rdd.compress")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.shuffle.file.buffer")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.executor.memory")
os.system('python configofoneparam.py ' + sys.argv[1] + " spark.storage.memoryFraction")

