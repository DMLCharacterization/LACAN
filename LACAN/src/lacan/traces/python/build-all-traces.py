#!/usr/bin/env python
# -*- coding: utf-8 -*-
##################################################
## Build all traces.
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
## python build-all-traces.py ../../../../../results/2019-05-26T11:06:34.518724 
##################################################

import sys
import os

k = str(4)
correl_threshold = str(0.75)
coeff_var_threshold = str(0.10)
overall_or_tasks = "overall"

os.system('python allconfigurations.py ' + sys.argv[1])
os.system('python configofeachparam.py ' + sys.argv[1])
os.system('python config2outputs4allworkloads.py ' + sys.argv[1] + " " + overall_or_tasks)
os.system('python mergecorrel.py ' + sys.argv[1] + " train overall " + k + " " + correl_threshold + " " + coeff_var_threshold)
os.system('python mergecorrel.py ' + sys.argv[1] + " test overall " + k + " " + correl_threshold + " " + coeff_var_threshold)
os.system('python mergecorrel.py ' + sys.argv[1] + " train tasks " + k + " " + correl_threshold + " " + coeff_var_threshold)
os.system('python mergecorrel.py ' + sys.argv[1] + " test tasks " + k + " " + correl_threshold + " " + coeff_var_threshold)



