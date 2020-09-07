# system
import os, sys, glob, re, itertools, collections, requests
import multiprocessing # parallelise list comprehensions
from pathlib import Path
# filler to import personal library
# sys.path.append('/home/alal/Desktop/code/py_libraries/')


# pyscience imports
import numpy as np
import pandas as pd
import janitor
import pandas_flavor as pf

import statsmodels.api as sm
import statsmodels.formula.api as smf

# viz
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
from plotnine import *
sns.set(style="ticks", context="talk")
font = {'family' : 'IBM Plex Sans',
               'weight' : 'normal',
               'size'   : 10}
plt.rc('font', **font)
plt.rcParams['figure.figsize'] = (10, 10)
matplotlib.style.use(['seaborn-talk', 'seaborn-ticks', 'seaborn-whitegrid'])
%matplotlib inline
%config InlineBackend.figure_format = 'retina'

# show all output
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = 'all'
# %%
