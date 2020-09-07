# system
import os, sys, glob, re, itertools, collections, requests
import multiprocessing, joblib
from pathlib import Path
from pathlib import Path

from bs4 import BeautifulSoup
from indictrans import Transliterator

# pyscience imports
import numpy as np
import pandas as pd

# show all output
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = 'all'
# %% functions
cleaner = lambda table: [x.strip() for x in table.text.split('\n') if not x.isspace() and x != ""]

def parse_record(path, noisy = False):
    # read all tables
    try:
        tables = BeautifulSoup(open(path,'r', errors = 'ignore').read()).find_all('table')
    except:
        print(f'parse error for {path}')
        return None
    # check for completeness
    if len(tables) < 21:
        if noisy: print(f"{path} is an incomplete record")
        return None
    # slice tables and clean up
    location     = cleaner(tables[4])
    bio          = cleaner(tables[5])
    consequences = cleaner(tables[7])
    metadata     = cleaner(tables[17])
    # init dict
    dat = dict()
    dat['path'] = path.split('/')[-1]
    # process location first
    dat['event_type']       = location[1]
    dat['event_district']   = [x.strip().split(':') for x in location if 'जिल्ला' in x][0][1].strip()
    dat['event_vdc']        = [x.strip().split(':') for x in location if 'गाविस' in x][0][1].strip()
    dat['event_ward']       = [x.strip().split(':') for x in location if 'वार्ड नं' in x][0][1].strip()
    dat['event_tole']       = [x.strip().split(':') for x in location if 'टोल' in x][0][1].strip()
    # metadata contains timestamp
    dat['event_date'] = metadata[3].strip()
    dat['event_time'] = metadata[4].strip()
    dat['event_note'] = metadata[-1].strip()
    # bio
    dat['name'] = [x.strip().split(':') for x in bio if 'पीडितको नाम' in x][0][1].strip()
    dat['age']  = [x.strip().split(':') for x in bio if 'उमेर' in x][0][1].strip()
    dat['sex']  = [x.strip().split(':') for x in bio if 'लिङ्ग' in x][0][1].strip()
    dat['cat']  = [x.strip().split(':') for x in bio if 'थर' in x][0][1].strip()
    dat['jat']  = [x.strip().split(':') for x in bio if 'जात' in x][0][1].strip()
    dat['lang'] = [x.strip().split(':') for x in bio if 'भाषा' in x][0][1].strip()
    dat['occ']  = [x.strip().split(':') for x in bio if 'पेशा' in x][0][1].strip()
    dat['party']= [x.strip().split(':') for x in bio if 'राजनीतिक दल' in x][0][1].strip()
    dat['educ'] = [x.strip().split(':') for x in bio if 'शैक्षिक स्तर' in x][0][1].strip()
    dat['econ'] = [x.replace("आर्थिक स्तर", "") for x in bio if 'आर्थिक स्तर' in x][0].strip()
    dat['marit']= [x.strip().split(':')  for x in bio if 'वैवाहिक स्थिति' in x][0][1].strip()
    dat['fname']= [x.replace("बाबुको नाम", "") for x in bio if 'बाबुको नाम' in x][0].strip()
    dat['mname']= [x.strip().split(':')  for x in bio if 'आमाको नाम' in x][0][1].strip()
    dat['sname']= [x.strip().split(':')  for x in bio if 'पति वा पत्नीको नाम' in x][0][1].strip()
    dat['mkids']= [x.strip().split(':')  for x in bio if 'छोराको संख्या' in x][0][1].strip()
    dat['fkids']= [x.strip().split(':')  for x in bio if 'छोरीको संख्या' in x][0][1].strip()
    # address
    dat['adr_district'] = bio[8].strip().split(':')[1].strip()
    dat['adr_vdc']      = bio[9].strip().split(':')[1].strip()
    dat['adr_ward']     = bio[10].strip().split(':')[1].strip()
    dat['adr_tole']     = bio[11].strip().split(':')[1].strip()
    # compensation
    dat['compensated']= [x.strip().split(':')  for x in bio if 'क्षतिपुर्ती प्राप्त' in x][0][1].strip()
    dat['compensated_by']= [x.strip().split(':')  for x in bio if 'प्राप्त भएको भए कोबाट' in x][0][1].strip()
    dat['comp_amt']= [x.replace("कति रकम बराबर(रु.) ", "") for x in bio if 'कति रकम बराबर(रु.)' in x][0].strip()
    # perpetrator - messy
    perptab = str(tables[15]).split('\n')
    id = [x for x, v in enumerate(perptab) if 'checked=\"checked\"' in v]
    perp = perptab[id[0]].strip()
    if 'value=\"government\"' in perp:
        dat['perpe'] = perp.split("/>")[-1]
    else:
        dat['perpe'] = perptab[id[0]+1].strip().replace("\t","-").replace("</td>","")
    return dat
# %%
root = Path('/home/alal/tmp/_scrapers/insec_nepal/')
raw = root/'raw_pages'
infiles = glob.glob(str(raw) + '/*.html' )
infiles.sort()
infiles[:5]
# %%
pool = multiprocessing.Pool(processes=6)
# %%
%%time
clean = pool.map(parse_record, infiles)
clean = [x for x in clean if x is not None]
# %%
data = pd.DataFrame(clean)
data.to_csv(root/'data/raw.csv', index = False)
# %%
