# system
import os, sys, glob, re, itertools, collections, requests
import multiprocessing
from joblib import Parallel, delayed
from pathlib import Path

import bikram
import datetime as dt
from indictrans import Transliterator

# pyscience imports
import numpy as np
import pandas as pd

# show all output
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = 'all'

# %%
root = Path('/home/alal/tmp/_scrapers/insec_nepal/')
df = pd.read_csv(root/'data/raw.csv').infer_objects()
df.drop(['Unnamed: 33'], axis = 1, inplace = True)

[(x, df[x].nunique()) for x in df.columns]
df.info()
# %%
######## ########     ###    ##    ##  ######  ##       #### ########
   ##    ##     ##   ## ##   ###   ## ##    ## ##        ##     ##
   ##    ##     ##  ##   ##  ####  ## ##       ##        ##     ##
   ##    ########  ##     ## ## ## ##  ######  ##        ##     ##
   ##    ##   ##   ######### ##  ####       ## ##        ##     ##
   ##    ##    ##  ##     ## ##   ### ##    ## ##        ##     ##
   ##    ##     ## ##     ## ##    ##  ######  ######## ####    ##


trn = Transliterator(source='nep', target='eng')
# %%
def translate(x): # pool needs picklable functions
    return(trn.transform(x))
# prototype lambda
remapper = lambda x: {x: trn.transform(x) for x in df[x].unique() if str(x) != "nan"}
# full parallelised fn
def remapper_par(x, n = 6):
    vals = [str(x) for x in df[x].unique()]
    pool = multiprocessing.Pool(processes=n)
    mapping = pool.map(translate, vals)
    return dict(zip(vals, mapping))
# %% runtime approx 2 mins locally
%%time
dist_mappings = remapper_par('event_district')
vdc_mappings  = remapper_par('event_vdc')
tole_mappings = remapper_par('event_tole')
name_mappings = remapper_par('name')
jat_mappings  = remapper_par('jat')
cat_mappings  = remapper_par('cat')
fname_mappings= remapper_par('fname')
mname_mappings= remapper_par('mname')

# %% manual mappings
##     ##    ###    ##    ## ##     ##    ###    ##
###   ###   ## ##   ###   ## ##     ##   ## ##   ##
#### ####  ##   ##  ####  ## ##     ##  ##   ##  ##
## ### ## ##     ## ## ## ## ##     ## ##     ## ##
##     ## ######### ##  #### ##     ## ######### ##
##     ## ##     ## ##   ### ##     ## ##     ## ##
##     ## ##     ## ##    ##  #######  ##     ## ########

translated_df = df.copy()
# event_type, sex, lang, educ, econ, marit, comp_amt
type_mappings = {
    'अपाङ्गता' : 'disabled',
    'बेपत्ता'    : 'disappeared',
    'हत्या'     : 'killed',
    'हत्या(अन्य)' : 'killed',
    'हत्या(आफ्नै बम बिश्फोटबाट मृत्\u200dयु)' : 'killed - suicide bomb',
    'हत्या(गम्भिर प्रकृति)' : 'killed by maoists',
    'हत्या(गैरन्यायीक)' : 'killed by state (extrajudicial)',
    'हत्या(दोहोरो भिडन्तमा मृत्\u200dयु)' : 'killed in combat',
    'हत्या(बेवारिसे बम बिश्फोटबाट मृत्\u200dयु)' : 'killed by stray explosive',
    'हत्या(विद्युतीय धरापमा  मृत्यु)' : 'killed by electrocution',
}
translated_df.event_type = translated_df.event_type.map(type_mappings)

# %%
# fix edge cases
df.loc[df.sex == "बुलिङ्गटार", 'sex'] = 'पुरुष'
sex_mappings = {'पुरुष' : 'M', 'महिला': 'F', 'लिङ्गसिङ्ग': 'NA'}
translated_df.sex = translated_df.sex.map(sex_mappings)

# %%
educ_mappings = {
    'नखुलेको': "unknown",
    'साक्षर': "literate",
    'प्रावि': "primary",
    'निमावि': "lower-secondary",
    'मावि': "secondary",
    'प्रमाणपत्र': "certificate / SLC",
    'स्नातक': "bachelors",
    'निरक्षर': "illiterate",
    'स्नातकोत्तर': "postgrad"
}
translated_df.educ = translated_df.educ.map(educ_mappings)
# %%
lang_mappings = {
    'नेपाली': "nepali",
    'राइ': "rai",
    'तमाङ': "tamang",
    'अन्य': "other",
    'नेवारी': "newari",
    'गुरुङ': "gurung",
    'थारु': "tharu",
    'हिन्दी': "hindi",
    'मैथिली': "maithili",
    'मगर': "magar",
    'भोजपुरी': "bhojpuri",
    'अवधी': "awadhi",
    'नखुलेको': "NA",
    'चेपाङ्': "chepang",
    'जिरेल': "jirel",
    'मुस्लिम': "muslim",
    'थामी': "thami",
    'धिमाल': "dhimal",
    'थकाली' : 'thakali'
}
translated_df.lang = translated_df.lang.map(lang_mappings)

# %%
econ_mappings = {
    'निम्नमध्यम': 'lower-middle', 'निम्न': 'lower', 'नखुलेको': 'unknown',
    'मध्यम': 'middle', 'उच्च': 'upper'
}
translated_df.econ = translated_df.econ.map(econ_mappings)

# %%
marit_mappings = {
    'विवाहित': 'married', 'अविवाहित': 'single', 'नखुलेको': 'unknown',
    'एकल': 'single', 'विदुर': 'widower', 'विधवा': 'widow'
}

translated_df.marit = translated_df.marit.map(marit_mappings)

# %%
occ_mappings = {'जागीर': 'office', 'कृषि': 'agriculture', 'प्रहरी': 'police',
                'राजनीति': 'politics', 'शिक्षक': 'teaching', 'सेना': 'army',
                'नखुलेको': 'unknown', 'विद्यार्थी': 'student', 'ज्यालादारी': 'wage labour',
                'अन्य': 'other', 'ड्राइभर': 'driver', 'व्यापारी': 'businessman',
                'अधिकारकर्मी': 'hr activist', 'सामाजिक कार्यकर्ता': 'soc activist',
                'भूपू सुरक्षाकर्मी': 'ex-security', 'वकिल': 'lawyer',
                'डाक्टर': 'doctor', 'खेलाडी': 'athlete', 'पत्रकार': 'press'}

translated_df.occ = translated_df.occ.map(occ_mappings)

# %%
perpe_mappings = {
    'राज्य': 'state',
    'गैर राज्य--एमाओवादी': 'nonstate - maoists',
    'गैर राज्य--अज्ञात': 'nonstate - unknown',
    'गैर राज्य--स्थानीय बासिन्दा': 'nonstate - locals',
    'गैर राज्य--प्रतिकार समूह': 'nonstate - retaliators',
   'गैर राज्य--': 'nonstate'
}
translated_df.perpe = translated_df.perpe.map(perpe_mappings)

# %% computer translations
translated_df.event_district  = translated_df.event_district.map(dist_mappings)
translated_df.event_vdc       = translated_df.event_vdc.map(vdc_mappings)
translated_df.event_tole      = translated_df.event_tole.map(tole_mappings)
translated_df.name            = translated_df.name.map(name_mappings)
translated_df.jat             = translated_df.jat.map(jat_mappings)
translated_df.cat             = translated_df.cat.map(cat_mappings)
translated_df.fname           = translated_df.fname.map(fname_mappings)
translated_df.mname           = translated_df.mname.map(mname_mappings)
# recycle location translations
translated_df.adr_district  = translated_df.adr_district.map(dist_mappings)
translated_df.adr_vdc       = translated_df.adr_vdc.map(vdc_mappings)
translated_df.adr_tole      = translated_df.adr_tole.map(tole_mappings)

# %%
translated_df['maoist'] = np.where(translated_df.party.str.contains("माओवादी") & translated_df.party.notnull(), 1, 0)
translated_df['cong']   = np.where(translated_df.party.str.contains("काँग्रेस") & translated_df.party.notnull(), 1, 0)

translated_df.maoist.value_counts()
translated_df.cong.value_counts()

# %% convert dates
def bs_ad(x):
    d = x.split('-')
    try:
        ce_dt = bikram.samwat(int(d[0]), int(d[1]), int(d[2])).ad
        return ce_dt
    except: # breaks for bad dates
        return None

translated_df['ce_date'] = pd.to_datetime(translated_df.event_date.apply(bs_ad))

translated_df['year']  = translated_df['ce_date'].dt.year
translated_df['month'] = translated_df['ce_date'].dt.month
translated_df['day']   = translated_df['ce_date'].dt.day

translated_df.to_csv(root/'data/translated_df.csv', index = False)
# %%
