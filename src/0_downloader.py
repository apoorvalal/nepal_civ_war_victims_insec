import os, sys, requests, re
from pathlib import Path
from requests_html import HTMLSession
import time

# %%
def download(id, dir, victim_root):
    try:
        fn = f"{dir}/id_{id}.html"
        r = requests.get(victim_root,
                params = {"MFID": id}
        )
        with open(fn,'wb') as f:
            f.write(r.content)
        print(f'{id} download successful.')
    except:
        print(f'{id} failed to download.')
        pass

# %%
maxid = 17509
insec_root = "http://www.insec.org.np/victim/candidate_details_user.php"
for i in range(1, maxid + 1):
    download(i, "/home/alal/tmp/_scrapers/insec_nepal/raw_pages", insec_root)
    time.sleep(0.1)
# %%
