'''
Code purpose: To query the ZTF database for new objects and compare them to the previous day's classifications. Appending any changes to a csv file.
Author name: Laura C. Cotter
Last major update: 2023-02-12  
'''
#%%
import numpy as np 
import pandas as pd 
from alerce.core import Alerce; client = Alerce()
from time import time 
from astropy.time import Time
from datetime import datetime, timedelta 

start = time()
print('Starting search...')

# --- Date Manipulation ---
date = datetime.today().strftime("%d_%m_%Y")
yesterday = (datetime.today() - timedelta(days=1)).strftime("%d_%m_%Y")

# --- Read in Data ---
yday_df = pd.read_csv('search_results/{}.csv'.format(yesterday))
names = np.array(yday_df['Name'])

# --- Alerce Search ---
probabilities = []; classes = []
for object in names:
    alerce_df = client.query_objects(oid = object, classifier = "lc_classifier_transient", format = "pandas")
    probabilities.append(alerce_df['probability'][0])
    classes.append(alerce_df['class'][0])
print('Object query complete.')

# --- Querying detection points in found Targets in g and r bands --- 
n_g = []; n_r = [] # - Number of detection points in g and r bands
for object in names:
    detections = client.query_detections(object, format = "pandas")
    filter_n = detections['fid'].value_counts()
    if 1 in detections['fid'].values:
        n_g.append(filter_n[1])
    if 2 in detections['fid'].values:
        n_r.append(filter_n[2])
    else:
        n_g.append(0)
        n_r.append(0)
print('Detection query complete.')

# --- Creating DataFrame ---
data = {'Name': names, 'Probability': probabilities, 'Class': classes, 'n_g': n_g, 'n_r': n_r}
today_df = pd.DataFrame(data)

# --- Comparing with yesterday's classifications ---
idxs = []
for i in range(0, len(yday_df)):
    if yday_df['Class'][i] == today_df['Class'][i]:
        continue
    if yday_df['Class'][i] != today_df['Class'][i]:
        print('Classifications have changed for {}!'.format(yday_df['Name'][i]))
        print('Previous classification: {}'.format(yday_df['Class'][i]))
        print('New classification: {}'.format(today_df['Class'][i]))
        idxs.append(i)
        
# --- Concatenating DataFrames and given yesterday new headers ---
if len(idxs) > 0:
    output = pd.concat([yday_df[idxs], today_df[idxs]], ignore_index = True)
    print(output.head())
    output.to_csv('search_results/{}_multi.csv'.format(date), index = False)
    
# --- Querying SNIbc ---
today_MJD = np.around(Time(datetime.today()).mjd, 0)
SNIbc_df = client.query_objects(classifier="lc_classifier_transient", class_name = 'SNIbc', format="pandas",page_size = 2000,firstmjd = [59897,today_MJD])
print('Number of SNIbc objects: {}'.format(len(SNIbc_df)))

# --- New SNIbc ---
nobj_n = 0 # - Number of new SNIbc objects
for name in SNIbc_df['oid']:
    if name in names:
        continue
    if name not in names:
        nobj_n += 1 
        print('New SNIbc object since yesterday: {}'.format(name))
print('Number of new SNIbc objects: {}'.format(nobj_n))

# - Dropping non new SNIbc objects -
new_SNIbc_df = today_df[today_df.Name.isin(SNIbc_df['oid'])]
if len(new_SNIbc_df) > 0:
    print('Number of new SNIbc objects: {}'.format(len(new_SNIbc_df)))
    new_SNIbc_df = new_SNIbc_df.drop_duplicates(subset = 'Name', keep = 'first')  # - check for duplicates -
    new_SNIbc_df.to_csv('search_results/{}.csv'.format(date), mode = 'a', header = False, index = False)
else: 
    print('No new SNIbc classifications. :(')
    
end = time()
print('Search complete. Time elapsed: {:.2f} mins'.format((end-start)/60))
# %%
