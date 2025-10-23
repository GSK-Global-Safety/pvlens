'''
Created on Oct 22, 2024

@author: Jeffery Painter
'''

#
# /*
# * This file is part of PVLens.
# *
# * Copyright (C) 2025 GlaxoSmithKline
# *
# * PVLens is free software: you can redistribute it and/or modify
# * it under the terms of the GNU General Public License as published by
# * the Free Software Foundation, either version 3 of the License, or
# * (at your option) any later version.
#  *
#  * PVLens is distributed in the hope that it will be useful,
#  * but WITHOUT ANY WARRANTY; without even the implied warranty of
#  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  * GNU General Public License for more details.
#  *
#  * You should have received a copy of the GNU General Public License
#  * along with PVLens.  If not, see <https://www.gnu.org/licenses/>.
#  */
#
#
#
# Download SRLC data
#
# The following notebook will download the product safety labeling changes published by the FDA
# https://www.accessdata.fda.gov/scripts/cder/safetylabelingchanges/index.cfm
#
#

# Imports
import pandas as pd
import os
import requests
import time

#####################################################################
# All punctuation
#####################################################################
punc = '!"#$%\'()*&+,-:;<=>?@[\\]^`{|}~'

#####################################################################
# Set a user agent used to get requests
#####################################################################
userAgent = { 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36' }

#####################################################################
# Global variables
#####################################################################
FDA_SRLC_PATH = "../../src/main/resources/fda_srlc/"
FDA_SRLC_HTML_SRC = FDA_SRLC_PATH + "html_download/"

# This file was created by going to the FDA site and searching
# for all labels from 1/1/2016 to current, then export as CSV
# Update whenever you re-run the database pipeline
LABEL_CHANGES = FDA_SRLC_PATH + "label_changes_20251022.csv"

#####################################################################
def download_file(url, local_filename):
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return local_filename

#####################################################################
# Main method
#####################################################################
if __name__ == '__main__':
    
    # Load the links to download
    labels = pd.read_csv(LABEL_CHANGES)
    
    drug_links = {}
    drug_appid_links = {}
    counter = 0
    for index, row in labels.iterrows():
        app_id    = row['Application Number']

        #  Drug ID stored in URL
        prd_url   = row['Link']
        drug_id   = -1
        drug_id   = int(prd_url.split("&")[1].split("=")[1])

        # Link drug id to app id
        drug_links[drug_id] = prd_url
        drug_appid_links[drug_id] = app_id
        counter += 1
    
    print("Total links: " + str(counter))
    print("Unique entries: " + str(len(drug_links.keys())))
    print("Unique entries: " + str(len(drug_appid_links.keys())))
    
    # Download and save the HTML pages locally for processing later
    # This should take a little less than 5 minutes to download ~2900 HTML pages
    
    # If the data has already been downloaded, change this to false
    DOWNLOAD_DATA = True
    DATA_PATH = FDA_SRLC_HTML_SRC
    WAIT_TIME = 0.2 # Wait time in seconds to throttle download time
    
    if DOWNLOAD_DATA == True:
        all_files = os.listdir(DATA_PATH)
        counter = 0
        for drug_id in drug_links.keys():
            
            prd_url   = drug_links[drug_id]
    
            # Create an output file to save the contents
            out_file = str(drug_id) + ".html"
            download_file(prd_url, DATA_PATH + out_file)
            print("Saving Product Label Change: " + str(drug_id) + " URL: " + prd_url)
            # Throttle calls to the website
            time.sleep(WAIT_TIME)
            counter += 1
            if counter % 100 == 0:
                print(">> " + str(counter))
                
    else:
        # Test that the data files exist
        all_files = os.listdir(DATA_PATH)
        expected = len(drug_links.keys())
        found = len(all_files)
        if expected == found:
            print("Total files: " + str(len(all_files)))
            print("Skippping, data already downloaded!")
        else:
            print("There are some files missing, expected: " + str(expected) + " Found: " + str(found))
    
    print("SRLC downloads complete!")    
