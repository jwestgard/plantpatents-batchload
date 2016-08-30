#!/usr/bin/env python

from __future__ import print_function

import argparse
import csv
import logging
import os
import requests
from io import BytesIO
import sys
import yaml


#============================================================================
# HELPER FUNCTIONS
#============================================================================

def check_repo_connection():
    print('Ready to load to endpoint => {0}'.format(REST_ENDPOINT))
    print('Testing connection with provided credentials => ', end='')
    response = requests.get(REST_ENDPOINT, 
                            auth=(FEDORA_USER, FEDORA_PASSWORD)
                            )
    if response == "200":
        return True
    else:
        return False 

   
def create_rdfsource():
    print("Creating resource... ", end='')
    response = requests.post(REST_ENDPOINT, 
                             auth=(FEDORA_USER, FEDORA_PASSWORD)
                             )
    if response.status_code == 201:
        print('Success!')
        return response.text
    else:
        print('Failed!')
        return False
        

def sparql_update(uri, payload):
    print("Updating resource... ", end='')
    response = requests.patch(uri, 
                              auth=(FEDORA_USER, FEDORA_PASSWORD),
                              data=payload
                              )
    if response.status_code == 204:
        print('Success!')
        return True
    else:
        print('Failed!')
        return False


#============================================================================
# RESOURCE CLASS
#============================================================================

class Resource():

    '''object representing the parts of a single item resource'''

    def __init__(self, metadata, asset_path):
        self.__dict__ = metadata
        self.filename = os.path.basename(self.image_url)
        self.filepath = os.path.join(asset_path + self.filename)
        self.triples = [ ("dc:identifier", self.patent_number),
                         ("dc:date", self.date), 
                         ("dc:year", self.year), 
                         ("dc:title", self.title),
                         ("exterms:category", self.large_category), 
                         ("dc:creator", self.inventor), 
                         ("exterms:inventorCity", self.city), 
                         ("exterms:inventorState", self.state), 
                         ("exterms:inventorCountry", self.country), 
                         ("exterms:uspcNumber", self.uspc), 
                         ("exterms:imageUrl", self.image_url),
                         ("exterms:sourceUrl", self.patent_url),
                         ("exterms:applicationNumber", self.application_number)
                         ]
                         
#        self.file_triples + [ ( , ),
#                            ]
        
    
    # confirm accessibility of an associated binary    
    def file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False


    # update item in fcrepo
    def sparql_payload(self):
        query = []
        for ns,uri in NAMESPACE_BINDINGS.items():
            query.append("PREFIX {0}: <{1}>".format(ns,uri))
        query.append("INSERT DATA {")
        for p,o in self.triples:
            if o is not "":
                query.append('<> {0} "{1}" .'.format(p, o))
        query.append("}")
        return "\n".join(query)


    # update file in fcrepo        
    def create_file_object(self, patent_uri):
        self.file_triples = [("exterms:scandate", self.scan_date),
                             ("exterms:filename", self.filename)
                             ("dc:extent", self.pages),
                             ("pcdm:memberOf", patent_uri)
                             ]
        

#============================================================================
# MAIN LOOP
#============================================================================

def main():
    
    '''Parse args, loop over repository and restore.'''
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Restore serialized Fedora repository.')
    
    parser.add_argument('-c', '--config', required=True,
                        help='relative or absolute path to the YAML config file'
                        )
    
    parser.add_argument('directory', 
                        help='path to metadata spreadsheet and binaries'
                        )
        
    args = parser.parse_args()
    
    # print header
    title = "| PLANT PATENTS BATCH LOADER |"
    border = "-" * len(title)
    print("\n".join(["", border, title, border]))
    
    # load and parse specified configuration settings
    with open(args.config, 'r') as configfile:
        globals().update(yaml.safe_load(configfile))
    
    # check the backup tree
    print('Scanning directory tree => {0}'.format(args.directory))
    
    # load metadata
    with open(os.path.join(args.directory, 'metadata.csv'), 'r') as metafile:
        reader = csv.DictReader(metafile)
        metadata = [row for row in reader]
        print('Found {0} rows of data.'.format(len(metadata)))
        
    # loop over files, generate containers, and POST to fcrepo
    for n, row in enumerate(metadata):
        r = Resource(row, args.directory)
        print("\n{0}. {1}: {2}".format(n+1, r.title, r.filename))
        print("Filepath: {0}".format(r.filepath))
        if r.file_exists():
            print("  => File found!")
        else:
            print("ERROR => Cannot access file {0}".format(r.filepath))
            sys.exit()

        # open transaction, create objects, update objects, commit
        #(1) start transaction
        
        #(2) create item
        patent_uri = create_rdfsource()
        
        #(3) create file
        # file_uri = create_rdfsource()
        
        #(4) attach binary
        #(5) update item
        sparql_update(patent_uri, r.sparql_payload())
        
        #(6) update file 
        #(7) commit transaction
        


if __name__ == "__main__":
    main()
