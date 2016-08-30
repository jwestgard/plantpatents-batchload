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

def check_repo_connection(endpoint, auth):
    print('Ready to load to endpoint => {0}'.format(endpoint))
    print('Testing connection with provided credentials => ', end='')
    response = requests.get(endpoint, auth=auth)
    if response == "200":
        return True
    else:
        return False


#============================================================================
# RESOURCE CLASS
#============================================================================

class Resource():

    '''object representing the parts of a single item resource'''

    def __init__(self, metadata):
        self.__dict__ = metadata
        self.filename = os.path.basename(self.image_url)
        self.filepath = os.path.abspath("./" + self.filename)

    def build_query(self):
        pass

    def deposit(self):
        data = BytesIO(self.turtle())
        response = requests.put(self.uri, 
                                data=data,
                                auth=(FEDORA_USER, FEDORA_PASSWORD)
                                )
        return response


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
        r = Resource(row)
        print("\n  {0}. {1}: {2}".format(n+1, r.title, r.filename))
        print("      Filepath: {0}".format(r.filepath))


if __name__ == "__main__":
    main()
