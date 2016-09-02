#!/usr/bin/env python

from __future__ import print_function

import argparse
import csv
import hashlib
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

   
def create_rdfsource(uri):
    print("Creating RDF resource... ", end='')
    response = requests.post(uri, 
                             auth=(FEDORA_USER, FEDORA_PASSWORD)
                             )
    if response.status_code == 201:
        print('{0}, success.'.format(response))
        return response.text
    else:
        print('Failed!')
        return False
        
        
def upload_file(uri, localpath, checksum, patent_uri):
    print("Creating binary resource... ", end='')
    data = open(localpath, 'rb').read()
    filename = os.path.basename(localpath)
    headers = { 'Content-Type': 'application/octet-stream',
                'Digest': 'sha1={0}'.format(checksum),
                'Content-Disposition': 
                    'attachment; filename="{0}"'.format(filename)
                }
    response = requests.post( uri, 
                              auth=(FEDORA_USER, FEDORA_PASSWORD),
                              data=data,
                              headers=headers
                              )
    if response.status_code == 201:
        print('{0}, success.'.format(response))
        return response.text
    else:
        print('Failed!')
        return False


def sparql_update(uri, payload):
    print("Updating resource... ", end='')
    response = requests.patch( uri, 
                               auth=(FEDORA_USER, FEDORA_PASSWORD),
                               data=payload,
                               headers={'Content-type': 
                                    'application/sparql-update'} 
                               )
    print(response)
    if response.status_code == 204:
        print('{0}, success.'.format(response))
        return True
    else:
        print('Failed!')
        print(response.headers)
        return False


def sha1(file):
    BUF_SIZE = 65536
    sha1 = hashlib.sha1()
    with open(file, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


#============================================================================
# RESOURCE CLASS
#============================================================================

class Resource():

    '''object representing a plant patent resource and associated images'''

    def __init__(self, metadata, asset_path):
        self.__dict__ = metadata
        self.filename = os.path.basename(self.image_url)
        self.filepath = os.path.join(asset_path + self.filename)
        self.checksum = sha1(self.filepath)
        
        self.triples = [ ("dc:identifier", self.patent_number),
                         ("dc:date", self.date), 
                         ("dc:year", self.year), 
                         ("dc:title", self.title),
                         ("exterms:category", self.large_category), 
                         ("exterms:uspcNumber", self.uspc), 
                         ("exterms:sourceUrl", self.patent_url),
                         ("exterms:applicationNumber", self.application_number),
                         ("rdf:type", "pcdm:Object")
                         ]
                         
        for inventor in self.inventor.split(';'):
            self.triples.append( ("dc:creator", inventor) )
        for city in self.city.split(';'):
            self.triples.append( ("exterms:inventorCity", city) )
        for state in self.state.split(';'):
            self.triples.append( ("exterms:inventorState", state) )
        for country in self.country.split(';'):
            self.triples.append( ("exterms:inventorCountry", country) )
                         
        self.file_triples = [ ("exterms:extent", self.pages),
                              ("exterms:scanDate", self.scan_date),
                              ("exterms:fileName", self.filename),
                              ("rdf:type", "pcdm:File")
                              ]
        
    
    # confirm accessibility of an associated binary    
    def file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False


    # create SPARQL update query for updating the item in fcrepo
    def sparql_payload(self):
        query = []
        for ns,uri in NAMESPACE_BINDINGS.items():
            query.append("PREFIX {0}: <{1}>".format(ns,uri))
        query.append("INSERT DATA {")
        for p,o in self.triples:
            if o is not "":
                query.append("<> {0} '{1}' .".format(p, o))
        query.append("}")
        print("\n".join(query))
        return "\n".join(query)


    # update file in fcrepo        
    def create_file_object(self, patent_uri):
        self.file_triples = [("exterms:scandate", self.scan_date),
                             ("exterms:filename", self.filename),
                             ("dc:extent", self.pages),
                             ("pcdm:fileOf", patent_uri),
                             ("rdf:type", "pcdm:Object")
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
    
    # open a file to log assets loaded and URIs
    logfile = open(os.path.join(args.directory, "load.log"), 'w')
    
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
        print("Local path: {0}".format(r.filepath), end='')
        if r.file_exists():
            print(" => file exists.")
        else:
            print("ERROR!\n => Cannot access file {0}".format(r.filepath))
            continue
        
        print("SHA1 checksum: {0}".format(r.checksum))    
    
        # open transaction, create objects, update objects, commit
        print("Opening transaction to create resources... ", end='')
        response = requests.post("http://localhost:8080/fcrepo/rest/fcr:tx", 
                                 auth=(FEDORA_USER, FEDORA_PASSWORD)
                                 )
        print("{0}, transaction open".format(response))
        transaction = response.headers
        commit_uri = transaction['Location'] + "/fcr:tx/fcr:commit"
        
        patent_uri = create_rdfsource(REST_ENDPOINT)
        print(patent_uri)
        file_uri = upload_file( REST_ENDPOINT, 
                                r.filepath, 
                                r.checksum,
                                patent_uri
                                )
        sparql_update(patent_uri, r.sparql_payload())
        
        print("Committing transaction... ", end='')
        response = requests.post(commit_uri, 
                                 auth=(FEDORA_USER, FEDORA_PASSWORD)
                                 )
        if response.status_code == 204:
            print('{0} transaction complete!'.format(response))
            logfile.write("\t".join([r.title, patent_uri, file_uri]) + "\n")
        else:
            print('Failed!')


if __name__ == "__main__":
    main()
