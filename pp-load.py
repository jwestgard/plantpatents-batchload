#!/usr/bin/env python3

from __future__ import print_function

import argparse
from csv import DictReader
import hashlib
import os
import pprint
import requests
from io import BytesIO
import yaml

from rdflib import Graph, Literal, BNode, RDF, URIRef
from rdflib.namespace import Namespace, NamespaceManager


ns = {
    'bibo':         Namespace('http://purl.org/ontology/bibo/'),
    'cerif':        Namespace('http://eurocris.org/ontologies/cerif/0.1#'),
    'dc':           Namespace('http://purl.org/dc/elements/1.1/'),
    'ex':           Namespace('http://www.example.org/terms/'),
    'ldp':          Namespace('http://www.w3.org/ns/ldp#'),
    'pcdm':         Namespace('http://pcdm.org/models#'),
    'premis':       Namespace('http://www.loc.gov/premis/rdf/v1#'),
    'rdf':          Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#'),
    'uspatent':     Namespace('http://us.patents.aksw.org/patent/'),
    'uspatent-s':   Namespace('http://us.patents.aksw.org/schema/'),
    'xml':          Namespace('http://www.w3.org/XML/1998/namespace/'),
    'xmlns':        Namespace('http://www.w3.org/2000/xmlns/'),
    'xsd':          Namespace('http://www.w3.org/2001/XMLSchema#'),
    'xsi':          Namespace('http://www.w3.org/2001/XMLSchema-instance/')
    }



#============================================================================
# HELPER FUNCTIONS
#============================================================================

# verify communication with repository
def check_connection(endpoint, auth):
    response = requests.get(endpoint, auth=auth)
    if response == 200:
        return True
    else:
        return False



#============================================================================
# CLASSES
#============================================================================

class Resource():

    ''' methods for interacting with fcrepo resources '''

    def __init__(self, metadata):
        self.graph = Graph()
        self.subj = URIRef('')
        self.__dict__.update(metadata)
        namespace_manager = NamespaceManager(Graph())
        for k, v in ns.items():
            namespace_manager.bind(k, v, override=True, replace=True)
        self.graph.namespace_manager = namespace_manager


    # show the object's graph, serialized as turtle
    def print_graph(self):
        print(self.graph.serialize(format="turtle").decode())


    # create an empty resource
    def create_rdf(*kwargs):
        response = requests.post(REST_ENDPOINT, auth=auth)
        if response.status_code == 201:
            self.uri = response.text
            return True
        else:
            return False


    def create_nonrdf(*kwargs):
        data = open(self.file, 'rb').read()
        headers = {'Content-Type': 'application/octet-stream',
                   'Digest': 'sha1={0}'.format(self.checksum),
                   'Content-Disposition': 
                        'attachment; filename="{0}"'.format(self.filename)
                    }
        response = requests.post(uri, auth=auth, data=data, headers=headers)
        if response.status_code == 201:
            self.uri = response.text
            return True
        else:
            return False


    # update existing resource with sparql update
    def sparql_update(transaction):
        response = requests.patch(self.uri, auth=auth, data=self.graph)
        if response.status_code == 204:
            return True
        else:
            return False



class Patent(Resource):

    '''represents a plant patent resource'''

    def __init__(self, metadata):
        Resource.__init__(self, metadata)
        
        self.filename = os.path.basename(self.asset_path)
        self.file_metadata = {'pages': self.pages, 
                              'scan_date': self.scan_date,
                              'asset_path': self.asset_path
                              }
        self.file = File(self.file_metadata)
        
        self.graph.add(
            (self.subj, ns['dc'].title, Literal(self.title))
            )
        self.graph.add(
            (self.subj, ns['uspatent-s'].docNo, Literal(self.patent_number))
            )
        self.graph.add(
            (self.subj, ns['dc'].date, Literal(self.date,
                                                datatype=ns['xsd'].date))
            )
        self.graph.add(
            (self.subj, ns['dc'].subject, Literal(self.large_category))
            )
        self.graph.add(
            (self.subj, ns['bibo'].webpage, URIRef(self.patent_url))
            )
        self.graph.add(
            (self.subj, ns['rdf'].type, ns['pcdm'].Object)
            )
        self.graph.add(
            (self.subj, ns['ex'].otherid, Literal(self.uspc))
            )
        self.graph.add(
            (self.subj, ns['ex'].otherid, Literal(self.application_number))
            )
        for inventor in self.inventor.split(';'):
            if inventor is not "":
                self.graph.add(
                    (self.subj, ns['dc'].creator, Literal(inventor))
                    )
        for city in self.city.split(';'):
            if city is not "":
                self.graph.add(
                    (self.subj, ns['ex'].inventorCity, Literal(city))
                    )
        for state in self.state.split(';'):
            if state is not "":
                self.graph.add(
                    (self.subj, ns['ex'].inventorState, Literal(state))
                    )
        for country in self.country.split(';'):
            if country is not "":
                self.graph.add(
                    (self.subj, ns['ex'].inventorCountry, Literal(country))
                    )



class File(Resource):
    
    '''object representing an associated image resource'''
    
    def __init__(self, metadata):
        Resource.__init__(self, metadata)
        self.filename = os.path.basename(self.asset_path)
        self.checksum = self.sha1(self.asset_path)
        
        self.graph.add(
            (self.subj, ns['rdf'].type, ns['pcdm'].File)
            )
        self.graph.add(
            (self.subj, ns['bibo'].numPages, Literal(self.pages,
                                                  datatype=ns['xsd'].integer))
            )
        self.graph.add(
            (self.subj, ns['dc'].date, Literal(self.scan_date, 
                                               datatype=ns['xsd'].datetime))
            )
        self.graph.add(
            (self.subj, ns['rdf'].type, ns['pcdm'].File)
            )
    
    
    # confirm accessibility of a local asset
    def file_exists(self):
        if os.path.isfile(self.filepath):
            return True
        else:
            return False
    
    
    # generate SHA1 checksum on a file
    def sha1(self, path):
        BUF_SIZE = 65536
        sha1 = hashlib.sha1()
        with open(path, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()



class Transaction():

    ''' wrapper class for managing a Fedora 4 transaction 

        TRANSACTION SYNTAX:
        ===================
        start:      /fcr:tx
        act:        /tx:{transaction_id}/path/to/resource
        keepalive:  /tx:{transaction_id}/fcr:tx
        commit      /tx:{transaction_id}/fcr:commit
        rollback    /tx:{transaction_id}/fcr:rollback '''

    def __init__(self, endpoint, auth):
        response = requests.post('{0}/fcr:tx'.format(endpoint), auth=auth)
        if response.status_code == 201:
            self.auth = auth
            self.id = response.headers['Location']
            self.uri = "{0}/tx:{1}".format(endpoint, self.id)
            self.commit_uri = "{0}/fcr:commit".format(self.uri)
            self.rollback_uri = "{0}/fcr:rollback".format(self.uri)
            return self.uri
        else:
            return False


    # commit transaction
    def transaction_commit():
        response = requests.post(self.commit_uri, auth=auth)
        if response.status_code == 204:
            return True
        else:
            return False


    # rollback transaction
    def transaction_rollback(transaction_id, auth):
        response = requests.post(self.rollback_uri, auth=auth)
        if response.status_code == 204:
            return True
        else:
            return False



#============================================================================
# MAIN LOOP
#============================================================================

def main():
    
    '''Parse args, loop over local directory and create resources'''
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Load data to a Fedora4 repository')
    
    parser.add_argument('-c', '--config', required=True,
                        help='relative or absolute path to the YAML config file'
                        )
                        
    args = parser.parse_args()
    
    # print header
    title = "| PLANT PATENTS BATCH LOADER |"
    border = "-" * len(title)
    print("\n".join(["", border, title, border]))

    # load and parse specified configuration settings
    print('Loading configuration from specified file: {0}'.format(args.config))
    with open(args.config, 'r') as configfile:
        globals().update(yaml.safe_load(configfile))

    auth = (FEDORA_USER, FEDORA_PASSWORD)

    # open a file to log assets loaded and URIs
    print('Opening log file: {0}'.format(LOG_FILE))
    logfile = open(LOG_FILE, 'w')

    # load metadata file and parse
    print('Reading metadata file: {0}'.format(METADATA_FILE))
    with open(METADATA_FILE, 'r') as metafile:
        metadata = [row for row in DictReader(metafile)]
        print('  => Found {0} rows of data'.format(len(metadata)))
    
    # loop over local assets
    for row in metadata:
        row['asset_path'] = os.path.abspath(
                                os.path.join(
                                    ASSET_DIR, 
                                    row['patent_number'].lower() + ".pdf"
                                    )
                                )
                                        
    print('  => Found {0} local assets'.format(len(os.listdir(ASSET_DIR))))
    
    # loop over metadata, generate containers, and POST to fcrepo
    for n, row in enumerate(metadata):
        p = Patent(row)
        print("\n{0}. {1}: {2}".format(n+1, p.title, p.filename))
        print("  => Local path: {0}".format(p.asset_path))
        print("  => SHA1 checksum: {0}".format(p.file.checksum))
        
        # open transaction, create objects, update objects, commit
        
        print()
        
        # open transaction
        print("Opening transaction to create resources... ", end='')
        print()
        
        # create empty container
        print("Creating repository container... ", end='')
        print()
        
        # create binary
        print("Uploading binary reource... ", end='')
        print()
        
        # update patent metadata graph
        print("Updating patent metadata graph... ", end='')
        print()
        print()
        p.print_graph()
        
        # update binary metadata graph
        print("Updating binary metadata graph... ", end='')
        print()
        print()
        p.file.print_graph()
        
        # update patent resource
        print("Patching patent resource... ", end='')
        print()
        
        # update binary resource
        print("Patching file resource metadata... ", end='')
        print()
        
        # commit transaction or rollback
        print("Committing transaction... ", end='')
        print()
        
        # write to log
        print("")
        
    
        
    logfile.close()
    
    
if __name__ == "__main__":
    main()
    
    
'''
        patent_uri = create_rdfsource(transaction['Location'])
        file_uri = upload_file( transaction['Location'], 
                                r.filepath, 
                                r.checksum,
                                patent_uri
                                )
        sparql_update(patent_uri, r.sparql_payload())
        

        response = requests.post(commit_uri, 
                                 auth=(FEDORA_USER, FEDORA_PASSWORD)
                                 )
        if response.status_code == 204:
            print('{0} transaction complete!'.format(response))
            logfile.writeline("\t".join([r.title, patent_uri, file_uri]))
        else:
            print('Failed!')
'''
