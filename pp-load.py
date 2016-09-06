#!/usr/bin/env python3

from __future__ import print_function

import argparse
from csv import DictReader
import hashlib
from io import BytesIO
import os
import requests
import sys
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
    if response.status_code == 200:
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
        self.uri = URIRef('')
        self.__dict__.update(metadata)
        namespace_manager = NamespaceManager(Graph())
        for k, v in ns.items():
            namespace_manager.bind(k, v, override=True, replace=True)
        self.graph.namespace_manager = namespace_manager


    # show the object's graph, serialized as turtle
    def print_graph(self):
        print(self.graph.serialize(format="turtle").decode())


    # create an rdfsource container resource
    def create_rdf(self, endpoint, auth):
        headers = {'Content-Type': 'text/turtle'}
        data = self.graph.serialize(format="turtle")
        response = requests.post(endpoint, 
                                 auth=auth, 
                                 data=data,
                                 headers=headers
                                 )
        if response.status_code == 201:
            self.uri = REST_ENDPOINT + response.text[len(endpoint):]
            return True
        else:
            return False


    # upload a binary resource
    def create_nonrdf(self, endpoint, auth):
        data = open(self.local_path, 'rb').read()
        headers = {'Content-Type': 'application/pdf',
                   'Digest': 'sha1={0}'.format(self.checksum),
                   'Content-Disposition': 
                        'attachment; filename="{0}"'.format(self.filename)
                    }
        response = requests.post(endpoint, 
                                 auth=auth, 
                                 data=data, 
                                 headers=headers
                                 )
        if response.status_code == 201:
            self.uri = REST_ENDPOINT + response.text[len(endpoint):]
            return True
        else:
            return False


    # update the subject of graph after URI assignment
    def update_graph(self):
        for (s, p, o) in self.graph:
            self.graph.remove((s, p, o))
            self.graph.add((URIRef(self.uri), p, o))


    # update existing resource with sparql update
    def sparql_update(self, endpoint, auth, triples):
        data = ['PREFIX pcdm: <http://pcdm.org/models#>']
        data.append('INSERT {')
        for (prefix, predicate, object) in triples:
            data.append('<> {0}:{1} "{2}" .'.format(prefix, predicate, object))
        data.append('} WHERE { }')
        payload = '\n'.join(data)
        headers = {'Content-Type': 'application/sparql-update'}
        response = requests.patch(endpoint, 
                                  auth=auth, 
                                  data=payload,
                                  headers=headers
                                  )
        if response.status_code == 204:
            return True
        else:
            print(response, response.headers)
            print(response.text)
            return False
    
    
    # replace triples on existing resource
    def put_graph(self, endpoint, auth):
        data = self.graph.serialize(format='turtle')
        headers = {'Content-Type': 'text/turtle'}
        response = requests.put(endpoint, 
                                auth=auth, 
                                data=data,
                                headers=headers
                                )
        if response.status_code == 204:
            return True
        else:
            print(response)
            print(response.headers)
            print(response.text)
            return False



class Patent(Resource):

    '''represents a plant patent resource'''

    def __init__(self, metadata):
        Resource.__init__(self, metadata)
        
        self.filename = os.path.basename(self.asset_path)
        self.file_metadata = {'pages': self.pages, 
                              'scan_date': self.scan_date,
                              'local_path': self.asset_path
                              }
        self.file = File(self.file_metadata)
        
        self.graph.add(
            (self.uri, ns['dc'].title, Literal(self.title))
            )
        self.graph.add(
            (self.uri, ns['uspatent-s'].docNo, Literal(self.patent_number))
            )
        self.graph.add(
            (self.uri, ns['dc'].date, Literal(self.date,
                                                datatype=ns['xsd'].date))
            )
        self.graph.add(
            (self.uri, ns['dc'].subject, Literal(self.large_category))
            )
        self.graph.add(
            (self.uri, ns['bibo'].webpage, URIRef(self.patent_url))
            )
        self.graph.add(
            (self.uri, ns['rdf'].type, ns['pcdm'].Object)
            )
        self.graph.add(
            (self.uri, ns['ex'].otherid, Literal(self.uspc))
            )
        self.graph.add(
            (self.uri, ns['ex'].otherid, Literal(self.application_number))
            )
        for inventor in self.inventor.split(';'):
            if inventor is not "":
                self.graph.add(
                    (self.uri, ns['dc'].creator, Literal(inventor))
                    )
        for city in self.city.split(';'):
            if city is not "":
                self.graph.add(
                    (self.uri, ns['ex'].inventorCity, Literal(city))
                    )
        for state in self.state.split(';'):
            if state is not "":
                self.graph.add(
                    (self.uri, ns['ex'].inventorState, Literal(state))
                    )
        for country in self.country.split(';'):
            if country is not "":
                self.graph.add(
                    (self.uri, ns['ex'].inventorCountry, Literal(country))
                    )



class File(Resource):
    
    '''object representing an associated image resource'''
    
    def __init__(self, metadata):
        Resource.__init__(self, metadata)
        self.filename = os.path.basename(self.local_path)
        self.checksum = self.sha1()
        
        self.graph.add(
            (self.uri, ns['rdf'].type, ns['pcdm'].File)
            )
        self.graph.add(
            (self.uri, ns['bibo'].numPages, Literal(self.pages,
                                                  datatype=ns['xsd'].integer))
            )
        self.graph.add(
            (self.uri, ns['dc'].date, Literal(self.scan_date, 
                                               datatype=ns['xsd'].datetime))
            )
        self.graph.add(
            (self.uri, ns['rdf'].type, ns['pcdm'].File)
            )
    
    
    # confirm accessibility of a local asset
    def file_exists(self):
        if os.path.isfile(self.local_path):
            return True
        else:
            return False
    
    
    # generate SHA1 checksum on a file
    def sha1(self):
        BUF_SIZE = 65536
        sha1 = hashlib.sha1()
        with open(self.local_path, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()



class Transaction():

    ''' Wrapper class for managing a Fedora 4 transaction 

        TRANSACTION SYNTAX:
        ===================
        start:      /fcr:tx
        act:        /tx:{transaction_id}/path/to/resource
        keepalive:  /tx:{transaction_id}/fcr:tx
        commit      /tx:{transaction_id}/fcr:tx/fcr:commit
        rollback    /tx:{transaction_id}/fcr:tx/fcr:rollback '''

    def __init__(self, endpoint, auth):
        response = requests.post('{0}/fcr:tx'.format(endpoint), auth=auth)
        if response.status_code == 201:
            self.auth = auth
            self.uri = response.headers['Location']
            self.id = self.uri[len(endpoint):]
            self.commit_uri = "{0}/fcr:tx/fcr:commit".format(self.uri)
            self.rollback_uri = "{0}/fcr:tx/fcr:rollback".format(self.uri)


    # commit transaction
    def commit(self):
        response = requests.post(self.commit_uri, auth=self.auth)
        if response.status_code == 204:
            return True
        else:
            return False


    # rollback transaction
    def rollback(self):
        response = requests.post(self.rollback_uri, auth=self.auth)
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
    logfile = open(LOG_FILE, 'w', 1)

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
    
    # check repository connection
    print("Checking repository connection... ", end='')
    if check_connection(REST_ENDPOINT, auth):
        print("success!")
    else:
        print("failed!")
        print("Verify that your repository is online and try again.")
        sys.exit()
    
    
    # loop over metadata, generate containers, and POST to fcrepo
    for n, row in enumerate(metadata):
        patent = Patent(row)
        print("\n{0}. {1}: {2}".format(n+1, patent.title, patent.filename))
        print("  => Local path: {0}".format(patent.asset_path))
        print("  => SHA1 checksum: {0}\n".format(patent.file.checksum))
        
        # open transaction
        print("Opening transaction to create resources... ", end='')
        transaction = Transaction(REST_ENDPOINT, auth)
        print("ID:{0}".format(transaction.id))
        
        # create empty container
        print("Creating repository container... ", end='')
        if patent.create_rdf(transaction.uri, auth):
            print("success!")
        else:
            print("failed!")
            transaction.rollback()
            continue
        
        # create binary
        print("Uploading binary reource... ", end='')
        if patent.file.create_nonrdf(transaction.uri, auth):
            print("success!")
        else:
            print("failed!")
            transaction.rollback()
            continue
        
        # update and patch patent metadata graph
        '''print("Updating patent metadata graph... ", end='')
        sparql_uri = "{0}{1}{2}".format(REST_ENDPOINT,
                                        transaction.id,
                                        patent.uri[len(REST_ENDPOINT):]
                                        )
        triples = [('pcdm', 'hasFile', patent.file.uri)]
        if patent.sparql_update(sparql_uri, auth, triples):
            print("success!")
        else:
            print("failed!")
            transaction.rollback()
            continue'''
        
        # update and patch binary metadata graph
        print("Updating binary metadata graph... ", end='')
        sparql_uri = "{0}{1}{2}{3}".format(REST_ENDPOINT,
                                           transaction.id,
                                           patent.file.uri[len(REST_ENDPOINT):],
                                           "/fcr:metadata"
                                           )
        triples = [('pcdm', 'fileOf', patent.uri)]
        if patent.file.sparql_update(sparql_uri, auth, triples):
            print("success!")
        else:
            print("failed!")
            transaction.rollback()
            continue
        
        # commit transaction or rollback
        print("Committing transaction... ", end='')
        if transaction.commit():
            print("success!")
        else:
            print("failed!")
            transaction.rollback()
        
        print("\t".join([patent.uri, patent.file.uri]))
        # write to log
        logfile.write("\t".join([patent.uri, patent.file.uri]) + "\n")


    # after all files processed, close log
    logfile.close()


if __name__ == "__main__":
    main()


