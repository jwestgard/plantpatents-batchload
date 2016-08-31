# plantpatents-batchload
This script takes a directory of binaries and CSV metadata, constructs objects according to a simple PCDM model, and POSTs the necessary containers and binaries to a Fedora 4 repository via its REST API.

### Usage
```python pp-load.py -c config.yml /path/to/resources```

The resources to be loaded are assumed to be in the same file with a metadata.csv file containing metadata to be converted to RDF triples.  The config.yml contains repository configuration settings, and namespace bindings for RDF predicates to be loaded to the Fedora repository.
