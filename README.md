# plantpatents-batchload
This script takes a directory of binaries and CSV metadata, constructs objects according to a simple PCDM model, and POSTs the necessary containers and binaries to a Fedora 4 repository via its REST API.

### Usage
```python pp-load.py -c config.yml /path/to/resources```

The resources to be loaded are assumed to be in the same directory with a file containing the metadata (metadata.csv) that is to be converted to RDF triples.  The config.yml contains repository configuration settings, and namespace bindings for RDF predicates to be loaded to the Fedora repository.

The script has been developed using Python 3, but in theory should run under either Python 2 or 3. The current version makes a number of assumptions about the content to be loaded and the metadata describing it, but a future version may be developed into a more general-purpose loading tool.
