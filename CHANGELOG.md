## 0.0.4-dev0

### Enhancements

### Features

### Fixes
* **Fix TypeError** Avoid raising TypeError when getting date_modified and date_created metadata for onedrive connector

## 0.0.3

### Enhancements

* **Improve documentation** Update the README's.
* **Explicit Opensearch classes** For the connector registry entries for opensearch, use only opensearch specific classes rather than any elasticsearch ones. 
* **Add missing fsspec destination precheck** check connection in precheck for all fsspec-based destination connectors

## 0.0.2

### Enhancements

* **Use uuid for s3 identifiers** Update unique id to use uuid derived from file path rather than the filepath itself.
* **V2 connectors precheck support** All steps in the v2 pipeline support an optional precheck call, which encompasses the previous check connection functionality. 
* **Filter Step** Support dedicated step as part of the pipeline to filter documents.

## 0.0.1

### Enhancements

### Features

* **Add Milvus destination connector** Adds support storing artifacts in Milvus vector database.

### Fixes

* **Remove old repo references** Any mention of the repo this project came from was removed. 

## 0.0.0

### Features

* **Initial Migration** Create the structure of this repo from the original code in the [Unstructured](https://github.com/Unstructured-IO/unstructured) project.

### Fixes
