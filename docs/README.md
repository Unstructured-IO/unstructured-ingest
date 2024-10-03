# Developing V2 Connectors
## Intro
The Unstructured open source library processes documents (artifacts) in a pipeline. The source and destination connectors sit at the front and back of the pipeline. For a visual example see the [sequence diagram](#sequence-diagram).

## Basic Example of a Pipeline
The most basic example of a pipeline starts with a local source connector, followed by a partitioner, and then ends with a local destination connector. Here is what the code to run this looks like:

>*** This is the type of Python file you'll want to run during development so that you can iterate on your connector.

`local.py`

```
from unstructured_ingest.v2.interfaces import ProcessorConfig
from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.processes.connectors.local import (
    LocalConnectionConfig,
    LocalDownloaderConfig,
    LocalIndexerConfig,
    LocalUploaderConfig,
)
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig

if __name__ == "__main__":
    Pipeline.from_configs(
        context=ProcessorConfig(
            verbose=True,
            work_dir="local-working-dir",
            reprocess=True,
            re_download=True,
        ),
        source_connection_config=LocalConnectionConfig(),
        indexer_config=LocalIndexerConfig(input_path="example-docs/fake-text.txt"),
        downloader_config=LocalDownloaderConfig(),
        partitioner_config=PartitionerConfig(),
        uploader_config=LocalUploaderConfig(output_dir="local-working-dir/output"),
    ).run()
```
You can run this with `python local.py` (Adjust the `input_path` and `output_dir` as appropriate.)

The result is a partitioned `fake-text.txt.json` file in the `local-output` directory.



Notice that the pipeline runs the following:

* context - The `ProcessorConfig` runs the pipeline. The arguments are related to the overall pipeline. We added some args (`verbose`, `work_dir`) to make development easier.
* source_connection - Takes arguments needed to connect to the source. Local files don't need anything here. Other connectors will.
* indexer - Takes the files in the `input_path` and creates `.json` files that point the downloader step to the right files. 
* downloader - This does the actual downloading of the raw files (for non-blob files it may do something different like create a `.txt` file for every row in a source table)
* partitioner - Partitions the downloaded file, provided it is a partionable file type. [See the supported file types.](https://github.com/Unstructured-IO/unstructured/blob/0c562d80503f6ef96504c6e38f27cfd9da8761df/unstructured/file_utils/filetype.py)
* chunker/embedder - *Not represented here* but often needed to prepare files for upload to a vector database.
* stager - *Not represented here* but is often used to prepare partitioned files for upload.
* uploader - Uploads the blob-like files to the `output_dir`.


If you look at the folders/files in `local-working-dir` you will see the files that the pipeline creates as it runs.

```
local-working-dir
- index
  - a4a1035d57ed.json
- output
  - fake-text.txt.json
- partition
  - 36caa9b04378.json
```

(Note that the index and partition file names are deterministic and based on the hash of the current step along with the previous step's hash.) In the case of the local source connector, it won't *download* files because they are already local. But for other source connectors there will be a `download` folder. Also note that the final file is named based on the original file with a `.json` extension since it has been partitioned. Not all output files will be named the same as the input file. (ex. database like sources)

You can see the source/destination connector file that it runs here:

https://github.com/Unstructured-IO/unstructured-ingest/blob/main/unstructured_ingest/v2/processes/connectors/local.py

If you look through the file you will notice these interfaces and functions

* LocalAccessConfig - This usually holds passwords, tokens, etc. This data gets hidden in all logs (and encrypted in our platform solution).

* LocalConnectionConfig - Username, host, port, etc. Anything needed for connecting to the service. It also imports the `AccessConfig`.

* LocalIndexerConfig - Holds attributes that allow Indexer to connect to the service and what kind of documents to filter for.

* LocalIndexer - Does the actual file listing and filtering. Note that it yields a FileData object.

* LocalDownloaderConfig - In this case it doesn't need anything if the LocalIndexerConfig already provides it.

* LocalDownloader - Does the actual downloading of the raw files.

* LocalUploaderConfig - Arguments for upload location

* LocalUploader - Does the actual uploading

* local_source_entry - Used to register the source connector here: [`unstructured_ingest/v2/processes/connectors/__init__.py`](https://github.com/Unstructured-IO/unstructured_ingest/blob/main/unstructured_ingest/v2/processes/connectors/__init__.py)

* local_destination_entry - Used to register the destination connector here: [`unstructured_ingest/v2/processes/connectors/__init__.py`](https://github.com/Unstructured-IO/unstructured_ingest/blob/main/unstructured_ingest/v2/processes/connectors/__init__.py)



## Building a Destination Connector
We'll start with building a destination connector because they are easier to build than source connectors.

In this case we'll use the Chroma vector database destination because:

* The service can be hosted locally.  https://docs.trychroma.com/guides
* We can show off the chunking and embedding step (used for vector database destinations)
* It uses a staging step to prepare the artifacts before uploading
* You can examine the Chroma database file easily since its just a sqlite database


The python file to iterate on development looks like this:

`chroma.py`

```
import random # So we get a new Chroma collections on every run

from unstructured_ingest.v2.interfaces import ProcessorConfig
from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.processes.chunker import ChunkerConfig
from unstructured_ingest.v2.processes.connectors.chroma import (
    ChromaAccessConfig,
    ChromaConnectionConfig,
    ChromaUploaderConfig,
    ChromaUploadStagerConfig,
)
from unstructured_ingest.v2.processes.connectors.local import (
    LocalConnectionConfig,
    LocalDownloaderConfig,
    LocalIndexerConfig,
)
from unstructured_ingest.v2.processes.embedder import EmbedderConfig
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig

if __name__ == "__main__":
    Pipeline.from_configs(
        context=ProcessorConfig(
            verbose=True,
            work_dir="chroma-working-dir",
            reprocess=True,
            re_download=True,
        ),
        source_connection_config=LocalConnectionConfig(),
        indexer_config=LocalIndexerConfig(input_path="example-docs/fake-text.txt"),
        downloader_config=LocalDownloaderConfig(),
        partitioner_config=PartitionerConfig(),

        chunker_config=ChunkerConfig(
            chunking_strategy="basic",
        ),
        embedder_config=EmbedderConfig(embedding_provider="langchain-huggingface"),
        
        destination_connection_config=ChromaConnectionConfig(
            access_config=ChromaAccessConfig(settings=None, headers=None),
            host="localhost",
            port=8000,
            collection_name=f"test-collection-{random.randint(1000,9999)}",
        ),
        stager_config=ChromaUploadStagerConfig(),
        uploader_config=ChromaUploaderConfig(batch_size=10),
    ).run()

```

Notice how the top part looks similar to the local connector running file. (link to local connector file above) But now we are adding a **chunker** and an **embedder**. Also note that there is a stager_config. This is where we prepare the document/artifact in a custom way before running the Uploader.

Let's run it.



* Make sure your python environment is set up and then run `pip install "unstructured[chroma]"`

* In a separate terminal (with chroma installed) run
`./scripts/chroma-test-helpers/create-and-check-chroma.sh chroma-db-file`
the service should now be running on port 8000

* Run your example file: `python chroma.py`
* You can examine the resulting sqlite database (`chroma.sqlite3`) in the `chroma-db-file` directory if you want to see the results.


Let's look at the python file in the Unstructured repo

https://github.com/Unstructured-IO/unstructured-ingest/blob/main/unstructured_ingest/v2/processes/connectors/chroma.py

* ChromaAccessConfig - Needed for connecting to Chroma. Usually sensitive attributes that will be hidden.

* ChromaConnectionConfig - Non sensitive attributes needed to connect to the client. `collection_name` does not have a default value. `access_config` imports the ChromaAccessConfig and hides the values via `enhanced_field(sensitive=True)`

* ChromaUploadStagerConfig - The Stager config. Didn't need anything for Chroma.

* ChromaUploadStager - The conform_dict is the critical method here. It takes the file we get from the Embedder step and prepares it for upload to the Chroma database. But it does not upload it. It saves the file to the `upload_stage` directory. The file type can be whatever makes sense for the Uploader phase (`.json`, `.csv`, `.txt`).

* ChromaUploaderConfig - Attributes that are necessary for the upload stage specifically. The ChromaUploader will be upserting artifacts in batches.

* ChromaUploader - Connects to the Client. And uploads artifacts. Note that it does the minimum amount of processing possible to the artifacts before uploading. The Stager phase is responsible for preparing artifacts. Chroma wants artifacts in a dictionary of lists so we do have to create that in the Uploader since there is not a practical way to store that in a `.json` file.

* chroma_destination_entry - Registers the Chroma destination connector with the pipeline. ([`unstructured_ingest/v2/processes/connectors/__init__.py`](https://github.com/Unstructured-IO/unstructured_ingest/blob/main/unstructured_ingest/v2/processes/connectors/__init__.py))

Note that the `chroma.py` file imports the official Chroma python package when it *creates* the client and not at the top of the file. This allows the classes to be *instantiated* without error,They will raise a runtime error though if the imports are missing.

Let's take a quick look at the `upload_stage` in  working directory:
```
chroma-working-dir
- chunk
  - f0987c36c3b0.json
- embed
  - dafc7add1d21.json
- index
  - a4a1035d57ed.json
- partition
  - 36caa9b04378.json
- upload_stage
  - e17715933baf.json
```
`e17715933baf.json` in the `upload_stage` is a `.json` file which is appropriate for this destination connector. But it could very well be a `.csv` (or file of your choosing) if the uploader is a relational database. 

If the destination is blob(file) storage, like AWS S3, you may not need the Staging phase. The partitioned/embedded `.json` file would be uploaded directly.

### Additional files

When you make a **new** destination connector you will need these files first:

* `unstructured_ingest/v2/processes/connectors/your_connector.py`
* And add that to: `unstructured_ingest/v2/processes/connectors/__init__.py`
* Your python file to iterate on development. You can call it `unstructured_ingest/v2/examples/example_your_connector.py`
* And some form of **live connection** to the destination service. In the case of Chroma we have a local service running. Often we will run a docker container (Elasticsearch). At other times we will use a hosted service if there is no docker image (Pinecone).

Once the connector is worked out with those files, you will need to add a few more files. 

* `unstructured_ingest/v2/cli/cmds/your_connector.py`
* Add that to: `unstructured_ingest/v2/cli/cmds/__init__.py`
* Makefile
* Manifest.in
* setup.py
* your_connector.in (to create the requirements file)
* Documentation

The CLI file. This allows the connector to be run via the command line. All the arguments for the connector need to be exposed.

`unstructured_ingest/v2/cli/cmds/your_connector.py`


### Intrgration Test
And lastly we need an executable .sh file that runs in CI/CD as an integration? test.

`test_unstructured_ingest/dest/weaviate.sh` is a good example because it uses a Docker container to act as the Weaviate service. 

If you run `./test_unstructured_ingest/dest/weaviate.sh` from the root it will spin up a docker container. Create a blank `elements` collection based on the schema. Partition `fake-memo.pdf`. Embed the artifact with vector embeddings. Upload the artifact to the Weaviate vector database. And then it runs `/python/test-ingest-weaviate-output.py` which counts the number of embeddings that were loaded.

In an ideal world, for a vector database destination, the test will also do a vector search and validate the results. (`scripts/elasticsearch-test-helpers/destination_connector/test-ingest-elasticsearch-output.py` is an example of this.)

If you can run the integration test successfully then most of the files should be in order.

## Building a Source Connector

The source connector example we will use is `onedrive.py`. The S3 connector might be a simpler example, but it relies on the incredibly useful fsspec package, so it is not a good general example.
https://filesystem-spec.readthedocs.io/en/latest/ 
If your source connector can take advantage of fsspec, then S3 might be one to check out.


The source connector instructions are similar to the destination connector  above.

But the key difference is the Indexer. The Indexer essentially gets a list of the documents/artifacts in the source service. (In the case of a local connector it would be like a bash `ls` command). It then creates individual files for each artifact that need to be downloaded. This is so that the next phase, the Downloader phase, can be scaled out with multiple workers. The Indexer phase needs to return pointers to those artifacts in the shape of the FileData object, which it then downloads as `.json` files.

The Downloader then uses the `.json` files that the Indexer created and downloads the raw files (in the case of a blob type file, .pdf, .txt) or as individual rows in a table, or any other needed format.

Here are some of the file types it can download and partition. 
https://github.com/Unstructured-IO/unstructured/blob/0c562d80503f6ef96504c6e38f27cfd9da8761df/unstructured/file_utils/filetype.py

The Indexer files (resulting `.json` files in the index folder) also contain metadata that will be used to determine if the files have already been processed.

The file to use for iteration would look like this:

>*** This is the type of Python file you'll want to run during development so that you can iterate on your connector.

`onedrive.py`

```
import os

from unstructured_ingest.v2.interfaces import ProcessorConfig
from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.processes.connectors.local import (
    LocalUploaderConfig,
)
from unstructured_ingest.v2.processes.connectors.onedrive import (
    OnedriveAccessConfig,
    OnedriveConnectionConfig,
    OnedriveDownloaderConfig,
    OnedriveIndexerConfig,
)
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig

if __name__ == "__main__":
    Pipeline.from_configs(
        context=ProcessorConfig(
            verbose=True,
            work_dir="onedrive-working-dir",
            reprocess=True,
            re_download=True,
        ),
        indexer_config=OnedriveIndexerConfig(path="/utic-test-ingest-fixtures", recursive=True),
        downloader_config=OnedriveDownloaderConfig(download_dir="onedrive-working-dir/download"),
        source_connection_config=OnedriveConnectionConfig(
            client_id=os.getenv("MS_CLIENT_ID"),
            user_pname=os.getenv("MS_USER_PNAME"),
            tenant=os.getenv("MS_TENANT_ID"),
            access_config=OnedriveAccessConfig(client_cred=os.getenv("MS_CLIENT_CRED")),
        ),
        partitioner_config=PartitionerConfig(),
        uploader_config=LocalUploaderConfig(output_dir="onedrive-working-dir/output"),
    ).run()
```
To run this would require service credentials for Onedrive. And we can't run a Docker container locally. So we will skip that part. But this still gives a good example of the kind of file you need to iterate with.

Let's look at the source connector file that it runs.

https://github.com/Unstructured-IO/unstructured_ingest/blob/main/unstructured_ingest/v2/processes/connectors/onedrive.py

If you look through the file you will notice these interfaces and functions

* OnedriveAccessConfig - Holds client credentials. This data gets hidden in all logs (and encrypted in our platform solution)

* OnedriveConnectionConfig - Client id, etc. Anything needed for connecting to the service. It also imports the AccessConfig. Notice the `@requires_dependencies` decorator which imports the microsoft `msal` package for that method.

* OnedriveIndexerConfig - Holds attributes that allow Indexer to connect to the service. Since Onedrive has a folder structure we allow `recursive` indexing. It will go down into all the folders of the `path`.

* OnedriveIndexer - Does the actual file listing. Note that it yields a FileData object

* OnedriveDownloaderConfig - In this case it doesn't need anything that the OndriveIndexerConfig already provides.

* OnedriveDownloader - Does the actual downloading of the raw files.

And those are the basics of a source connector. Each connector will have its specific problems to sort out. 

### Additional Files

See the additional files above. (link to section)

### Intrgration Test

We need a test to run in the CI/CD. See the Chroma integration test section above. (link to section)

## Conclusion

Building a connector is relatively straightforward, especially if there is an existing connector that closely matches the new one. For example, most of the vector destinations are quite similar.

If you have any questions post in the public Slack channel `ask-for-help-open-source-library`

### Sequence Diagram

Yellow (without the Uncompressing) represents the steps in a source connector. Orange represents a destination connector.

![unstructured_ingest diagram](assets/pipeline.png)



