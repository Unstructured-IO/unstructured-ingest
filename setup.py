"""
setup.py

unstructured-ingest - pre-processing tools for unstructured data

Copyright 2022 Unstructured Technologies, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from pathlib import Path
from typing import List, Union

from setuptools import find_packages, setup

from unstructured_ingest.__version__ import __version__


def load_requirements(file: Union[str, Path]) -> List[str]:
    path = file if isinstance(file, Path) else Path(file)
    requirements: List[str] = []
    if not path.is_file():
        raise FileNotFoundError(f"path does not point to a valid file: {path}")
    if not path.suffix == ".in":
        raise ValueError(f"file should have .in extension: {path}")
    file_dir = path.parent.resolve()
    with open(file, encoding="utf-8") as f:
        raw = f.read().splitlines()
        requirements.extend([r for r in raw if not r.startswith("#") and not r.startswith("-")])
        recursive_reqs = [r for r in raw if r.startswith("-r")]
    for recursive_req in recursive_reqs:
        file_spec = recursive_req.split()[-1]
        file_path = Path(file_dir) / file_spec
        requirements.extend(load_requirements(file=file_path.resolve()))
    # Remove duplicates and any blank entries
    return list({r for r in requirements if r})


csv_reqs = load_requirements("requirements/local_partition/tsv.in")
doc_reqs = load_requirements("requirements/local_partition/docx.in")
docx_reqs = load_requirements("requirements/local_partition/docx.in")
epub_reqs = load_requirements("requirements/local_partition/epub.in")
markdown_reqs = load_requirements("requirements/local_partition/md.in")
msg_reqs = load_requirements("requirements/local_partition/msg.in")
odt_reqs = load_requirements("requirements/local_partition/odt.in")
org_reqs = load_requirements("requirements/local_partition/org.in")
pdf_reqs = load_requirements("requirements/local_partition/pdf.in")
ppt_reqs = load_requirements("requirements/local_partition/pptx.in")
pptx_reqs = load_requirements("requirements/local_partition/pptx.in")
rtf_reqs = load_requirements("requirements/local_partition/rtf.in")
rst_reqs = load_requirements("requirements/local_partition/rst.in")
tsv_reqs = load_requirements("requirements/local_partition/tsv.in")
xlsx_reqs = load_requirements("requirements/local_partition/xlsx.in")

all_doc_reqs = list(
    set(
        csv_reqs
        + docx_reqs
        + epub_reqs
        + markdown_reqs
        + msg_reqs
        + odt_reqs
        + org_reqs
        + pdf_reqs
        + pptx_reqs
        + rtf_reqs
        + rst_reqs
        + tsv_reqs
        + xlsx_reqs,
    ),
)
connectors_reqs = {
    "airtable": load_requirements("requirements/connectors/airtable.in"),
    "astradb": load_requirements("requirements/connectors/astradb.in"),
    "azure": load_requirements("requirements/connectors/azure.in"),
    "azure-ai-search": load_requirements(
        "requirements/connectors/azure-ai-search.in",
    ),
    "biomed": load_requirements("requirements/connectors/biomed.in"),
    "box": load_requirements("requirements/connectors/box.in"),
    "chroma": load_requirements("requirements/connectors/chroma.in"),
    "clarifai": load_requirements("requirements/connectors/clarifai.in"),
    "confluence": load_requirements("requirements/connectors/confluence.in"),
    "couchbase": load_requirements("requirements/connectors/couchbase.in"),
    "delta-table": load_requirements("requirements/connectors/delta-table.in"),
    "discord": load_requirements("requirements/connectors/discord.in"),
    "dropbox": load_requirements("requirements/connectors/dropbox.in"),
    "duckdb": load_requirements("requirements/connectors/duckdb.in"),
    "elasticsearch": load_requirements("requirements/connectors/elasticsearch.in"),
    "gcs": load_requirements("requirements/connectors/gcs.in"),
    "github": load_requirements("requirements/connectors/github.in"),
    "gitlab": load_requirements("requirements/connectors/gitlab.in"),
    "google-drive": load_requirements("requirements/connectors/google-drive.in"),
    "hubspot": load_requirements("requirements/connectors/hubspot.in"),
    "jira": load_requirements("requirements/connectors/jira.in"),
    "kafka": load_requirements("requirements/connectors/kafka.in"),
    "kdbai": load_requirements("requirements/connectors/kdbai.in"),
    "lancedb": load_requirements("requirements/connectors/lancedb.in"),
    "milvus": load_requirements("requirements/connectors/milvus.in"),
    "mongodb": load_requirements("requirements/connectors/mongodb.in"),
    "neo4j": load_requirements("requirements/connectors/neo4j.in"),
    "notion": load_requirements("requirements/connectors/notion.in"),
    "onedrive": load_requirements("requirements/connectors/onedrive.in"),
    "opensearch": load_requirements("requirements/connectors/opensearch.in"),
    "outlook": load_requirements("requirements/connectors/outlook.in"),
    "pinecone": load_requirements("requirements/connectors/pinecone.in"),
    "postgres": load_requirements("requirements/connectors/postgres.in"),
    "qdrant": load_requirements("requirements/connectors/qdrant.in"),
    "reddit": load_requirements("requirements/connectors/reddit.in"),
    "redis": load_requirements("requirements/connectors/redis.in"),
    "s3": load_requirements("requirements/connectors/s3.in"),
    "sharepoint": load_requirements("requirements/connectors/sharepoint.in"),
    "salesforce": load_requirements("requirements/connectors/salesforce.in"),
    "sftp": load_requirements("requirements/connectors/sftp.in"),
    "slack": load_requirements("requirements/connectors/slack.in"),
    "snowflake": load_requirements("requirements/connectors/snowflake.in"),
    "wikipedia": load_requirements("requirements/connectors/wikipedia.in"),
    "weaviate": load_requirements("requirements/connectors/weaviate.in"),
    "databricks-volumes": load_requirements("requirements/connectors/databricks-volumes.in"),
    "databricks-delta-tables": load_requirements(
        "requirements/connectors/databricks-delta-tables.in"
    ),
    "singlestore": load_requirements("requirements/connectors/singlestore.in"),
    "vectara": load_requirements("requirements/connectors/vectara.in"),
    "vastdb": load_requirements("requirements/connectors/vastdb.in"),
    "zendesk": load_requirements("requirements/connectors/zendesk.in"),
}

embed_reqs = {
    "embed-huggingface": load_requirements("requirements/embed/huggingface.in"),
    "embed-octoai": load_requirements("requirements/embed/octoai.in"),
    "embed-vertexai": load_requirements("requirements/embed/vertexai.in"),
    "embed-voyageai": load_requirements("requirements/embed/voyageai.in"),
    "embed-mixedbreadai": load_requirements("requirements/embed/mixedbreadai.in"),
    "openai": load_requirements("requirements/embed/openai.in"),
    "bedrock": load_requirements("requirements/embed/bedrock.in"),
    "togetherai": load_requirements("requirements/embed/togetherai.in"),
}

docs_reqs = {
    "csv": csv_reqs,
    "doc": doc_reqs,
    "docx": docx_reqs,
    "epub": epub_reqs,
    "md": markdown_reqs,
    "msg": msg_reqs,
    "odt": odt_reqs,
    "org": org_reqs,
    "pdf": pdf_reqs,
    "ppt": ppt_reqs,
    "pptx": pptx_reqs,
    "rtf": rtf_reqs,
    "rst": rst_reqs,
    "tsv": tsv_reqs,
    "xlsx": xlsx_reqs,
}

extras_require = {
    "remote": load_requirements("requirements/remote/client.in"),
}
for d in [docs_reqs, connectors_reqs, embed_reqs]:
    extras_require.update(d)

setup(
    name="unstructured-ingest",
    description="A library that prepares raw documents for downstream ML tasks.",
    long_description=open("README.md", encoding="utf-8").read(),  # noqa: SIM115
    long_description_content_type="text/markdown",
    keywords="NLP PDF HTML CV XML parsing preprocessing",
    url="https://github.com/Unstructured-IO/unstructured-ingest",
    python_requires=">=3.9.0,<3.14",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    author="Unstructured Technologies",
    author_email="devops@unstructuredai.io",
    license="Apache-2.0",
    packages=find_packages(),
    version=__version__,
    entry_points={
        "console_scripts": ["unstructured-ingest=unstructured_ingest.main:main"],
    },
    install_requires=load_requirements("requirements/common/base.in"),
    extras_require=extras_require,
    package_dir={"unstructured_ingest": "unstructured_ingest"},
    package_data={"unstructured_ingest": ["py.typed"]},
    include_package_data=True,
)
