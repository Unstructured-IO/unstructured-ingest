"""Local STDIO MCP server that lands Unstructured Transform MCP output into a
local vector store for retrieval.

This module pairs with the remote Transform MCP. Transform parses, chunks, and
(optionally) embeds documents on the platform and hands back Element JSON; this
server fetches that JSON out of band, embeds it locally when Transform did not,
upserts it into a local Chroma collection, and serves matched-space similarity
search. See ``README.md`` for the end-to-end flow.
"""
