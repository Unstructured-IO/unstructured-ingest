import duckdb
import sys
from pathlib import Path

if __name__ == "__main__":
    connection = duckdb.connect(sys.argv[1])

    query = None
    script_path = (Path(__file__).parent / Path("create-duckdb-schema.sql")).resolve()
    with open(script_path) as f:
        query = f.read()    
    connection.execute(query)
    connection.close()