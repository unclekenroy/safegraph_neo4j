# SafeGraph to Neo4j Pipeline

This repository contains a pipeline for SafeGraph COVID-19 mobility data to Neo4j DB. It was written by Marcus Bedeau for his TACC 2021 REU project.

## Quick Start

1. Spin up a Neo4j instance:
```bash
docker run --rm -it \
    -p7474:7474 -p7687:7687 \
    -v $HOME/neo4j/data:/data \
    -v $HOME/neo4j/logs:/logs \
    -v $HOME/neo4j/import:/var/lib/neo4j/import \
    -v $HOME/neo4j/plugins:/plugins \
    --env NEO4J_AUTH=neo4j/test \
    neo4j:latest
```

2. Import SafeGraph data from CSV:
```bash
poetry install
poetry run python3 scripts/just_cbg.py -f data/safegraph/feb2020.csv
```

## Authors

- Marcus Bedeau (`unclekenroy`)
- Ethan Ho (`eho-tacc`)

## Neo4j on TACC

Ethan Ho 7/16/2021

- Managed to get neo4j Docker image running on TACC
- Can copy the `neo4j.conf` from Docker container:
```bash
docker run --rm -it neo4j:latest cat /var/lib/neo4j/conf/neo4j.conf > neo4j.conf
```
- Then rsync it to the `neo4j/conf` dir and invoke in detached container:
```bash
$ ls neo4j
conf  data  import  logs  plugins
$ singularity pull neo4j_latest.sif docker://neo4j:latest
$ nohup singularity exec \
    -B neo4j/data:/data \
    -B neo4j/logs:/logs \
    -B neo4j/import:/var/lib/neo4j/import \
    -B neo4j/conf:/var/lib/neo4j/conf \
    -B neo4j/plugins:/plugins \
    --env NEO4J_AUTH=neo4j/password \
    neo4j_latest.sif /docker-entrypoint.sh neo4j &
$ curl localhost:7474
{
  "bolt_routing" : "neo4j://localhost:7687",
  "transaction" : "http://localhost:7474/db/{databaseName}/tx",
  "bolt_direct" : "bolt://localhost:7687",
  "neo4j_version" : "4.3.2",
  "neo4j_edition" : "community"
}
```

Then we can connect via bolt using the Python API:

```python
from neo4j import GraphDatabase 
driver = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "password"))
session = driver.session()
session.run('MATCH (n) RETURN n LIMIT 25')
# <neo4j.work.result.Result at 0x2ae5e1427d60>
```

As a one-liner:

```bash
$ python3 -c 'from neo4j import GraphDatabase; driver = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "password")); session = driver.session(); print(session.run("MATCH (n) RETURN n LIMIT 25"))'
<neo4j.work.result.Result object at 0x2ba4ffee8040>
```