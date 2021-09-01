#!/bin/bash
#
#-----------------------------------------------------------------------------
# Ethan Ho 7/16/2021
#-----------------------------------------------------------------------------
#
#SBATCH -J safegraph_neo4j            # Job name
#SBATCH -o safegraph_neo4j.o%j
#SBATCH -e safegraph_neo4j.e%j
#SBATCH -N 1                          # Total number of nodes requested (56 cores/node)
#SBATCH -n 1                          # Total number of mpi tasks requested

# module configuration
echo "unloading xalt"
module unload xalt
module load python3/3.9.2 tacc-singularity
module list
python3 -m pip freeze

#--------------------------------------------------------------------------
# ---- You normally should not need to edit anything below this point -----
#--------------------------------------------------------------------------

# getopts
OPTIND=1
PYSCRIPT=
NEO_HOME=./neo4j
NEO_PASS=test
while getopts "s:p:" opt; do
    case "$opt" in
    s)  PYSCRIPT=$OPTARG
        ;;
    p)  NEO_PASS=$OPTARG
        ;;
    esac
done
shift $((OPTIND-1))
[ "${1:-}" = "--" ] && shift

# param checking
[ -z $PYSCRIPT ] && echo "ERROR: must pass a PYSCRIPT via -p option" && exit 1
[ -f $PYSCRIPT ] && echo "ERROR: PYSCRIPT at ${PYSCRIPT} does not exist" && exit 1

# create neo4j dirs if DNE
mkdir -p \
    $NEO_HOME/data \
    $NEO_HOME/logs \
    $NEO_HOME/import \
    $NEO_HOME/conf \
    $NEO_HOME/plugins
[ -f $NEO_HOME/conf/neo4j.conf ] || echo "WARNING: no neo4j.conf found"

# start neo4j from public Docker image
nohup singularity exec \
    -B $NEO_HOME/data:/data \
    -B $NEO_HOME/logs:/logs \
    -B $NEO_HOME/import:/var/lib/neo4j/import \
    -B $NEO_HOME/conf:/var/lib/neo4j/conf \
    -B $NEO_HOME/plugins:/plugins \
    --env NEO4J_AUTH=neo4j/${NEO_PASS} \
    docker://neo4j:latest /docker-entrypoint.sh neo4j &
WAIT=10
echo "Sleeping ${WAIT} seconds..."
sleep $WAIT
echo "curling localhost:7474..."
curl localhost:7474 | jq -rc .
# Expecting:
# {
#   "bolt_routing" : "neo4j://localhost:7687",
#   "transaction" : "http://localhost:7474/db/{databaseName}/tx",
#   "bolt_direct" : "bolt://localhost:7687",
#   "neo4j_version" : "4.3.2",
#   "neo4j_edition" : "community"
# }

# run python script, passing posargs
python3 $PYSCRIPT $@
echo "Python script at ${PYSCRIPT} exited gracefully"