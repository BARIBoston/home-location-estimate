# Run main.py in parallel. This wrapper script is necessary because turicreate
# - the clustering library that we use for DBSCAN - does not play well with
# Python's multiprocessing library. Thus, we need to do job management outside
# of Python.
#
# Usage: ./main-mp.sh [-j JOBS] user-ids.txt database.db [extra.db [...]]
#
#     e.g. ./main-mp.sh -j 10 input/ma-users.txt input/geotweets-ma-dbscan.db
#                       ^^^^^
#                       10 threads
#
#  Pipes also work: cat input/ma-users.txt | shuf -n 10 |. ./main-mp.sh ...
#

jobs=4 # default number of jobs
AGGREGATES_OUTPUT="aggregates/"

while getopts "j:" opt; do
    case "$opt" in
    j)  jobs=$OPTARG
        ;;
    esac
done
shift $((OPTIND-1))

user_ids_file="$1"
shift 1
databases="$@"

process_user() {
    user=$1
    shift 1
    output_file="${AGGREGATES_OUTPUT}/${user}.csv"

    if ! [ -f "$output_file" ]; then
        echo starting $user
        ./main.py -i ${user} -o "${output_file}" "$@"
    else
        echo skipping $user
    fi
}

export -f process_user
export AGGREGATES_OUTPUT
mkdir -p ${AGGREGATES_OUTPUT}

cat "$user_ids_file" | xargs -n 1 -P ${jobs} -I {} bash -c "process_user {} \"$databases\""
