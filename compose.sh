#!/bin/bash
# Usage using comment after ')' for each argument
usage(){ echo "$0 usage:" && grep " .)\ #" $0; exit 0;}
dc="docker compose"
m="" # MinIO compose file
l="" # LocalStack compose file
# getopts
while getopts "bdmshlu" arg; do
    case $arg in
        s) # Stop Docker
            s="stop"
            ;;
        d) # Down Docker
            d="down"
            ;;
        m) # Add minio '-f docker-compose.yml -f docker-compose.minio.yml'
            m="-f docker-compose.yml -f docker-compose.minio.yml"
            ;;
        l) # Add localstack '-f docker-compose.yml -f docker-compose.localstack.yml'
            l="-f docker-compose.yml -f docker-compose.localstack.yml"
            ;;
        u) # Docker compose up
            u="up"
            ;;
        b) # Start Docker with --build flag
            b="--build"
            ;;
        h | *) # Display help
            usage
            exit 0
            ;;
    esac
done

# Combine compose file options
compose_files="-f docker-compose.yml"
[ ! -z "$m" ] && compose_files+=" -f docker-compose.minio.yml"
[ ! -z "$l" ] && compose_files+=" -f docker-compose.localstack.yml"

# Construct command
if [[ ! -z ${s} ]]
then
    cmd="$dc $compose_files $s"
elif [[ ! -z ${d} ]]
then
    cmd="$dc $compose_files $d"
elif [[ ! -z ${b} ]]
then
    cmd="$dc $compose_files up $b"
else
    cmd="$dc $compose_files up"
fi

[ $# -eq 0 ] && cmd="$dc up"

echo "$cmd"
eval "$cmd"