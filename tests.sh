#!/bin/bash

# Build Image
docker build -t airflow-config-tests:latest -f tests-Dockerfile .

# usage(){ printf "\n$0 usage:\n\n" && grep " .*)\ #" $0; exit 0;}

# if [ $# -eq 0 ];then
#     usage
# fi

# -k option ('keep') bind-mounts ./cumulus-geoproc-test-results on host 
#                    to /output in container for manually evaluating output files
# while getopts ":km:th" option; do
#     case ${option} in 
#         k)
#             VOLUMES="-v $PWD/cumulus-geoproc-test-results:/output"
#             ;;
#         m) # Test file; ./tests.sh -m tests/integration/abrfc-qpe-01h/test_abrfc_qpe_01h.py
#            # Test function: ./tests.sh -m tests/integration/abrfc-qpe-01h/test_abrfc_qpe_01h.py::test_func
#            # https://docs.pytest.org/en/latest/how-to/usage.html#specifying-which-tests-to-run
#             test_method="${OPTARG}"
#             ;;
#         t) # Run all tests under 'tests' directory with verbose and stdout enabled
#             test_method="-vs"
#             ;;
#         h) # Print this message
#             usage
#             exit 1
#             ;;
#     esac
# done

docker run --env-file tests/airflow.env --memory=1g airflow-config-tests:latest
