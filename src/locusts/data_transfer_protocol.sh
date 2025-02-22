#!/bin/bash

LOCAL_PATH=${1}
HPC_PATH=${2}
>&2 echo "rsync -a ${LOCAL_PATH} ${HPC_PATH}"
rsync -a --no-perms --no-g --chmod=ugo=rwX ${LOCAL_PATH} ${HPC_PATH}
