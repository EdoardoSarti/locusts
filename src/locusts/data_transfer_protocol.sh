#!/bin/bash

LOCAL_PATH==${1}
HPC_PATH=${2}
rsync -a ${PATH} ${HPC_PATH}
