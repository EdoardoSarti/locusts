#!/bin/bash

PATH_FROM=${1}
PATH_TO=${2}

IFS=":" read -a ARR_FROM <<< ${PATH_FROM}
if [[ "${#ARR_FROM[@]}" == "2" ]]
then
    PATH_FROM="helix.nih.gov:${ARR_FROM[1]}"
fi

IFS=":" read -a ARR_TO <<< ${PATH_TO}
if [[ "${#ARR_TO[@]}" == "2" ]]
then
    PATH_TO="helix.nih.gov:${ARR_TO[1]}"
fi

#echo "rsync -a ${PATH_FROM} ${PATH_TO}" >> baf

rsync -a ${PATH_FROM} ${PATH_TO}
