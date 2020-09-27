../helper_scripts/be_polite.sh

ARG=${1}

OUT_A="../other_files_1/class_A/expected_output_A/"
OUT_B="../other_files_1/class_B/expected_output_B/"

for fname in `ls ../important_files/*.txt`
do
    sleep 1
    isA=`grep "class A" ${fname}`
    isB=`grep "class B" ${fname}`
    if [[ "${isA}" != "" ]]
    then
        cp ${fname} ${OUT_A}
    elif [[ "${isB}" != "" ]]
    then
        cp ${fname} ${OUT_B}
    else
        continue
    fi
done

NEWSUBDIR="${OUT_A}/logs"
echo "The run with argument ${ARG} has been completed" > ${NEWSUBDIR}/log_${ARG}.txt
