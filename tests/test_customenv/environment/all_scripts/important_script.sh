bash helper_scripts/be_polite.sh

ARG=${1}

OUT_A="../other_files_1/class_A/expected_output_A/"
OUT_B="../other_files_1/class_B/expected_output_B/"

for fname in `ls ../important_files/*.txt`
do
#    sleep 1
    bfname=`basename ${fname}`
    isA=`grep "class A" ${fname}`
    isB=`grep "class B" ${fname}`
    if [[ "${isA}" != "" && ! -s "${OUT_A}/${bfname}" ]]
    then
        cp ${fname} ${OUT_A}
    elif [[ "${isB}" != ""  && ! -s "${OUT_B}/${bfname}" ]]
    then
        cp ${fname} ${OUT_B}
    else
        continue
    fi
done

echo "HURRAY!" >> as_useless_3.sh 
echo "HEYHEY!" >> helper_scripts/be_polite.sh

NEWSUBDIR="${OUT_A}/logs"
if [[ ! -d "${NEWSUBDIR}" ]]
then
    mkdir ${NEWSUBDIR}
fi

echo "The run with argument ${ARG} has been completed" > ${NEWSUBDIR}/log_${ARG}.txt
