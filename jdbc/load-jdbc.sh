#!/bin/bash

if [[ -z ${@} ]] ; then

    echo "Usage $0 <connection>"
    exit 1

fi

dir=`dirname $0`

if [ -f ${dir}/hive.jar ] ; then

    loadjava -order \
             -verbose \
             -resolve \
             -recursivejars \
             -resolver "((* hive) (* sys) (* public))" \
             -user "${@}" \
             -schema hive \
             ${dir}/hive.jar \
        2>&1 | tee --append ${dir}/loadjava.`date +%Y%m%d%H%M%S`.log

else

    echo "Could not find ${dir}/hive.jar"
    exit 1

fi

exit $?
