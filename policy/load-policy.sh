#!/bin/bash

if [[ -z ${@} ]] ; then

    echo "Usage $0 <connection>"
    exit 1

fi

dir=`dirname $0`

if [ -f ${dir}/local_policy.jar ] ; then

    if [ -f ${dir}/US_export_policy.jar ] ; then

        loadjava -order \
                 -verbose \
                 -resolve \
                 -recursivejars \
                 -resolver "((* hive) (* sys) (* public))" \
                 -user "${@}" \
                 -schema hive \
                 ${dir}/local_policy.jar \
                 ${dir}/US_export_policy.jar \
            2>&1 | tee --append ${dir}/loadjava.`date +%Y%m%d%H%M%S`.log

    else

        echo "Could not find ${dir}/US_export_policy.jar"
        exit 1

    fi

else

    echo "Could not find ${dir}/local_policy.jar"
    exit 1

fi

exit $?
