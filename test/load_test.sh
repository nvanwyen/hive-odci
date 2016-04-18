#!/bin/bash

if [[ -z ${@} ]] ; then

    echo "Usage $0 <connection>"
    exit 1

fi

loadjava -order \
         -verbose \
         -resolve \
         -recursivejars \
         -resolver "((* hive) (* sys) (* public))" \
         -user "${@}" \
         -schema hive \
         hive.jar \
    2>&1 | tee --append loadjava.`date +%Y%m%d%H%M%S`.log

exit $?
