#!/bin/bash

src=`dirname $0`

function chkrc()
{
    local rc=$1
    local ex=$2

    if [[ -z ${ex} ]] ; then

        ex=0

    fi

    if [ ${rc} -ne 0 ] ; then

        echo "Falied"

        if [ ${ex} -gt 0 ] ; then

            exit ${ex}

        fi

    fi
}

if [[ ! -z $ORACLE_HOME ]] ; then

    # example location
    dir=$ORACLE_HOME/krb

    if [ ! -d ${dir} ] ; then

        mkdir -p "${dir}"
        chkrc $? 1

    fi

    #
    if [ ! -f ${dir}/krb5.conf ] ; then

        if [ -f ${src}/krb5.conf ] ; then

            cp "${src}/krb5.conf" "${dir}/krb5.conf"
            chkrc $? 1

            echo "Copied example, make sure to update paraemters"
            echo "update hive.param$ set value = '${dir}/krb/krb5.conf' where name = 'java.security.krb5.conf';"

        else

            echo "Warning: example [${src}/krb5.conf] not found!"

        fi

    else

        echo "${dir}/krb5.conf already exists!"

    fi

    #
    if [ ! -f ${dir}/jdbc.conf ] ; then

        if [ -f ${src}/jdbc.conf ] ; then

            cp "${src}/jdbc.conf" "${dir}/krb5.conf"
            chkrc $? 1

            echo "Copied example, make sure to update paraemters"
            echo "update hive.param$ set value = '${dir}/krb/jdbc.conf' where name = 'java.security.auth.login.config';"

        else

            echo "Warning: example [${src}/jdbc.conf] not found!"

        fi

    else

        echo "${dir}/jdbc.conf already exists!"

    fi

else

    echo "\$ORACLE_HOME not defined!"
    exit 1

fi
