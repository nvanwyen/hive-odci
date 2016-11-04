#!/bin/bash

# Hive-ODCI - Copyright (C) 2006-2016 Metasystems Technologies Inc. (MTI)
# Nicholas Van Wyen
# 
# This library is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Lesser General Public License as published by 
# the Free Software Foundation; either version 2.1 of the License, or (at 
# your option) any later version.
# 
# This library is distributed in the hope that it will be useful, but 
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
# or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
# License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License 
# along with this library; if not, write to the
# 
#                 Free Software Foundation, Inc.
#                 59 Temple Place, Suite 330,
#                 Boston, MA 02111-1307 USA

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

            echo
            echo "Copied example, make sure to update paraemters"
            echo

        else

            echo "Warning: example [${src}/krb5.conf] not found!"
            echo

        fi

    else

        echo "${dir}/krb5.conf already exists!"
        echo

    fi

    #
    if [ ! -f ${dir}/jdbc.conf ] ; then

        if [ -f ${src}/jdbc.conf ] ; then

            cp "${src}/jdbc.conf" "${dir}/jdbc.conf"
            chkrc $? 1

            echo
            echo "Copied example, make sure to update paraemters"
            echo

        else

            echo "Warning: example [${src}/jdbc.conf] not found!"
            echo

        fi

    else

        echo "${dir}/jdbc.conf already exists!"
        echo

    fi

else

    echo "\$ORACLE_HOME not defined!"
    exit 1

fi
