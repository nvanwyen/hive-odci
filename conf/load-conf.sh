#!/bin/bash

# Copyright (c) 2016, Metasystems Technologies Inc (MTI), Nicholas Van Wyen
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without 
# modification, are permitted provided that the following conditions are met:
# 
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 
# 3. Neither the name of the copyright holder nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
            echo "update hive.param$ set value = '${dir}/krb5.conf' where name = 'java.security.krb5.conf';"
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
            echo "update hive.param$ set value = '${dir}/jdbc.conf' where name = 'java.security.auth.login.config';"
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
