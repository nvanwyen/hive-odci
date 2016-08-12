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

dts=`date +%Y%m%d%H%M%S`
base="."

function update_jdk_policy()
{
    local jdk=$1
    local lsha=`sha1sum ${base}/local_policy.jar | awk '{print $1}'`
    local xsha=`sha1sum ${base}/US_export_policy.jar | awk '{print $1}'`

    if [ -f $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/local_policy.jar ] ; then

        osha=`sha1sum $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/local_policy.jar | awk '{print $1}'`

        if [ ${lsha} = ${osha} ] ; then

            echo "Policy up-to-date: $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/local_policy.jar"

        else

            echo "Backing up policy: \$ORACLE_HOME/javavm/jdk/${jdk}/lib/security/local_policy.jar"
            cp $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/local_policy.jar $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/loca                                                                                                                 l_policy.jar.${dts}

            echo "Updating policy: \$ORACLE_HOME/javavm/jdk/${jdk}/lib/security/local_policy.jar"
            cp ${base}/local_policy.jar $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/local_policy.jar

        fi

    else

        echo "Policy not found: \$ORACLE_HOME/javavm/jdk/${jdk}/lib/security/local_policy.jar"
        exit 1

    fi

    if [ -f $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/US_export_policy.jar ] ; then

        osha=`sha1sum $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/US_export_policy.jar | awk '{print $1}'`

        if [ ${xsha} = ${osha} ] ; then

            echo "Policy up-to-date: $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/US_export_policy.jar"

        else

            echo "Backing up policy: \$ORACLE_HOME/javavm/jdk/${jdk}/lib/security/US_export_policy.jar"
            cp $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/US_export_policy.jar $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/                                                                                                                 US_export_policy.jar${dts}

            echo "Updating policy: \$ORACLE_HOME/javavm/jdk/${jdk}/lib/security/US_export_policy.jar"
            cp ${base}/US_export_policy.jar $ORACLE_HOME/javavm/jdk/${jdk}/lib/security/US_export_policy.jar

        fi

    else

        echo "Policy not found: \$ORACLE_HOME/javavm/jdk/${jdk}/lib/security/US_export_policy.jar"
        exit 1

    fi
}

if [[ -z $ORACLE_HOME ]] ; then

    echo "\$ORACLE_HOME not set!"
    exit 1

fi

if [[ ! -z ${base} ]] ; then

    if [ -d ${base} ] ; then

        if [ ! -f ${base}/local_policy.jar ] ; then

            echo "Cannot find ${base}/local_policy.jar"
            exit 1

        fi

        if [ ! -f ${base}/US_export_policy.jar ] ; then

            echo "Cannot find ${base}/US_export_policy.jar"
            exit 1

        fi

        update_jdk_policy jdk6
        update_jdk_policy jdk7

    fi
 
fi

exit $?
