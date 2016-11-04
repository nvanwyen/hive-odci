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
