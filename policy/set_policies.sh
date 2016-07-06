#!/bin/bash

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
