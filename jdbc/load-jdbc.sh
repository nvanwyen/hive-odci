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

#
if [[ -z ${@} ]] ; then

    echo "Usage $0 <connection>"
    exit 1

fi

#
dts=`date +%Y%m%d%H%M%S`
dir=`dirname $0`

#
if [ `ls ${dir}/*.jar | wc -l` -gt 0 ] ; then

    #
    u=$1
    p=$2

    #
    if [[ -z ${u} ]] ; then

        #
        echo -n "User [sys]: "
        read u

        #
        if [[ -z ${u} ]] ; then

            u="SYS"

        fi

    fi

    #
    if [[ -z ${u} ]] ; then

        #
        echo "User must be supplied!"
        exit 1

    fi

    #
    if [[ -z ${p} ]] ; then

        #
        echo -n "Password [none]: "
        read p

        #
        if [[ -z ${p} ]] ; then

            p="sys"

        fi

    fi

    #
    if [[ -z ${p} ]] ; then

        #
        echo "Password must be supplied!"
        exit 1

    fi

    #
    st="`date +%Y-%b-%d` `date +%H:%M:%S`" ; st=${st^^}

    #
    for jar in `ls ${dir}/*.jar` ; do

        #
        echo                                     | tee --append ${dir}/load-jdbc.${dts}.log
        echo "Loading jar: ${jar}"               | tee --append ${dir}/load-jdbc.${dts}.log
        echo "---------------------------------" | tee --append ${dir}/load-jdbc.${dts}.log

        #
        (
            loadjava -force \
                     -genmissing \
                     -order \
                     -verbose \
                     -resolve \
                     -recursivejars \
                     -resolver "((* hive) (* sys) (* public))" \
                     -user "${u}" \
                     -schema hive \
                     ${jar} << !
${p}

!
        ) 2>&1 | tee --append ${dir}/load-jdbc.${dts}.log \
               | egrep "^Classes|^Resources|^Sources|^Published|^Classes|^Classes|^Synonyms|^Errors"


    done ;

    #
    if [ ${u,,} = "sys" ] ; then

        #
        sqlplus -S "/ as sysdba" << !
            --
            set linesize 160
            set pagesize 50000

            --
            col line for 9999990
            col name for a40 word_wrap
            col text for a75 word_wrap

            --
            -- note: do not show "notes", "verification warnings", 
            --       or "known classes from hive-jdbc-thin-1.1.0.jar
            --
            select dbms_java.longname( name ) name, 
                   text
              from dba_errors
             where owner = 'HIVE'
               and text not like 'Note: %'
               and text not like 'ORA-29552: verification warning: %'
               and dbms_java.longname( name ) 
                   not in ( 'org/apache/hadoop/hive/shims/Jetty23Shims',
                            'org/apache/hadoop/hive/shims/Jetty23Shims\$1',
                            'org/apache/hadoop/hive/shims/Jetty23Shims\$Server',
                            'org/jets3t/service/impl/soap/axis/SoapS3Service',
                            'org/jets3t/service/impl/soap/axis/_2006_03_01/AmazonS3SoapBindingStub',
                            'org/jets3t/service/impl/soap/axis/_2006_03_01/AmazonS3_ServiceLocator' )
             order by name, line;
!
    fi

    #
    et="`date +%Y-%b-%d` `date +%H:%M:%S`" ; et=${et^^}

    #
    echo                | tee --append ${dir}/load-jdbc.${dts}.log
    echo "Start: ${st}" | tee --append ${dir}/load-jdbc.${dts}.log
    echo "Ended: ${et}" | tee --append ${dir}/load-jdbc.${dts}.log
    echo                | tee --append ${dir}/load-jdbc.${dts}.log

else

    #
    echo "No Jar files to load found!"
    exit 1

fi

#
exit $?
