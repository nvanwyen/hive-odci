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
        echo                       | tee --append ${dir}/load-jdbc.${dts}.log
        echo "Loading jar: ${jar}" | tee --append ${dir}/load-jdbc.${dts}.log

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
        ) 2>&1 | tee --append ${dir}/load-jdbc.${dts}.log

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
