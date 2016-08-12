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

echo=`which echo`
sed=`which sed`
sqlplus=`which sqlplus`

#
dir=`dirname $0`

#
function class_name()
{
    local cn=$1
    ${echo} "$(${echo} ${cn} | ${sed} 's/\.class//g')"
}

#
function drop_java()
{
    local jn=$1
    local rc=0

    ${sqlplus} -S "/ as sysdba" << !
        --
        whenever sqlerror exit sql.sqlcode
        --
        set feedback off
        set serveroutput on
        --
        declare

            typ varchar2( 4000 );

        begin

            select lower( object_type ) into typ
              from dba_objects
             where owner = 'HIVE'
               and dbms_java.longname( object_name ) = '${jn}';


            if ( typ is not null ) then

                execute immediate 'drop ' || typ || ' hive."${jn}"';

            end if;

            exception
                when no_data_found then
                    dbms_output.put_line( 'not found' );

                when others then
                    dbms_output.put_line( sqlerrm );
                    raise;

        end;
        /
!
    rc=$?

    if [ ${rc} -eq 0 ] ; then

        ${echo} "OK"

    fi
}

#
function show_java()
{
    #
    ${sqlplus} -S "/ as sysdba" << !
        --
        set linesize 160
        set pagesize 50000
        --
        col type for a16 word_wrap
        col name for a40 word_wrap

        select object_type type,
               dbms_java.longname( object_name ) name
          from dba_objects
         where owner = 'HIVE'
           and object_type like 'JAVA%'
         order by name, type;
!
}

#
if [ `ls ${dir}/*.jar | wc -l` -gt 0 ] ; then

    #
    for jar in `ls ${dir}/*.jar` ; do

        ${echo} "Processing jar: ${jar}"

        for cls in `jar tf ${jar}` ; do

            cn=$(class_name ${cls})

            echo -n "Processing: ${cn} ... "
            drop_java ${cn}

        done ;

    done ;

    #
    show_java

else

    #
    ${echo} "No Jar files to load found!"
    exit 1

fi

#
exit $?
