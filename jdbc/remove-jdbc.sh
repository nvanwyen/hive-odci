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
