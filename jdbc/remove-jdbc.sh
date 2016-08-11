#!/bin/bash

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
