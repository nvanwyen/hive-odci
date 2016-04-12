#!/bin/bash

#
dir=`dirname ${0}`
sqlplus=`which sqlplus`

#
jars="hive-jdbc-1.0.1-standalone.jar hadoop-core-1.2.1.jar"

#
if [ ! -f $sqlplus ] ; then

    echo "SQL*Plus utility [sqlplus] cannot be found"
    exit 1

fi

function load_jar()
{
    local jar=${1}

    if [ -f ${jar} ] ; then

        ${sqlplus} -S "/ as sysdba" << !

            --
            whenever sqlerror exit sql.sqlcode;

            --
            exec dbms_java.loadjava('-v -schema hive ${jar}' );

            --
            set serveroutput on
            declare

                c number := 0;

            begin

                select count(0) into c
                  from dba_tables
                 where owner = 'HIVE'
                   and table_name = 'CREATE\$JAVA\$LOB\$TABLE';

                if ( c > 0 ) then

                    execute immediate 'select count(0) from hive.create\$java\$lob\$table'
                       into c;

                    if ( c = 0 ) then

                        execute immediate 'drop table hive.create\$java\$lob\$table purge';

                    else

                        dbms_output.put_line( 'Refused to drop load table [CREATEi\$JAVAi\$LOB\$TABLE], with existing data' );

                    end if;

                else

                    dbms_output.put_line( 'Load table [CREATE\$JAVA\$LOB\$TABLE] does not exist' );

                end if;

            end;
            /
!
        rc=$?

        if [ $rc -ne 0 ] ; then

            echo "Failed to clean up Load Table, may need manual intervention"
            exit $?
        fi

    else

        echo "Java JAR [${jar}] not found!"
        exit 1

    fi
}

#
for jar in ${jars} ; do 

    #
    load_jar ${dir}/jdbc/${jar}

done

# loadjava -verbose -force -resolve -user "sys/Password99 as sysdba" -schema hive hive-jdbc-1.0.1-standalone.jar 2>&1 | tee hive-jdbc.log
# loadjava -verbose -force -resolve -user "sys/Password99 as sysdba" -schema hive hadoop-core-1.2.1.jar          2>&1 | tee hadoop-core.log

exit $?
