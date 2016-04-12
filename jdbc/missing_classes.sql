set linesize 160
set pagesize 50000
set trimspool on

set serveroutput on

spool missing_classes.log

/* *** example thread, how to find why a class cannot be resolved

    SQL> col name for a80
    SQL> select dbms_java.longname( name ) name
       2   from dba_errors 
       3  where owner = 'HIVE' 
       4    and dbms_java.longname( name ) like '%HiveDriver%';

    NAME
    -------------------------------
    org/apache/hive/jdbc/HiveDriver

    //
    SQL> alter java class hive."org/apache/hive/jdbc/HiveDriver" resolve;

    //
    SQL> col text for a80
    SQL> select text
       2   from dba_errors
       3 where owner = 'HIVE'
       4   and dbms_java.longname( name ) like 'org/apache/hive/jdbc/HiveDriver'

    TEXT
    -------------------------------------------------------------------------------------------
    ORA-29534: referenced object HIVE.org/apache/hive/jdbc/HiveConnection could not be resolved

    //
    SQL> alter java class hive."org/apache/hive/jdbc/HiveConnection" resolve;

    //
    SQL> select text
       2   from dba_errors
       3 where owner = 'HIVE'
       4   and dbms_java.longname( name ) like 'org/apache/hive/jdbc/HiveConnection';

    TEXT
    ------------------------------------------------------------------------------------------------------------------------
    ORA-29534: referenced object HIVE.org/apache/hive/service/cli/thrift/EmbeddedThriftBinaryCLIService could not be resolved

    //
    SQL> alter java class hive."org/apache/hive/service/cli/thrift/EmbeddedThriftBinaryCLIService" resolve;

    //
    SQL> select text
       2   from dba_errors
       3 where owner = 'HIVE'
       4   and dbms_java.longname( name ) like 'org/apache/hive/service/cli/thrift/EmbeddedThriftBinaryCLIService';

    TEXT
    ------------------------------------------------------------------------------------------------------------------------
    ORA-29534: referenced object HIVE.org/apache/hive/service/cli/CLIService could not be resolved

    //
    SQL> alter java class hive."org/apache/hive/service/cli/CLIService" resolve;

    //
    SQL> select text
       2   from dba_errors
       3 where owner = 'HIVE'
       4   and dbms_java.longname( name ) like 'org/apache/hive/service/cli/CLIService';

    TEXT
    ------------------------------------------------------------------------------------------------------------------------
    ORA-29521: referenced name org/apache/hadoop/hive/ql/metadata/HiveException could not be found
    ORA-29521: referenced name org/apache/hadoop/hive/ql/session/SessionState could not be found
    ORA-29521: referenced name org/apache/hadoop/hive/ql/metadata/HiveException could not be found
    ORA-29521: referenced name org/apache/hadoop/hive/ql/metadata/HiveException could not be found
    ORA-29521: referenced name org/apache/hadoop/hive/ql/exec/FunctionRegistry could not be found
    ORA-29521: referenced name org/apache/hadoop/hive/ql/metadata/Hive could not be found

    // !!! These are the missing class files for the HiveDiver to be resolved !!!

*** */

-- find all classes "not found" in the loaded jar files

/* *** the hard way ***
declare

   dml varchar2( 4000 );
   err varchar2( 4000 );

begin

    for rec in ( select dbms_java.longname( name ) name,
                        text
                   from dba_errors 
                  where owner = 'HIVE' 
                    and type = 'JAVA CLASS' 
                  order by name,
                           sequence ) loop

        if ( text like '% could not be resolved' ) then

            dml := 'alter java class hive."' || rec.name || '" resolve';
            execute immediate dml;

            for err in ( select text
                           from dba_errors
                          where owner = 'HIVE'
                            and dbms_java.longname( name ) = rec.name ) loop

                if ( lower( err.text ) like 'ORA-29521:% not found%' ) then

                    dbms_output.put_line( replace( replace( err.text, 
                                                            'ORA-29521: referenced name', '' ),
                                                   ' could not be found', '' ) );

                end if;

            end loop

        end if;

    end loop;

end;
/
*** */

/* *** the easy way *** */

    col class for a80

    select distinct
           replace( replace( text,
                             'ORA-29521: referenced name', '' ),
                    ' could not be found', '' ) class
      from dba_errors
     where owner = 'HIVE'
       and type = 'JAVA CLASS'
       and text like '%could not be found%'
       and text not like '%$%'
     order by class asc;

/* *** */

exit
