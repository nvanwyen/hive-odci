--------------------------------------------------------------------------------
--
-- 2017-09-16, NV - drop_java.sql
--
 
--
prompt ... running drop_java.sql
 
--
set serveroutput on
 
prompt ... dropping HIVE Java Objects ...
 
--
begin
 
    for rec in ( select dbms_java.longname( object_name ) object_name,
                        object_type
                   from dba_objects
                  where owner = 'HIVE'
                    and object_type in ( 'JAVA CLASS',
                                         'JAVA RESOURCE' ) ) loop
 
        begin
 
            if ( rec.object_type = 'JAVA CLASS' ) then
 
                execute immediate 'drop java class hive."' || rec.object_name || '"';
                dbms_output.put_line( 'hive."' || rec.object_name
                                   || '" class dropped' );
 
            elsif ( rec.object_type = 'JAVA RESOURCE' ) then
 
                execute immediate 'drop java resource hive."' || rec.object_name || '"';
                dbms_output.put_line( 'hive."' || rec.object_name
                                   || '" resource dropped' );
 
            else
 
                dbms_output.put_line( 'hive."' || rec.object_name
                                   || '" not a class or resource!' );
 
            end if;
 
            exception when others then
                dbms_output.put_line( 'hive."' || rec.object_name
                                   || '" drop error: ' || sqlerrm );
 
        end;
 
    end loop;
 
end;
/
 
prompt ... done!
 
exit
 
--
-- ... done
--
