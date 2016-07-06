--------------------------------------------------------------------------------
--
-- 2016-04-13, NV - complile_resolve.sql
--

-- resolve the HIVE schema java classes

set linesize 160
set pagesize 50000

set serveroutput on

prompt ... Please be patient, this may take awhile!
prompt ... running 2 pass java class resolve on all objects currently "not resolved"

set timing on
begin

    -- runs twice ...
    begin

        for rec in ( select name,
                            text
                       from all_errors
                      where owner = 'HIVE'
                        and text like '% could not be resolved' ) loop


            begin

                execute immediate 'alter java class hive."' || rec.name || '" resolve';

                exception
                    when others then
                        null;   -- do not report until second pass is issued
            end;

        end loop;

    end;

    -- ... and again
    begin

        for rec in ( select name,
                            text
                       from all_errors
                      where owner = 'HIVE'
                        and text like '% could not be resolved' ) loop


            begin

                execute immediate 'alter java class hive."' || rec.name || '" resolve';
                --dbms_output.put_line( 'Resolved class: ' || rec.name );

                exception
                    when others then
                        dbms_output.put_line( 'Failed final resolve [' || rec.name || ']: ' || sqlerrm );
            end;

        end loop;

    end;

end;
/

set timing off
col name for a30 head "name"
col text for a80 head "text" word_wrap

select name,
       text
  from all_errors
 where owner = 'HIVE';

--
-- ... done!
--
