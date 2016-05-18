--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - dbms_hive.pkb.sql
--

--
prompt ... running dbms_hive.pkb.sql

--
alter session set current_schema = hive;

--
create or replace package body dbms_hive as

    --
    function param( name in varchar2 ) return varchar2 is

        val varchar2( 4000 );

    begin

        --
        select a.value into val
          from param$ a
         where a.name = name;

        --
        return val;

        --
        exception
            when no_data_found then
                return null;

    end param;

    --
    procedure param( name in varchar2, value in varchar2 ) is

        pragma autonomous_transaction;

    begin

        --
        insert into param$ a ( a.name, a.value )
        values ( name, value );

        --
        commit;

        exception
            --
            when dup_val_on_index then
                --
                begin

                    --
                    update param$ a set a.value = value
                     where a.name = name;

                    --
                    commit;

                    --
                    exception
                        when others then rollback; raise;

                end;

            --
            when others then rollback; raise;

    end param;

    --
    procedure purge_log is

        pragma autonomous_transaction;

    begin

        --
        delete from log$;
        commit;

        exception
            when others then
                rollback;
                raise;

    end purge_log;

    --
    procedure purge_filter( key in varchar2 default null )

        pragma autonomous_transaction;

    begin

        --
        delete from filter$ a
         where 1 = case when key is null 
                        then 1 
                        else key = a.key 
                   end;
        commit;

        exception
            when others then
                rollback;
                raise;

    end purge_filter;

end dbms_hive;
/

show errors

--
-- ... done!
--
