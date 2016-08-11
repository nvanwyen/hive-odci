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
    function exist( name in varchar2 ) return boolean is

        c number := 0;
        n varchar2( 4000 ) := name;

    begin

        --
        select count(0) into c
          from param$ a
         where a.name = n;

        --
        return ( c > 0 );

    end exist;

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
        n varchar2( 4000 ) := name;
        v varchar2( 4000 ) := value;

    begin

        --
        if ( not exist( name ) ) then

            --
            insert into param$ a ( a.name, a.value )
            values ( n, v );

        else

            --
            update param$ a
               set a.value = v
             where a.name = n;

        end if;

        --
        commit;

        exception
            when others then rollback; raise;

    end param;

    --
    procedure remove( name in varchar2 ) is

        pragma autonomous_transaction;
        n varchar2( 4000 ) := name;

    begin

        if ( exist( n ) ) then

            delete from param$ a
             where a.name = n;

            commit;

        end if;

        exception
            when no_data_found then null;
            when others then rollback; raise;

    end remove;

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
    procedure purge_filter( key in varchar2 default null ) is

        pragma autonomous_transaction;

    begin

        if ( key is null ) then

            --
            delete from filter$;

        else

            --
            delete from filter$ a
             where key = a.key;

        end if;

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
