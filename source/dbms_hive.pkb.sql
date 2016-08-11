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
    procedure log_err_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_error( :p0 ); end;'
          using 'dbms_hive:' || txt;

        exception when others then null;

    end log_err_;

    --
    procedure log_wrn_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_warn( :p0 ); end;'
          using 'dbms_hive:' || txt;

        exception when others then null;

    end log_wrn_;

    --
    procedure log_inf_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_info( :p0 ); end;'
          using 'dbms_hive:' || txt;

        exception when others then null;

    end log_inf_;

    --
    procedure log_trc_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_trace( :p0 ); end;'
          using 'dbms_hive:' || txt;

        exception when others then null;

    end log_trc_;

    --
    function exist( name in varchar2 ) return boolean is

        c number := 0;
        n varchar2( 4000 ) := name;

    begin

        log_trc_( 'exist( ' || n || ' ) called' );

        --
        select count(0) into c
          from param$ a
         where a.name = n;

        --
        log_trc_( 'exist( ' || n || ' ) returns' || case when ( c > 0 ) then 'true' else 'false' end );
        return ( c > 0 );

        exception
            when others then
                log_err_( 'exist( ' || n || ' ) error: ' || sqlerrm );
                raise;

    end exist;

    --
    function param( name in varchar2 ) return varchar2 is

        val varchar2( 4000 );

    begin

        log_trc_( 'param( ' || name || ' ) called' );

        --
        select a.value into val
          from param$ a
         where a.name = name;

        --
        log_trc_( 'param( ' || name || ' ) returns: ' || nvl( val, '{null}' ) );
        return val;

        --
        exception
            when no_data_found then
                log_wrn_( 'param( ' || name || ' ) found no data, returning NULL' );
                return null;

            when others then
                log_err_( 'param( ' || name || ' ) error: ' || sqlerrm );
                raise;

    end param;

    --
    procedure param( name in varchar2, value in varchar2 ) is

        pragma autonomous_transaction;
        n varchar2( 4000 ) := name;
        v varchar2( 4000 ) := value;

    begin

        log_trc_( 'param( ' || n || ', ' || v || ' ) called' );

        --
        if ( not exist( name ) ) then

            --
            insert into param$ a ( a.name, a.value )
            values ( n, v );

            log_inf_( 'param inserted new item: ' || n );

        else

            --
            update param$ a
               set a.value = v
             where a.name = n;

            log_inf_( 'param updated existing item: ' || n );

        end if;

        --
        commit;

        exception
            when others then 
                rollback; 
                log_err_( 'param( ' || n || ', ' || v || ' ) error: ' || sqlerrm );
                raise;

    end param;

    --
    procedure remove( name in varchar2 ) is

        pragma autonomous_transaction;
        n varchar2( 4000 ) := name;

    begin

        log_trc_( 'remove( ' || n || ' ) called' );

        if ( exist( n ) ) then

            delete from param$ a
             where a.name = n;

            log_wrn_( 'removed deleted item: ' || n );

            commit;

        end if;

        exception
            when no_data_found then 
                log_wrn_( 'remove found no data for: ' || n );
                null;

            when others then 
                rollback; 
                log_err_( 'remove( ' || n || ' ) error: ' || sqlerrm );
                raise;

    end remove;

    --
    procedure purge_log is

        pragma autonomous_transaction;

    begin

        log_inf_( 'purge_log called' );

        --
        delete from log$;

        log_wrn_( 'purge_log removed ' || to_char( sql%rowcount ) || ' row(s)' );

        commit;

        exception
            when others then
                rollback;
                log_err_( 'purge_log error: ' || sqlerrm );
                raise;

    end purge_log;

    --
    procedure purge_filter( key in varchar2 default null ) is

        pragma autonomous_transaction;

    begin

        log_wrn_( 'purge_filter( ' || nvl( key, '{null}' ) || ' ) called' );

        if ( key is null ) then

            --
            delete from filter$;

            log_wrn_( 'purge_filter removed all ' || to_char( sql%rowcount ) || ' row(s)' );

        else

            --
            delete from filter$ a
             where key = a.key;

            log_wrn_( 'purge_filter removed ' || to_char( sql%rowcount ) || ' row(s) for: ' || key );

        end if;

        commit;

        exception
            when others then
                rollback;
                log_err_( 'purge_filter( ' || nvl( key, '{null}' ) || ' ) error: ' || sqlerrm );
                raise;

    end purge_filter;

end dbms_hive;
/

show errors

--
-- ... done!
--
