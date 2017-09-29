--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - dbms_hive.pkb.sql
--

/*
  Hive-ODCI - Copyright (C) 2006-2016 Metasystems Technologies Inc. (MTI)
  Nicholas Van Wyen
  
  This library is free software; you can redistribute it and/or modify it 
  under the terms of the GNU Lesser General Public License as published by 
  the Free Software Foundation; either version 2.1 of the License, or (at 
  your option) any later version.
  
  This library is distributed in the hope that it will be useful, but 
  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
  License for more details.
  
  You should have received a copy of the GNU Lesser General Public License 
  along with this library; if not, write to the
  
                  Free Software Foundation, Inc.
                  59 Temple Place, Suite 330,
                  Boston, MA 02111-1307 USA
*/

--
prompt ... running dbms_hive.pkb.sql

--
alter session set current_schema = hive;

--
create or replace package body dbms_hive as

    --
    auth_grant      constant number :=   0;
    auth_revoke     constant number :=   1;

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
        n varchar2( 4000 ) := name;

    begin

        log_trc_( 'param( ' || name || ' ) called' );

        --
        select a.value into val
          from param$ a
         where a.name = n;

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
    procedure purge_log( usr in varchar2 default null ) is

        pragma autonomous_transaction;

    begin

        log_inf_( 'purge_log called, usr: ' || nvl( usr, '{null}' ) );

        if ( usr is not null ) then

            --
            delete from log$ where name = usr;
            log_wrn_( 'purge_log removed ' || to_char( sql%rowcount ) || ' row(s) for name: ' || usr );

        else

            --
            execute immediate 'truncate table log$ drop storage';
            log_wrn_( 'purge_log truncated log' );

        end if;

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

    --
    procedure move_ts( ts in varchar2, obj in varchar2 default null ) is

        tsz number := 0;
        osz number := 0;
        qsz number := 0;

        tab varchar2( 256 );

        unlimited_quota constant number := -1;

        --
        function ts_size_( t in varchar2 ) return number is

            c number := 0;

        begin

            --
            select a.bytes + b.bytes into c
              from ( select bytes
                       from dba_free_space
                      where tablespace_name = t ) a,
                   ( select bytes
                       from dba_data_files
                      where tablespace_name = t ) b;

            --
            return c;

            --
            exception
                when no_data_found then
                    return 0;

                when others then
                    raise;

        end ts_size_;

        --
        function tab_size_( o in varchar2 ) return number is

            c number := 0;

        begin


            select bytes into c
              from dba_segments
             where segment_name = o
               and owner = 'HIVE';

            --
            return c;

            --
            exception
                when no_data_found then
                    return 0;

                when others then
                    raise;

        end tab_size_;

        --
        function quota_( t in varchar2 ) return number is

            c number := 0;

        begin

            --
            select max_bytes into c
              from dba_ts_quotas
             where tablespace_name = t
               and username = 'HIVE';

            --
            return c;

            --
            exception
                when no_data_found then
                    return 0;

                when others then
                    raise;

        end quota_;

        --
        function tab_name_( o in varchar2 ) return varchar2 is

            n varchar2( 256 );

        begin

            --
            select table_name into n
              from dba_tables
             where owner = 'HIVE'
               and ( table_name = o or table_name = o || '$' );

            --
            return n;

            --
            exception
                when no_data_found then
                    return null;

                when others then
                    raise;

        end tab_name_;

        --
        procedure tab_move_( t in varchar2, o varchar2 ) is
        begin

            --
            execute immediate 'alter table hive.' || o || ' move tablespace ' || t;

        end tab_move_;

        --
        procedure idx_rebuild_( o varchar2 ) is
        begin

            --
            for rec in ( select index_name
                           from dba_indexes
                          where owner = 'HIVE'
                            and table_name = o ) loop

                --
                execute immediate 'alter index hive.' || rec.index_name || ' rebuild online';

            end loop;

        end idx_rebuild_;

    begin

        tsz := ts_size_( ts );

        if ( tsz > 0 ) then

            qsz := quota_( ts );

            if ( ( qsz > 0 ) or ( qsz = unlimited_quota ) ) then

                --
                if ( obj is null ) then

                    osz := osz + tab_size_( 'LOG$' );
                    osz := osz + tab_size_( 'PRIV$' );
                    osz := osz + tab_size_( 'FILTER$' );
                    osz := osz + tab_size_( 'PARAM$' );


                    if ( osz >= tsz ) then

                        raise_application_error( ec_space, es_space );

                    end if;

                    if ( qsz = unlimited_quota ) then

                        qsz := osz + 1;

                    end if;

                    if ( osz >= qsz ) then

                        raise_application_error( ec_quota, es_quota );

                    end if;

                    tab_move_( ts, 'LOG$' );
                    idx_rebuild_( 'LOG$' );

                    tab_move_( ts, 'PRIV$' );
                    idx_rebuild_( 'PRIV$' );

                    tab_move_( ts, 'FILTER$' );
                    idx_rebuild_( 'FILTER$' );

                    tab_move_( ts, 'PARAM$' );
                    idx_rebuild_( 'PARAM$' );

                else

                    tab := tab_name_( obj );

                    if ( tab is not null ) then

                        osz := tab_size_( tab );

                        if ( qsz = unlimited_quota ) then

                            qsz := osz + 1;

                        end if;

                        if ( osz = 0 ) then

                            raise_application_error( ec_segment, es_segment );

                        end if;

                        if ( osz >= tsz ) then

                            raise_application_error( ec_space, es_space );

                        end if;

                        if ( osz >= qsz ) then

                            raise_application_error( ec_quota, es_quota );

                        end if;

                        tab_move_( ts, tab );
                        idx_rebuild_( tab );

                    else

                        raise_application_error( ec_exists, es_exists );

                    end if;

                end if;

            else

                raise_application_error( ec_quota, es_quota );

            end if;

        else

            raise_application_error( ec_zero, es_zero );

        end if;

    end move_ts;

    --
    procedure grant_access( opr in varchar2,
                            tab in varchar2,
                            gnt in varchar2 ) is
    begin

        execute immediate 'begin impl.sql_priv( :p0, :p1, :p2, :p3 ); end;'
          using tab,
                gnt,
                opr,
                auth_grant;

    end grant_access;

    --
    procedure revoke_access( opr in varchar2,
                             tab in varchar2,
                             gnt in varchar2 ) is
    begin

        execute immediate 'begin impl.sql_priv( :p0, :p1, :p2, :p3 ); end;'
          using tab,
                gnt,
                opr,
                auth_revoke;

    end revoke_access;

end dbms_hive;
/

--
show errors

--
-- ... done!
--
