--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.pkb.sql
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
prompt ... running hive.pkb.sql

--
alter session set current_schema = hive;

--
create or replace package body remote as

    --
    procedure log_err_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_error( :p0 ); end;'
          using 'remote:' || txt;

        exception when others then null;

    end log_err_;

    --
    procedure log_wrn_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_warn( :p0 ); end;'
          using 'remote:' || txt;

        exception when others then null;

    end log_wrn_;

    --
    procedure log_inf_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_info( :p0 ); end;'
          using 'remote:' || txt;

        exception when others then null;

    end log_inf_;

    --
    procedure log_trc_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_trace( :p0 ); end;'
          using 'remote:' || txt;

        exception when others then null;

    end log_trc_;

    --
    function param_( n in varchar2 ) return varchar2 is

        v varchar2( 4000 );

    begin

        log_trc_( 'param_( ' || n || ' ) called' );

        v := session_param( n );

        if ( v is null ) then

            log_trc_( 'param_ ' || n || ': not found at session level' );

            --
            select a.value into v
              from param$ a
             where a.name = n;

        else

            log_trc_( 'param_ ' || n || ': found at session level' );

        end if;

        --
        log_trc_( 'param_( ' || n || ' ) returns: ' || v );
        return v;

        --
        exception
            when no_data_found then
                log_err_( 'param_( ' || n || ' ) no data found' );
                return null;

            when others then
                log_err_( 'param_, error: ' || sqlerrm );
                raise;

    end param_;

    --
    function session_param( name in varchar2 ) return varchar2 is
    begin

        log_trc_( 'session_param( ' || name || ' ) called' );
        return impl.session_param( name );

        exception
            when others then
                log_err_( 'session_param( ' || name || ' ) error: ' || sqlerrm );
                raise;

    end session_param;

    --
    procedure session_param( name  in varchar2,
                             value in varchar2 ) is
    begin

        log_trc_( 'session_param( ' || name || ', ' || value || ' ) called' );
        impl.session_param( name, value );

        exception
            when others then
                log_err_( 'session_param( ' || name || ', ' || value || ' ) error: ' || sqlerrm );
                raise;

    end session_param;

    --
    procedure session_log_level( typ in number ) is
    begin

        log_trc_( 'session_log_level( ' || to_char( typ ) || ' ) called' );
        impl.session_log_level( typ );

    end session_log_level;

    --
    procedure session_clear is
    begin

        log_trc_( 'session clear' );
        impl.session_clear;

    end session_clear;

    --
    procedure session( url in varchar2 ) is
    begin

        log_trc_( 'session set: ' || url );
        impl.session( url );

        exception
            when others then
                log_err_( 'session( ' || url || ' ) error: ' || sqlerrm );
                raise;

    end session;

    --
    procedure session( usr in varchar2,
                       pwd in varchar2 ) is
    begin

        log_trc_( 'session set: ' || usr || ', ' || pwd );
        impl.session( usr, pwd );

        exception
            when others then
                log_err_( 'session( ' || usr || ', ' || pwd || ' ) error: ' || sqlerrm );
                raise;

    end session;

    -- 
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2 ) is
    begin
    
        log_trc_( 'session set: ' || url || ', ' || usr || ', ' || pwd );
        impl.session( url, usr, pwd );
     
        exception
            when others then
                log_err_( 'session( ' || url || ', ' || usr || ', ' || pwd || ' ) error: ' || sqlerrm );
                raise;
     
    end session;

    --
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2,
                       ath in varchar2 ) is
    begin       
     
        log_trc_( 'session set: ' || url || ', ' || usr || ', ' || pwd || ', ' || ath );
        impl.session( url, usr, pwd, ath );
     
        exception
            when others then
                log_err_( 'session( ' || url || ', ' || usr || ', ' || pwd || ', ' || ath || ' ) error: ' || sqlerrm );
                raise;
        
    end session;

    --
    procedure session( con in connection ) is
    begin

        log_trc_( 'session set: <connection>' );
        impl.session( con );

        exception
            when others then
                log_err_( 'session( <connection> ) error: ' || sqlerrm );
                raise;

    end session;

    --
    function session return connection is

        con connection;

    begin

        con := impl.session;

        if ( con is not null ) then

            con.pass := null;

        end if;

        log_trc_( 'connection returns: <con>' );
        return con;

        exception
            when others then
                log_err_( 'connection error: ' || sqlerrm );
                raise;

    end session;

    --
    procedure dml( stm in varchar2,
                   bnd in binds      default null,
                   con in connection default null ) is
    begin

        log_trc_( 'dml( ' || stm || ', <bnd>, <con> ) called' );
        impl.sql_dml( stm, bnd, con );

        exception
            when others then
                log_err_( 'dml: ' || stm || ', error: ' || sqlerrm );
                raise;

    end dml;

    --
    procedure ddl( stm in varchar2,
                   con in connection default null ) is
    begin

        log_trc_( 'ddl( ' || stm || ', <con> ) called' );
        impl.sql_ddl( stm, con );

        exception
            when others then
                log_err_( 'ddl: ' || stm || ', error: ' || sqlerrm );
                raise;

    end ddl;

end remote;
/

--
show errors

--
-- ... done!
--
