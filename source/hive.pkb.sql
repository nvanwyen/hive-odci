--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.pkb.sql
--

--
prompt ... running hive.pkb.sql

--
alter session set current_schema = hive;

--
create or replace package body remote as

    --
    ctx constant varchar2( 7 ) := 'hivectx';

    --
    function param_( n in varchar2 ) return varchar2 is

        v varchar2( 4000 );

    begin

        v := session_param( n );

        if ( v is null ) then

            --
            select a.value into v
              from param$ a
             where a.name = n;

        end if;

        --
        return v;

        --
        exception
            when no_data_found then
                return null;

    end param_;

    --
    function session_param( name  in varchar2 ) return varchar2 is
    begin

        return sys_context( ctx, substr( name, 1, 30 ), 4000 );

    end session_param;

    --
    procedure session_param( name  in varchar2,
                             value in varchar2 ) is
    begin

        if ( name in ( 'hive_host',
                       'hive_port',
                       'hive_user',
                       'hive_pass',
                       'log_level' ) ) then

            dbms_session.set_context( ctx, substr( name, 1, 30 ), value );

        else

            raise_application_error( -20001, 'Paramter [' || name || '] is not eligible for change at the session level' );

        end if;

    end session_param;

    -- 
    procedure session( usr in varchar2,
                       pwd in varchar2 ) is
    begin

        impl.session( usr, pwd );

    end session;

    -- 
    procedure session( hst in varchar2,
                       prt in varchar2,
                       usr in varchar2,
                       pwd in varchar2 ) is
    begin

        impl.session( hst, prt, usr, pwd );

    end session;

    -- set session connection data
    procedure session( con in connection ) is
    begin

        impl.session( con );

    end session;

    --
    function session return connection is

        con connection;

    begin

        con := impl.session;

        if ( con is not null ) then

            con.pass := null;

        end if;

        return con;

    end session;

    --
    procedure dml( stm in varchar2,
                   bnd in binds      default null,
                   con in connection default null ) is
    begin

        impl.sql_dml( stm, bnd, con );

    end dml;

    --
    procedure ddl( stm in varchar2,
                   con in connection default null ) is
    begin

        impl.sql_ddl( stm, con );

    end ddl;

end remote;
/

show errors

--
-- ... done!
--
