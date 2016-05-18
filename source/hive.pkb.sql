--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.pkb.sql
--

--
prompt ... running hive.pkb.sql

--
alter session set current_schema = hive;

--
create or replace package body hive as

    --
    function param_( n in varchar2 ) return varchar2 is

        v varchar2( 4000 );

    begin

        --
        select a.value into v
          from param$ a
         where a.name = n;

        --
        return v;

        --
        exception
            when no_data_found then
                return null;

    end param_;

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

        null;

    end dml;

    --
    procedure ddl( stm in varchar2,
                   con in connection default null ) is
    begin

        null;

    end ddl;

end hive;
/

show errors

--
-- ... done!
--
