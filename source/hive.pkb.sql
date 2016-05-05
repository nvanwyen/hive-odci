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
    procedure connection( usr in varchar2,
                          pwd in varchar2 ) is
    begin

        --
        impl.connection( usr, pwd );

    end connection;

    -- 
    procedure connection( hst in varchar2,
                          prt in service,
                          usr in varchar2,
                          pwd in varchar2 ) is
    begin

        --
        impl.connection( hst, prt, usr, pwd );

    end connection;

    --
    procedure connection( con in session ) is
    begin

        --
        impl.connection( con );

    end connection;

    --
    function connection return session is

        con session;

    begin

        --
        con := impl.connection;
        con.pass := null;

        --
        return con;

    end connection;

    --
    procedure dml( stm in varchar2,
                   bnd in binds default null ) is
    begin

        null;

    end dml;

    --
    procedure ddl( stm in varchar2 ) is
    begin

        null;

    end ddl;

end hive;
/

show errors

--
-- ... done!
--
