--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.pks.sql
--

--
prompt ... running hive.pks.sql

--
alter session set current_schema = hive;

--
create or replace package hive as

    -- implementation subtypes
    subtype service  is impl.service;
    subtype session  is impl.session;

    -- 
    procedure connection( usr in varchar2,
                          pwd in varchar2 );

    -- 
    procedure connection( hst in varchar2,
                          prt in service,
                          usr in varchar2,
                          pwd in varchar2 );

    -- set session connection data
    procedure connection( con in session );

    -- get session connection data (password returned is intentionally null)
    function connection return session;

    --
    function query( stm in varchar2,
                    bnd in binds default null ) return anydataset pipelined using hive_t;

    --
    procedure dml( stm in varchar2,
                   bnd in binds default null );

    --
    procedure ddl( stm in varchar2 );

end hive;
/

show errors

--
-- ... done!
--
