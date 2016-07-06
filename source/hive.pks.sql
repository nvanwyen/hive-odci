--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - hive.pks.sql
--

--
prompt ... running hive.pks.sql

--
alter session set current_schema = hive;

--
create or replace package remote as

    --
    function session_param( name  in varchar2 ) return varchar2;

    --
    procedure session_param( name  in varchar2,
                             value in varchar2 );

    -- 
    procedure session( usr in varchar2,
                       pwd in varchar2 );

    -- 
    procedure session( hst in varchar2,
                       prt in varchar2,
                       usr in varchar2,
                       pwd in varchar2 );

    -- set session connection data
    procedure session( con in connection );

    -- get session connection data (password returned is intentionally null)
    function session return connection;

    --
    function query( stm in varchar2,
                    bnd in binds      default null,
                    con in connection default null ) return anydataset pipelined using hive_t;

    --
    procedure dml( stm in varchar2,
                   bnd in binds      default null,
                   con in connection default null );

    --
    procedure ddl( stm in varchar2,
                   con in connection default null );

end remote;
/

show errors

--
-- ... done!
--
