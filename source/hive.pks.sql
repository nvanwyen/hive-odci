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

    -- get session level paraemter
    function session_param( name  in varchar2 ) return varchar2;

    -- set session level paraemter
    procedure session_param( name  in varchar2,
                             value in varchar2 );

    -- set the session log level
    procedure session_log_level( typ in number );

    -- (re)set connection paraemter
    procedure session( url in varchar2 );

    -- (re)set connection paraemters
    procedure session( usr in varchar2,
                       pwd in varchar2 );

    -- (re)set connection paraemters
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2 );

    -- (re)set connection paraemters
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2,
                       ath in varchar2 );

    -- (re)set the connection object
    procedure session( con in connection );

    -- current session object
    function session return connection;

    -- execute a remote query
    function query( stm in varchar2,
                    bnd in binds      default null,
                    con in connection default null ) return anydataset pipelined using hive_t;

    -- execute remote DML
    procedure dml( stm in varchar2,
                   bnd in binds      default null,
                   con in connection default null );

    -- execute remote DDL
    procedure ddl( stm in varchar2,
                   con in connection default null );

end remote;
/

show errors

--
-- ... done!
--
