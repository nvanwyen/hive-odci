--------------------------------------------------------------------------------
--
-- 2016-04-19, NV - impl.pks.sql
--

--
prompt ... running impl.pks.sql

--
alter session set current_schema = hive;

--
create or replace package impl as

    --
    none  constant number := 0;
    error constant number := 1;
    warn  constant number := 2;
    info  constant number := 4;
    trace constant number := 8;

    --
    procedure log( typ in number, txt in varchar2 );

    --
    procedure session_log_level( typ in number );

    -- 
    procedure session( krb in varchar2 );

    -- 
    procedure session( usr in varchar2,
                       pwd in varchar2 );

    -- 
    procedure session( hst in varchar2,
                       prt in varchar2,
                       krb in varchar2 );

    -- 
    procedure session( hst in varchar2,
                       prt in varchar2,
                       usr in varchar2,
                       pwd in varchar2 );

    --
    procedure session( con in connection );

    --
    function session return connection;

    --
    function sql_describe( stm in varchar2,
                           bnd in binds      default null,
                           con in connection default null ) return anytype;

    --
    function sql_describe( typ out anytype,
                           stm in  varchar2,
                           bnd in  binds      default null,
                           con in  connection default null ) return number;

    --
    function sql_describe( key in  number,
                           typ out anytype ) return number;

    --
    function sql_open( key out number,
                       stm in  varchar2,
                       bnd in  binds      default null,
                       con in  connection default null ) return number;

    --
    function sql_fetch( key in  number,
                        num in  number,
                        rws out records ) return number;

    --
    function sql_close( key in number ) return number;

end impl;
/

show errors

--
-- ... done!
--
