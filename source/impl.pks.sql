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
    procedure session( usr in varchar2,
                       pwd in varchar2 );

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
    function sql_describe( stm in varchar2 ) return anytype;

    --
    function sql_describe( stm in  varchar2,
                           typ out anytype ) return number;

    --
    function sql_describe( key in  number,
                           typ out anytype ) return number;

    --
    function sql_open( stm in  varchar2,
                       key out number ) return number;

    --
    function sql_fetch( key in  number,
                        num in  number,
                        rws out records ) return number;

    --
    function sql_close( key in number ) return number;

--    --
--    procedure test_connection;
--    procedure test_binding;

end impl;
/

show errors

--
-- ... done!
--
