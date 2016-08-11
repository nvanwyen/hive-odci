--------------------------------------------------------------------------------
--
-- 2016-04-08, NV - dbms_hive.pks.sql
--

--
prompt ... running dbms_hive.pks.sql

--
alter session set current_schema = hive;

--
create or replace package dbms_hive as

    --
    function exist( name in varchar2 ) return boolean;      -- paraemter exists

    --
    function param( name in varchar2 ) return varchar2;     -- get
    procedure param( name in varchar2, value in varchar2 ); -- set

    --
    procedure remove( name in varchar2 );                   -- unset (e.g. remove)

    --
    procedure purge_log;
    procedure purge_filter( key in varchar2 default null );

end dbms_hive;
/

show errors

--
-- ... done!
--
