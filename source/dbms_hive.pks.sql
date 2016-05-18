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
    function param( name in varchar2 ) return varchar2;
    procedure param( name in varchar2, value in varchar2 );

    --
    procedure purge_log;
    procedure purge_filter( key in varchar2 default null );

end dbms_hive;
/

show errors

--
-- ... done!
--
