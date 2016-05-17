--------------------------------------------------------------------------------
--
-- 2016-04-07, NV - hive.tbl.sql
--

--
prompt ... running hive.tbl.sql

--
alter session set current_schema = hive;

--
create table param$
(
    name  varchar2( 240 ) not null,
    value varchar2( 4000 )    null
)
/

--
create table filter$
(
    key   varchar2( 64 )    not null,
    seq   number            not null,
    type  number            not null,
    scope number            not null,
    value varchar2( 4000 )      null
)
/

--
-- ... done!
--
