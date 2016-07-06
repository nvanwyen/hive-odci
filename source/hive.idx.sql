--------------------------------------------------------------------------------
--
-- 2016-04-07, NV - hive.idx.sql
--

--
prompt ... running hive.idx.sql

--
alter session set current_schema = hive;

--
create unique index param$name
    on param$ ( name )
/

--
create index param$value
    on param$ ( value )
/

--
create unique index filter$key
    on filter$ ( key, seq, owner )
/

--
create index filter$seq
    on filter$ ( seq, type, owner, scope )
/

--
-- ... done!
--
