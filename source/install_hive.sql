--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - install_hive.sql
--

--
prompt ... running install_hive.sql

-- account
@@hive.usr.sql

-- permissions
@@hive.prm.sql

-- tables
@@hive.tbl.sql
@@hive.idx.sql

--
@@hive.vws.sql

--
@@impl.pks.sql
@@impl.pkb.sql

--
@@bind.typ.sql

--
@@hive.tys.sql
@@hive.tyb.sql

--
@@hive.pks.sql
@@hive.pkb.sql

--
@@dbms_hive.pks.sql
@@dbms_hive.pkb.sql

--
@@hive.syn.sql

--
@hive.rol.sql

--
@@wrap.pls.sql

--
@@hive.gnt.sql

--
prompt ... show post installation errors

--
set linesize 160
set pagesize 50000

col name for a30 head "name"
col text for a80 head "text" word_wrap

select name,
       text
  from all_errors
 where owner = 'HIVE';

--
exit

--
-- ... done!
--
