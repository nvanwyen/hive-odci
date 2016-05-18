--------------------------------------------------------------------------------
--
-- 2016-04-06, NV - install_hive.sql
--

--
whenever oserror  exit 9;
whenever sqlerror exit sql.sqlcode;

--
column logfile new_value logfile noprint;

--
set termout off;
select sys_context( 'userenv', 'db_name' ) 
    || '_install_hive_odci.' 
    || to_char( sysdate, 'YYYYMMDDHH24MISS' ) 
    || '.log' logfile
  from dual;
set termout on;

--
spool &&logfile

--
prompt ... running install_hive.sql

--
select current_timestamp "beginning installation"
  from dual;

-- schema
@@hive.usr.sql

-- roles
@@hive.rol.sql

-- schema permissions
@@hive.prm.sql

-- tables
@@hive.tbl.sql
@@hive.idx.sql

-- views
@@hive.vws.sql

-- types
@@attr.typ.sql
@@bind.typ.sql

-- implemntation
@@impl.pks.sql
@@impl.pkb.sql

-- administrative
@@dbms_hive.pks.sql
@@dbms_hive.pkb.sql

-- transient
@@hive.jva.sql
@@hive.typ.sql

-- interface
@@hive.pks.sql
@@hive.pkb.sql

-- grants
@@hive.gnt.sql

-- synonyms
@@hive.syn.sql

-- parameters
@@hive.par.sql

-- obfuscation
@@wrap.pls.sql

--
prompt ... show post installation object errors

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
select current_timestamp "completed installation"
  from dual;

--
spool off
exit

--
-- ... done!
--
