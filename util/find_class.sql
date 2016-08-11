
set linesize 160
set pagesize 50000

col owner for a30
col name  for a80

select owner,
       dbms_java.longname( object_name ) name
  from dba_objects 
 where object_type like '%JAVA%'
   --and dbms_java.longname( object_name ) like '%ClientFactory%' 
   and dbms_java.longname( object_name ) like 'com.sun.security.sasl%'
/
