-- issue #3

whenever sqlerror exit sql.sqlcode;

--
exec hive.remote.session_log_level( 15 );
exec dbms_hive.purge_log;

--
exec hive.impl.log_trace( '---------------- test start (issue #3) ----------------' );

--
col tab_name for a30

--

-- second fails with "Invalid key"
select * from table( hive_q( 'show tables', null, null ) );


exec hive.impl.log_trace( '---------------- test fail (issue #3) ----------------' );
select * from table( hive_q( 'show tables', null, null ) );

exec hive.impl.log_trace( '---------------- test end (issue #3) ----------------' );

exec hive.impl.log_trace( '---------------- test union (issue #3) ----------------' );
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) )
union
select * from table( hive_q( 'show tables', null, null ) );

--
exit
