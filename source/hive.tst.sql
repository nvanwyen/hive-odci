
select * from table( hive.query( 'select * from movies' ) );
select * from table( hive_t( 'select * from movies' ) );
select * from table( hive_q( 'select * from movies' ) );
