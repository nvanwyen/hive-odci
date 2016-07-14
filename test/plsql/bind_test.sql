declare

    b hive_binds := hive_binds();

begin

    hive_binding.append( value => 'xyz', lst => b );
    hive_binding.save( 'abc', b );

end;
/

-- create a new bind
exec hive_binding.append( 'abc', '123' );
exec hive_binding.append( 'abc', '456' );
exec hive_binding.append( 'abc', '789' );

-- create a new bind
exec hive_binding.append( 'xxx', 'aaa' );
exec hive_binding.append( 'xxx', 'bbb' );
exec hive_binding.append( 'xxx', 'ccc' );

exec hive_binding.deny( 'xxx', 'public' );

col key   for a12
col type  for a12
col scope for a12
col value for a12
select * from dba_hive_filters;


col key     for a12
col grantee for a21
col read    for a10
col write   for a10
select * from dba_hive_filter_privs;

grant hive_user to scott;
connect scott/tiger

declare

    b hive_binds;

begin

    b := hive_binding.get( 'abc' );

end;
/

col key   for a12
col type  for a12
col scope for a12
col value for a12
select * from user_hive_filters;


col key     for a12
col grantee for a21
col read    for a10
col write   for a10
select * from user_hive_filter_privs;


connect / as sysdba
exec hive_binding.allow( 'abc', 'scott', hive_binding.priv_readwrite );
exec hive_binding.deny( 'abc', 'public' );

exec hive_binding.allow( 'xyz', 'public', hive_binding.priv_read );
exec hive_binding.deny( 'xyz', 'public' );
