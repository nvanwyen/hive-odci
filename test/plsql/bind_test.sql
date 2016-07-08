declare

    b hive_binds := hive_binds();

begin

    hive_binding.append( value => 'xyz', lst => b );
    hive_binding.save( 'abc', b );

end;
/

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

exec hive_binding.allow( 'abc', 'scott', hive_binding.priv_read );
