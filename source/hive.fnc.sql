--------------------------------------------------------------------------------
--
-- 2016-04-24, NV - hive.fnc.sql
--

--
prompt ... running hive.fnc.sql

--
alter session set current_schema = hive;

--
create or replace function hive_q( stm varchar2,
                                   bnd in binds      := null,
                                   con in connection := null ) return anydataset pipelined using hive_t;
/

show errors

-- bitand is intrinsic to PL/SQL, but not the following (so create them)

--
create or replace function bitor( x in number, y in number ) return number parallel_enable is
begin

    return x + y - bitand( x, y );

end bitor;
/

show errors

--
create or replace function bitxor( x in number, y in number ) return number parallel_enable is
begin

    return bitor( x, y ) - bitand( x, y );

end bitxor;
/ 

show errors

--
create or replace function bitnot( x in number ) return number parallel_enable is 
begin

    return ( 0 - x ) - 1;

end bitnot;
/

show errors

--
create or replace function oid( o in varchar2 ) return number parallel_enable is

    id number := null;

begin

    if ( o is null ) then

        id := to_number( sys_context( 'userenv', 'session_userid' ) );

    else

        select user# into id
          from sys.user$ u left outer join
               sys.resource_group_mapping$ cgm
               on ( cgm.attribute = 'ORACLE_USER'
                and cgm.status = 'ACTIVE'
                and cgm.value = u.name ),
               sys.user_astatus_map m
         where ( ( u.astatus = m.status# )
              or ( u.astatus = ( m.status# + 16 - bitand( m.status#, 16 ) ) ) )
           and u.type# in ( 0, 1 )
           and 1 = case when ( instr( o, '"' ) > 0 )
                        then case when u.name = replace( o, '"', '' )
                                  then 1
                                  else 0
                             end
                        else case when u.name = upper( o )
                                  then 1
                                  else 0
                             end
                   end;

    end if;

    return id;

    exception
        when no_data_found then
            return null;

end oid;
/

show errors

--
-- ... done!
--
