--------------------------------------------------------------------------------
--
-- 2016-04-30, NV - hive.par.sql
--

--
prompt ... running hive.par.sql

--
alter session set current_schema = hive;

--
set serveroutput on

--
declare

    --
    function exists_( n varchar2 ) return boolean is

        c number := 0;

    begin

        select count(0) into c
          from param$
         where name = n;

        return ( c > 0 );

    end exists_;

    --
    procedure insert_( n in varchar2, v in varchar2 ) is

        pragma autonomous_transaction;

    begin

        insert into param$ ( name, value )
        values ( n, v );

        dbms_output.put_line( 'Paraemter added:   ' || n );
        commit;

        exception
            when others then
                rollback;
                dbms_output.put_line( 'Insert error [' || n || ']: ' || sqlerrm );

    end insert_;

    --
    procedure update_( n in varchar2, v in varchar2 ) is

        pragma autonomous_transaction;

    begin

        update param$
           set value = v
         where name = n;

        dbms_output.put_line( 'Paraemter updated: ' || n );
        commit;

        exception
            when others then
                rollback;
                dbms_output.put_line( 'Update error [' || n || ']: ' || sqlerrm );

    end update_;

    --
    procedure param_( n in varchar2, v in varchar2 ) is
    begin

        if ( exists_( n ) ) then

            update_( n, v );

        else

            insert_( n, v );

        end if;

    end param_;

begin

    --
    param_( 'application', 'Hive ODCI' );
    param_( 'version', 'v0.0.0.2' );

    --
    param_( 'log_level', 'error, warn, info, debug' );

    --
    param_( 'encrypted_values', 'hive_pass' );

    --
    param_( 'hive_users', 'hive_user, hive_admin' );
    param_( 'hive_admin', 'hive_admin' );

    --
    param_( 'hive_jdbc_driver', 'com.ddtek.jdbc.hive.HiveDriver' );
    param_( 'hive_jdbc_url', 'jdbc:datadirect:hive://%host%:%port%;User=%user%;Password=%pass%' );

    --
    param_( 'bind_priv', 'public' );

    --
    param_( 'hive_host', 'orabdc.local' );
    param_( 'hive_port', '10000' );
    param_( 'hive_user', 'oracle' );
    param_( 'hive_pass', 'welcome1' );

end;
/

--
-- ... done!
--
