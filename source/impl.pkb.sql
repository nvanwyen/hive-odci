--------------------------------------------------------------------------------
--
-- 2016-04-19, NV - impl.pkb.sql
--

--
prompt ... running impl.pkb.sql

--
alter session set current_schema = hive;

--
create or replace package body impl as

    --
    session_ connection := connection( null, null, null, null );

    --
    function describe_( stm in varchar2, atr out attributes ) return number as
    language java
    name 'oracle.mti.hive.SqlDesc( java.lang.String, oracle.sql.ARRAY[] ) return java.math.BigDecimal';

    --
    function describe_( key in number, atr out attributes ) return number as
    language java
    name 'oracle.mti.hive.SqlDesc( java.math.BigDecimal, oracle.sql.ARRAY[] ) return java.math.BigDecimal';

    --
    function open_( stm in varchar2, key out number ) return number as
    language java
    name 'oracle.mti.hive.SqlOpen( java.lang.String, java.math.BigDecimal[] ) return java.math.BigDecimal';

    --
    function fetch_( key in number, num in number, rws out records ) return number as
    language java
    name 'oracle.mti.hive.SqlFetch( java.math.BigDecimal, java.math.BigDecimal, oracle.sql.ARRAY[] ) return java.math.BigDecimal';

    --
    function close_( key in number ) return number as
    language java
    name 'oracle.mti.hive.SqlClose( java.math.BigDecimal ) return java.math.BigDecimal';

    --
    function con_( con in connection ) return number as
    language java
    name 'oracle.mti.hive.SqlConnection( oracle.sql.STRUCT ) return java.math.BigDecimal';

    --
    function bnd_( bnd in binds ) return number as
    language java
    name 'oracle.mti.hive.SqlBinding( oracle.sql.ARRAY ) return java.math.BigDecimal';

    --
    procedure desc_( att in attributes, typ out anytype ) is

        --
        col anytype; 

    begin

        if ( att.count > 0 ) then

            -- --
            -- debug_attributes( 'desc_: att', att );
            -- --
            

            anytype.begincreate( dbms_types.typecode_object, col );

            --
            for i in 1 .. att.count loop

                --
                col.addattr( att( i ).name,
                             case when att( i ).code  = -1 then null else att( i ).code  end,
                             case when att( i ).prec  = -1 then null else att( i ).prec  end,
                             case when att( i ).scale = -1 then null else att( i ).scale end,
                             case when att( i ).len   = -1 then null else att( i ).len   end,
                             case when att( i ).csid  = -1 then null else att( i ).csid  end,
                             case when att( i ).csfrm = -1 then null else att( i ).csfrm end );

            end loop;

            --
            col.endcreate;

            -- --
            -- debug_info( 'desc_: col', col );
            -- --

            --
            anytype.begincreate( dbms_types.typecode_table, typ );
            typ.setinfo( null, null, null, null, null, col, dbms_types.typecode_object, 0 );
            typ.endcreate();

            -- --
            -- debug_info( 'desc_: typ', typ );
            -- --

        end if;

    end desc_;

    --
    function param_( n in varchar2 ) return varchar2 is

        v varchar2( 4000 );

    begin

        --
        select a.value into v
          from param$ a
         where a.name = n;

        --
        return v;

        --
        exception
            when no_data_found then
                return null;

    end param_;

    --
    function connection_ return connection is
    begin

        return session_;

    end connection_;

    --
    procedure connection_( con in connection ) is
    begin

        --
        session_.host := case when ( con.host is null )
                              then case when ( session_.host is null )
                                        then param_( 'default_hive_host' )
                                        else session_.host
                                   end
                              else con.host
                         end;

        --
        session_.port := case when ( con.port is null )
                              then case when ( session_.port is null )
                                        then param_( 'default_hive_port' )
                                        else session_.port
                                   end
                              else con.port
                         end;

        --
        session_.name := case when ( con.name is null )
                              then case when ( session_.name is null )
                                        then param_( 'default_hive_user' )
                                        else session_.name
                                   end
                              else con.name
                         end;

        --
        session_.pass := case when ( con.pass is null )
                              then case when ( session_.pass is null )
                                        then param_( 'default_hive_pass' )
                                        else session_.pass
                                   end
                              else con.pass
                         end;

    end connection_;

    -- 
    procedure session( usr in varchar2,
                       pwd in varchar2 ) is

        con connection := session_;

    begin

        con.name := usr;
        con.pass := pwd;

        session( con );

    end session;

    -- 
    procedure session( hst in varchar2,
                       prt in varchar2,
                       usr in varchar2,
                       pwd in varchar2 ) is

        con connection := session_;

    begin

        --
        con.host := case when ( hst is null )
                         then param_( 'default_hive_host' )
                         else hst
                    end;

        --
        con.port := case when ( prt is null )
                         then param_( 'default_hive_port' )
                         else prt
                    end;

        --
        con.name := case when ( usr is null )
                         then param_( 'default_hive_user' )
                         else usr
                    end;

        --
        con.pass := case when ( pwd is null )
                         then param_( 'default_hive_pass' )
                         else pwd
                    end;

        --
        session( con );

    end session;

    --
    procedure session( con in connection ) is
    begin

        connection_( con );

    end session;

    --
    function session return connection is
    begin

        return connection_;

    end session;

    --
    function sql_describe( stm in varchar2 ) return anytype is

        ret number   := odciconst.error;
        typ anytype;

    begin

        ret := sql_describe( stm, typ );

        if ( ret = odciconst.success ) then

            return typ;

        end if;

        return null;

    end sql_describe;

    --
    function sql_describe( stm in  varchar2,
                           typ out anytype ) return number is
        --
        ret number     := odciconst.error;
        att attributes := attributes();

    begin

        ret := describe_( stm, att );

        if ( ret = odciconst.success ) then

            if ( att.count > 0 ) then

                desc_( att, typ );

            else

                ret := odciconst.error;

            end if;

        end if;

        return ret;

    end sql_describe;

    --
    function sql_describe( key in  number,
                           typ out anytype ) return number is

        --
        ret number     := odciconst.error;
        att attributes := attributes();

    begin

        ret := describe_( key, att );

        if ( ret = odciconst.success ) then

            if ( att.count > 0 ) then

                desc_( att, typ );

                -- --
                -- debug_attributes( 'sql_describe: att', att );
                -- debug_info( 'sql_describe: typ', typ );
                -- --

            else

                ret := odciconst.error;

            end if;

        end if;

        return ret;

    end sql_describe;

    --
    function sql_open( stm in  varchar2,
                       key out number ) return number is
    begin

        return open_( stm, key );

    end sql_open;

    --
    function sql_fetch( key in  number,
                        num in  number,
                        rws out records ) return number is
    begin

        return fetch_( key, num, rws );

    end sql_fetch;

    --
    function sql_close( key in number ) return number is
    begin

        return close_( key );

    end sql_close;

--    /* ************************************** TESTING ************************************** */
--
--    --
--    procedure test_connection is
--
--        r number;
--
--    begin
--
--        session( 'host-x', '1234', 'jdoe', 'password' );
--        r := con_( connection_ );
--
--        /*
--            set serveroutput on
--            exec dbms_java.set_output( 1000000 );
--            exec impl.test_connection;
--        */
--
--    end test_connection;
--
--    --
--    procedure test_binding is
--
--        b binds := binds();
--        r number;
--
--    begin
--
--        b.extend;
--        b( b.count ) := bind( 'A', 1, 1 );
--
--        b.extend;
--        b( b.count ) := bind( 'B', 2, 2 );
--
--        b.extend;
--        b( b.count ) := bind( 'C', 3, 3 );
--
--        --r := bnd_( b );
--        r := bnd_( null );
--
--        /*
--            set serveroutput on
--            exec dbms_java.set_output( 1000000 );
--            exec impl.test_binding;
--        */
--
--    end test_binding;

end impl;
/

show errors

--
-- ... done!
--
