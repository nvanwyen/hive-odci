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
    connect_ session;

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
    function connection_ return session is
    begin

        return connect_;

    end connection_;

    --
    procedure connection_( con in session ) is
    begin

        --
        connect_.host := case when ( con.host is null )
                              then case when ( connect_.host is null )
                                        then param_( 'default_hive_host' )
                                        else connect_.host
                                   end
                              else con.host
                         end;

        --
        connect_.port := case when ( con.port is null )
                              then case when ( connect_.port is null )
                                        then param_( 'default_hive_port' )
                                        else connect_.port
                                   end
                              else con.port
                         end;

        --
        connect_.user := case when ( con.user is null )
                              then case when ( connect_.user is null )
                                        then param_( 'default_hive_user' )
                                        else connect_.user
                                   end
                              else con.user
                         end;

        --
        connect_.pass := case when ( con.pass is null )
                              then case when ( connect_.pass is null )
                                        then param_( 'default_hive_pass' )
                                        else connect_.pass
                                   end
                              else con.pass
                         end;

    end connection_;

    -- 
    procedure connection( usr in varchar2,
                          pwd in varchar2 ) is

        con session := connect_;

    begin

        con.user := usr;
        con.pass := pwd;

        connection( con );

    end connection;

    -- 
    procedure connection( hst in varchar2,
                          prt in service,
                          usr in varchar2,
                          pwd in varchar2 ) is

        con session := connect_;

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
        con.user := case when ( usr is null )
                         then param_( 'default_hive_user' )
                         else usr
                    end;

        --
        con.pass := case when ( pwd is null )
                         then param_( 'default_hive_pass' )
                         else pwd
                    end;

        --
        connection( con );

    end connection;

    --
    procedure connection( con in session ) is
    begin

        connection_( con );

    end connection;

    --
    function connection return session is
    begin

        return connection_;

    end connection;

    --
    function sql_describe( stm in  varchar2,
                           atr out attributes ) return number is
    begin

        return describe_( stm, atr );

    end sql_describe;

    --
    function sql_describe( key in  number,
                           typ out anytype ) return number is

        --
        col anytype; 

        --
        ret number     := odciconst.error;
        att attributes := attributes();

    begin

        ret := describe_( key, att );

        if ( ret = odciconst.success ) then

            if ( att.count > 0 ) then

                anytype.begincreate( dbms_types.typecode_object, col );

                --
                for i in 1 .. att.count loop

                    begin

                    --
                    col.addattr( att( i ).name,
                                 case when att( i ).code  = -1 then null else att( i ).code  end,
                                 case when att( i ).prec  = -1 then null else att( i ).prec  end,
                                 case when att( i ).scale = -1 then null else att( i ).scale end,
                                 case when att( i ).len   = -1 then null else att( i ).len   end,
                                 case when att( i ).csid  = -1 then null else att( i ).csid  end,
                                 case when att( i ).csfrm = -1 then null else att( i ).csfrm end );

                    exception
                        when others then
                            raise_application_error( -20002, 'WTF!' );

                    end;

                end loop;

                --
                col.endcreate;

                --
                anytype.begincreate( dbms_types.typecode_table, typ );
                typ.setinfo( null, null, null, null, null, col, dbms_types.typecode_object, 0 );
                typ.endcreate();

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

end impl;
/

show errors

--
-- ... done!
--
