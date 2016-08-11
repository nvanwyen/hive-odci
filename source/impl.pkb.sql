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
    ctx constant varchar2( 7 ) := 'hivectx';

    --
    log_     number     := -1;
    session_ connection := connection( null, null, null, null );

    --
    function describe_( atr out attributes, stm in varchar2, bnd in binds, con in connection ) return number as
    language java
    name 'oracle.mti.hive.SqlDesc( oracle.sql.ARRAY[], java.lang.String, oracle.sql.ARRAY, oracle.sql.STRUCT ) return java.math.BigDecimal';

    --
    function describe_( atr out attributes, key in number ) return number as
    language java
    name 'oracle.mti.hive.SqlDesc( oracle.sql.ARRAY[], java.math.BigDecimal ) return java.math.BigDecimal';

    --
    function open_( key out number, stm in varchar2, bnd in binds, con in connection ) return number as
    language java
    name 'oracle.mti.hive.SqlOpen( java.math.BigDecimal[], java.lang.String, oracle.sql.ARRAY, oracle.sql.STRUCT ) return java.math.BigDecimal';

    --
    function fetch_( rws out records, key in number, num in number ) return number as
    language java
    name 'oracle.mti.hive.SqlFetch( oracle.sql.ARRAY[], java.math.BigDecimal, java.math.BigDecimal ) return java.math.BigDecimal';

    --
    function close_( key in number ) return number as
    language java
    name 'oracle.mti.hive.SqlClose( java.math.BigDecimal ) return java.math.BigDecimal';

    --
    procedure dml_( stm in varchar2, bnd in binds, con in connection ) as
    language java
    name 'oracle.mti.hive.SqlDml( java.lang.String, oracle.sql.ARRAY, oracle.sql.STRUCT )';

    --
    procedure ddl_( stm in varchar2, con in connection ) as
    language java
    name 'oracle.mti.hive.SqlDdl( java.lang.String, oracle.sql.STRUCT )';

    --
    function param_( n in varchar2 ) return varchar2 is

        v varchar2( 4000 );

    begin

        v := sys_context( ctx, substr( n, 1, 30 ), 4000 );

        if ( v is null ) then

            --
            select a.value into v
              from param$ a
             where a.name = n;

        end if;

        --
        return v;

        --
        exception
            when no_data_found then
                return null;

    end param_;

    --
    function log_level_ return number is
    begin

        if ( log_ = -1 ) then

            log_ := to_number( param_( 'log_level' ) );

            if ( log_ is null ) then

                log_ := none;

            end if;

        end if;

        return log_;

    end log_level_;

    -- convert from ATTRIBUTES array to ANYTYPE table
    procedure conv_( att in attributes, typ out anytype ) is

        --
        col anytype; 

    begin

        if ( att.count > 0 ) then

            --
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

            --
            anytype.begincreate( dbms_types.typecode_table, typ );
            typ.setinfo( null, null, null, null, null, col, dbms_types.typecode_object, 0 );
            typ.endcreate();

        end if;

    end conv_;

    --
    function url_ return varchar2 is

        i number           := 0;
        u varchar2( 4000 ) := param_( 'hive_jdbc_url' );
        v varchar2( 4000 ) := null;

    begin

        while ( true ) loop

            i := i + 1;
            v := param_( 'hive_jdbc_url.' || to_char( i ) );

            if ( length( trim( v ) ) > 0 ) then

                u := u || ';' || v;

            else

                exit;

            end if;

        end loop;

        return u;

    end url_;

    --
    function connection_ return connection is
    begin

        return session_;

    end connection_;

    --
    procedure connection_( con in connection ) is
    begin

        --
        session_.url := case when ( con.url is null )
                             then case when ( session_.url is null )
                                       then url_
                                       else session_.url
                                  end
                             else con.url
                        end;

        --
        session_.name := case when ( con.name is null )
                              then case when ( session_.name is null )
                                        then param_( 'hive_user' )
                                        else session_.name
                                   end
                              else con.name
                         end;

        --
        session_.pass := case when ( con.pass is null )
                              then case when ( session_.pass is null )
                                        then param_( 'hive_pass' )
                                        else session_.pass
                                   end
                              else con.pass
                         end;

        --
        session_.auth := case when ( con.auth is null )
                              then case when ( session_.auth is null )
                                        then nvl( param_( 'hive_auth' ), 'normal' )
                                        else session_.auth
                                   end
                              else con.auth
                         end;

    end connection_;

    --
    function current_ return connection is
    begin

        if ( ( session_.url  is null ) and
             ( session_.name is null ) and
             ( session_.pass is null ) and
             ( session_.auth is null ) ) then

            connection_( session_ );

        end if;

        return session_;

    end current_;

    --
    procedure log( typ in number, txt in varchar2 ) is

        pragma autonomous_transaction;

    begin

        if ( bitand( typ, log_level_ ) > 0 ) then

            insert into log$ a
            (
                a.stamp,
                a.type,
                a.text
            )
            values
            (
                current_timestamp,
                typ,
                txt
            );

            commit write immediate nowait;

        end if;

        exception
            when others then rollback;

    end log;

    --
    procedure session_log_level( typ in number ) is
    begin

        log_ := typ;

    end session_log_level;

    -- 
    procedure session( url in varchar2 ) is

        con connection := session_;

    begin

        con.url := url;

        session( con );

    end session;

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
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2 ) is

        con connection := session_;

    begin

        --
        con.url := case when ( url is null )
                        then url_
                        else url
                   end;

        --
        con.name := case when ( usr is null )
                         then param_( 'hive_user' )
                         else usr
                    end;

        --
        con.pass := case when ( pwd is null )
                         then param_( 'hive_pass' )
                         else pwd
                    end;

        --
        session( con );

    end session;

    -- 
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2,
                       ath in varchar2 ) is

        con connection := session_;

    begin

        --
        con.url := case when ( url is null )
                        then url_
                        else url
                   end;

        --
        con.name := case when ( usr is null )
                         then param_( 'hive_user' )
                         else usr
                    end;

        --
        con.pass := case when ( pwd is null )
                         then param_( 'hive_pass' )
                         else pwd
                    end;

        --
        con.auth := case when ( ath is null )
                         then nvl( param_( 'hive_auth' ), 'normal' )
                         else ath
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

        return current_;

    end session;

    --
    function sql_describe( stm in varchar2, bnd in binds, con in connection ) return anytype is

        ret number   := odciconst.error;
        typ anytype;

    begin

        ret := sql_describe( typ, stm, bnd, nvl( con, current_ ) );

        if ( ret = odciconst.success ) then

            return typ;

        end if;

        return null;

    end sql_describe;

    --
    function sql_describe( typ out anytype,
                           stm in  varchar2,
                           bnd in  binds      default null,
                           con in  connection default null ) return number is
        --
        ret number     := odciconst.error;
        att attributes := attributes();

    begin

        ret := describe_( att, stm, bnd, nvl( con, current_ ) );

        if ( ret = odciconst.success ) then

            if ( att.count > 0 ) then

                conv_( att, typ );

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

        ret := describe_( att, key );

        if ( ret = odciconst.success ) then

            if ( att.count > 0 ) then

                conv_( att, typ );

            else

                ret := odciconst.error;

            end if;

        end if;

        return ret;

    end sql_describe;

    --
    function sql_open( key out number,
                       stm in  varchar2,
                       bnd in  binds      default null,
                       con in  connection default null ) return number is
    begin

        return open_( key, stm, bnd, nvl( con, current_ ) );

    end sql_open;

    --
    function sql_fetch( key in  number,
                        num in  number,
                        rws out records ) return number is
    begin

        return fetch_( rws, key, num );

    end sql_fetch;

    --
    function sql_close( key in number ) return number is
    begin

        return close_( key );

    end sql_close;

    --
    procedure sql_dml( stm in  varchar2,
                       bnd in  binds      default null,
                       con in  connection default null ) is
    begin

        dml_( stm, bnd, con );

    end sql_dml;

    --
    procedure sql_ddl( stm in  varchar2,
                       con in  connection default null ) is
    begin

        ddl_( stm, con );

    end sql_ddl;

end impl;
/

show errors

--
-- ... done!
--
