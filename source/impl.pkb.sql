--------------------------------------------------------------------------------
--
-- 2016-04-19, NV - impl.pkb.sql
--

/*
  Hive-ODCI - Copyright (C) 2006-2016 Metasystems Technologies Inc. (MTI)
  Nicholas Van Wyen
  
  This library is free software; you can redistribute it and/or modify it 
  under the terms of the GNU Lesser General Public License as published by 
  the Free Software Foundation; either version 2.1 of the License, or (at 
  your option) any later version.
  
  This library is distributed in the hope that it will be useful, but 
  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
  or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
  License for more details.
  
  You should have received a copy of the GNU Lesser General Public License 
  along with this library; if not, write to the
  
                  Free Software Foundation, Inc.
                  59 Temple Place, Suite 330,
                  Boston, MA 02111-1307 USA
*/

--
prompt ... running impl.pkb.sql

--
alter session set current_schema = hive;

--
create or replace package body impl as

    --
    ctx_ constant varchar2( 7 ) := 'hivectx';

    --
    log_     number     := -1;
    session_ connection := connection( null, null, null, null );

    --
    function describe_( atr out attributes, stm in varchar2, bnd in binds, con in connection ) return number as
    language java
    name 'oracle.mti.odci.hive.SqlDesc( oracle.sql.ARRAY[], java.lang.String, oracle.sql.ARRAY, oracle.sql.STRUCT ) return java.math.BigDecimal';

    --
    function describe_( atr out attributes, key in number ) return number as
    language java
    name 'oracle.mti.odci.hive.SqlDesc( oracle.sql.ARRAY[], java.math.BigDecimal ) return java.math.BigDecimal';

    --
    function open_( key out number, stm in varchar2, bnd in binds, con in connection ) return number as
    language java
    name 'oracle.mti.odci.hive.SqlOpen( java.math.BigDecimal[], java.lang.String, oracle.sql.ARRAY, oracle.sql.STRUCT ) return java.math.BigDecimal';

    --
    function fetch_( rws out records, key in number, num in number ) return number as
    language java
    name 'oracle.mti.odci.hive.SqlFetch( oracle.sql.ARRAY[], java.math.BigDecimal, java.math.BigDecimal ) return java.math.BigDecimal';

    --
    function close_( key in number ) return number as
    language java
    name 'oracle.mti.odci.hive.SqlClose( java.math.BigDecimal ) return java.math.BigDecimal';

    --
    procedure dml_( stm in varchar2, bnd in binds, con in connection ) as
    language java
    name 'oracle.mti.odci.hive.SqlDml( java.lang.String, oracle.sql.ARRAY, oracle.sql.STRUCT )';

    --
    procedure ddl_( stm in varchar2, con in connection ) as
    language java
    name 'oracle.mti.odci.hive.SqlDdl( java.lang.String, oracle.sql.STRUCT )';

    --
    function param_( n in varchar2 ) return varchar2 is

        v varchar2( 4000 );

    begin

        if ( log_ > 0 ) then

            log_trace( 'impl::param_( ' || n || ' ) called' );

        end if;

        v := sys_context( ctx_, substr( n, 1, 30 ), 4000 );

        if ( v is null ) then

            --
            select a.value into v
              from param$ a
             where a.name = n;

            if ( log_ > 0 ) then

                log_trace( 'impl::param_( ' || n || ' ) not found at session level' );

            end if;

        else

            if ( log_ > 0 ) then

                log_trace( 'impl::param_( ' || n || ' ) found at session level' );

            end if;

        end if;

        --
        if ( log_ > 0 ) then

            log_trace( 'impl::param_( ' || n || ' ) returns: ' || v );

        end if;

        return v;

        --
        exception
            when no_data_found then
                --
                if ( log_ > 0 ) then

                    log_trace( 'impl::param_( ' || n || ' ) no data found' );

                end if;
                return null;

            when others then
                --
                if ( log_ > 0 ) then

                    log_trace( 'impl::param_ error: ' || sqlerrm );

                end if;
                raise;

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

        log_trace( 'impl::conv_( <att>, <typ> ) called' );

        if ( att.count > 0 ) then

            --
            anytype.begincreate( dbms_types.typecode_object, col );

            log_trace( 'impl::conv_ began ANYTYPE object creation' );

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

                log_trace( 'impl::conv_ added ATTRIBUTE: ' || att( i ).name );

            end loop;

            --
            col.endcreate;

            log_trace( 'impl::conv_ ended ANYTYPE object creation' );

            --
            anytype.begincreate( dbms_types.typecode_table, typ );
            typ.setinfo( null, null, null, null, null, col, dbms_types.typecode_object, 0 );
            typ.endcreate();

            log_trace( 'impl::conv_ set ANYTYPE metadata' );

        else

            log_trace( 'impl::conv_ ATTRIBUTE count is zero' );

        end if;

    end conv_;

    --
    function url_ return varchar2 is

        i number           := 0;
        u varchar2( 4000 ) := param_( 'hive_jdbc_url' );
        v varchar2( 4000 ) := null;

    begin

        log_trace( 'impl::url_ called' );

        while ( true ) loop

            i := i + 1;
            v := param_( 'hive_jdbc_url.' || to_char( i ) );

            if ( length( trim( v ) ) > 0 ) then

                log_trace( 'impl::url_ found ' || 'hive_jdbc_url.' || to_char( i ) || ': ' || v );

                u := u || ';' || v;

            else

                log_trace( 'impl::url_ ' || 'hive_jdbc_url.' || to_char( i - 1 ) || ' is last parameter' );
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
                a.name,
                a.type,
                a.text
            )
            values
            (
                current_timestamp,
                dbms_standard.login_user,
                typ,
                txt
            );

            commit write immediate nowait;

        end if;

        exception
            when others then rollback;

    end log;

    --
    procedure log_error( txt in varchar2 ) is
    begin

        log( error, txt );

    end log_error;

    --
    procedure log_warn( txt in varchar2 ) is
    begin

        log( warn, txt );

    end log_warn;

    --
    procedure log_info( txt in varchar2 ) is
    begin

        log( info, txt );

    end log_info;

    --
    procedure log_trace( txt in varchar2 ) is
    begin

        log( trace, txt );

    end log_trace;

    --
    function session_param( name in varchar2 ) return varchar2 is

        n varchar2( 4000 ) := substr( name, 1, 30 );
        v varchar2( 4000 ) := null;

    begin

        v := sys_context( ctx_, n, 4000 );

        log_trace( 'impl::session_param( ' || n || ' ) context: ' || ctx_ || ', returns: ' || v );
        return v;

        exception
            when others then
                log_error( 'impl::session_param( ' || n || ' ) error: ' || sqlerrm );
                raise;

    end session_param;

    --
    procedure session_param( name  in varchar2,
                             value in varchar2 ) is
    begin

        if ( name in ( 'application',
                       'version',
                       'license',
                       'encrypted_values',
                       'hive_users',
                       'hive_admin',
                       'hive_jdbc_driver',
                       'query_limit' ) ) then

            log_warn( 'impl::session_param( ' || name || ' ): ' || es_not_eligible );
            raise_application_error( ec_not_eligible, es_not_eligible );

        else

            log_info( 'impl::session_param( ' || name || ' ) set: ' || nvl( value, '{null}' ) );
            dbms_session.set_context( ctx_, substr( name, 1, 30 ), value );

        end if;

        exception
            when others then
                log_error( 'impl::session_param( ' || name || ', ' || value || ' ) error: ' || sqlerrm );
                raise;

    end session_param;

    --
    procedure session_log_level( typ in number ) is
    begin

        log_info( 'impl::session_log_level resetting value ' || to_char( log_ ) || ' to ' || to_char( typ ) );
        log_ := typ;

    end session_log_level;

    --
    procedure session_clear is
    begin

        log_trace( 'impl::session_clear called' );
        session_ := connection( null, null, null, null );

    end session_clear;

    -- 
    procedure session( url in varchar2 ) is

        con connection := session_;

    begin

        log_trace( 'impl::session( ' || url || ' ) called' );

        con.url := url;

        session( con );

        exception
            when others then
                log_error( 'impl::session( ' || url || ' ) error: ' || sqlerrm );
                raise;

    end session;

    -- 
    procedure session( usr in varchar2,
                       pwd in varchar2 ) is

        con connection := session_;

    begin

        log_trace( 'impl::session( ' || usr || ', ' 
                                     || pwd || ' ) called' );

        con.name := usr;
        con.pass := pwd;

        session( con );

        exception
            when others then
                log_error( 'impl::session( ' || usr || ', ' || pwd || ' ) error: ' || sqlerrm );
                raise;

    end session;

    -- 
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2 ) is

        con connection := session_;

    begin

        log_trace( 'impl::session( ' || url || ', ' 
                                     || usr || ', ' 
                                     || pwd || ' ) called' );

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

        exception
            when others then
                log_error( 'impl::session( ' || url || ', ' || pwd || ' ) error: ' || sqlerrm );
                raise;

    end session;

    -- 
    procedure session( url in varchar2,
                       usr in varchar2,
                       pwd in varchar2,
                       ath in varchar2 ) is

        con connection := session_;

    begin

        log_trace( 'impl::session( ' || url || ', ' 
                                     || usr || ', ' 
                                     || pwd || ', ' 
                                     || ath || ' ) called' );

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

        log_trace( 'impl::session( ' || con.url || ', ' 
                                     || con.name || ', ' 
                                     || con.pass || ', ' 
                                     || con.auth || ' ) by object called' );
        connection_( con );

        exception
            when others then
                log_error( 'impl::session( ' || con.url || ', ' 
                                             || con.name || ', ' 
                                             || con.pass || ', ' 
                                             || con.auth || ' ) object error: ' || sqlerrm );
                raise;

    end session;

    --
    function session return connection is
    begin

        log_trace( 'impl::session returns currect connection' );
        return current_;

        exception
            when others then
                log_error( 'impl::session currect connection error: ' || sqlerrm );
                raise;

    end session;

    --
    function sql_describe( stm in varchar2, bnd in binds, con in connection ) return anytype is

        ret number   := odciconst.error;
        typ anytype;

    begin

        log_trace( 'impl::sql_describe( ' || nvl( stm, '{null}' ) || ', <bnd>, <con> ) called' );

        ret := sql_describe( typ, stm, bnd, nvl( con, current_ ) );

        if ( ret = odciconst.success ) then

            log_trace( 'impl::sql_describe success for: ' || nvl( stm, '{null}' ) );
            return typ;

        end if;

        log_warn( 'impl::sql_describe failed for: ' || nvl( stm, '{null}' ) );
        return null;

        exception
            when others then
                log_error( 'impl::sql_describe ' || nvl( stm, '{null}' ) || ', error: ' || sqlerrm );
                raise;

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

        log_trace( 'impl::sql_describe( <typ>, ' || nvl( stm, '{null}' ) || ', <bnd>, <con> ) called' );

        ret := describe_( att, stm, bnd, nvl( con, current_ ) );

        if ( ret = odciconst.success ) then

            log_trace( 'impl::sql_describe success for: ' || nvl( stm, '{null}' ) );

            if ( att.count > 0 ) then

                log_trace( 'impl::sql_describe succeeded with ' || to_char( att.count ) || ' attribute(s)' );
                conv_( att, typ );

            else

                log_warn( 'impl::sql_describe failed (no attributes) for: ' || nvl( stm, '{null}' ) );
                ret := odciconst.error;

            end if;

        end if;

        log_trace( 'impl::sql_describe returns: ' || to_char( ret ) );
        return ret;

        exception
            when others then
                log_error( 'impl::sql_describe ' || nvl( stm, '{null}' ) || ', error: ' || sqlerrm );
                raise;

    end sql_describe;

    --
    function sql_describe( key in  number,
                           typ out anytype ) return number is

        --
        ret number     := odciconst.error;
        att attributes := attributes();

    begin

        log_trace( 'impl::sql_describe( ' || to_char( key ) || ', <typ> ) called' );

        ret := describe_( att, key );

        if ( ret = odciconst.success ) then

            if ( att.count > 0 ) then

                log_trace( 'impl::sql_describe succeeded with ' || to_char( att.count ) 
                                                                || ' attribute(s) for key: ' 
                                                                || to_char( key ) );
                conv_( att, typ );

            else

                log_warn( 'impl::sql_describe failed (no attributes) for: ' || to_char( key ) );
                ret := odciconst.error;

            end if;

        else

            log_warn( 'impl::sql_describe falied for key: ' || to_char( key ) );

        end if;

        log_trace( 'impl::sql_describe returns: ' || to_char( ret ) || ' for key: ' || to_char( key ) );
        return ret;

        exception
            when others then
                log_error( 'impl::sql_describe key: ' || to_char( key ) || ', error: ' || sqlerrm );
                raise;

    end sql_describe;

    --
    function sql_open( key out number,
                       stm in  varchar2,
                       bnd in  binds      default null,
                       con in  connection default null ) return number is

        rc number := 0;
        ky number := 0;

    begin

        log_trace( 'impl::sql_open( <key>, ' || nvl( stm, '{null}' ) || ', <bnd>, <con> ) called' );

        rc := open_( ky, stm, bnd, nvl( con, current_ ) );
        log_trace( 'impl::sql_open setting key: ' || to_char( ky ) );
        key := ky;

        log_trace( 'impl::sql_open returns: ' || to_char( rc ) );
        return rc;

        exception
            when others then
                log_error( 'impl::sql_open ' || nvl( stm, '{null}' ) || ', error: ' || sqlerrm );
                raise;

    end sql_open;

    --
    function sql_fetch( key in  number,
                        num in  number,
                        rws out records ) return number is
    begin

        log_trace( 'impl::sql_fetch( ' || to_char( key ) || ', ' || to_char( num ) || ', <rws> ) called' );
        return fetch_( rws, key, num );

        exception
            when others then
                log_error( 'impl::sql_fetch key: ' || to_char( key ) 
                                      || ', num: ' || to_char( num ) 
                                      || ', error: ' || sqlerrm );
                raise;

    end sql_fetch;

    --
    function sql_close( key in number ) return number is
    begin

        log_trace( 'impl::sql_close( ' || to_char( key ) || ' ) called' );
        return close_( key );

        exception
            when others then
                log_error( 'impl::sql_close key: ' || to_char( key ) 
                                    || ', error: ' || sqlerrm );
                raise;

    end sql_close;

    --
    procedure sql_dml( stm in  varchar2,
                       bnd in  binds      default null,
                       con in  connection default null ) is
    begin

        log_trace( 'impl::sql_dml( ' || nvl( stm, '{null}' ) || ', <bnd>, <con> ) called' );
        dml_( stm, bnd, con );

        exception
            when others then
                log_error( 'impl::sql_dml ' || nvl( stm, '{null}' ) || ', error: ' || sqlerrm );
                raise;

    end sql_dml;

    --
    procedure sql_ddl( stm in  varchar2,
                       con in  connection default null ) is
    begin

        log_trace( 'impl::sql_ddl( ' || nvl( stm, '{null}' ) || ', <con> ) called' );
        ddl_( stm, con );

        exception
            when others then
                log_error( 'impl::sql_ddl ' || nvl( stm, '{null}' ) || ', error: ' || sqlerrm );
                raise;

    end sql_ddl;

end impl;
/

show errors

--
-- ... done!
--
