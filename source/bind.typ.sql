--------------------------------------------------------------------------------
--
-- 2016-04-7, NV - bind.typ.sql
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
prompt ... running bind.typ.sql

--
alter session set current_schema = hive;

--
create or replace type bind as object
(
    value   varchar2( 4000 ), -- value
    type    number,           -- typeof (e.g. bool, string, ...)
    scope   number            -- reference (e.g. in, out, ... )
);
/

show errors

--
create or replace type binds as table of bind;
/

show errors

--
create or replace package binding as

    --
    subtype reference is number;
    subtype typeof    is number;
    subtype guard     is number;

    --
    none            constant number    :=  0;

    --
    priv_read       constant guard     :=  1;
    priv_write      constant guard     :=  2;
    priv_readwrite  constant guard     :=  3; -- priv_read + priv_write

    --
    scope_in        constant reference :=  1;
    scope_out       constant reference :=  2;
    scope_inout     constant reference :=  3;

    --
    type_bool       constant typeof    :=  1;
    type_date       constant typeof    :=  2;
    type_float      constant typeof    :=  3;
    type_int        constant typeof    :=  4;
    type_long       constant typeof    :=  5;
    type_null       constant typeof    :=  6;
    type_rowid      constant typeof    :=  7;
    type_short      constant typeof    :=  8;
    type_string     constant typeof    :=  9;
    type_time       constant typeof    := 10;
    type_timestamp  constant typeof    := 11;
    type_url        constant typeof    := 12;

    --
    function get( key in varchar2 ) return binds;
    function get( idx in number, lst in binds ) return bind;

    --
    function count( key in varchar2 ) return number;
    function count( lst in binds ) return number;

    --
    function new( value in varchar2, 
                  type  in typeof    default type_string,
                  scope in reference default scope_in ) return bind;

    --
    procedure append( key   in varchar2,
                      value in varchar2, 
                      type  in typeof    default type_string,
                      scope in reference default scope_in );

    --
    procedure append( value in     varchar2, 
                      type  in     typeof    default type_string,
                      scope in     reference default scope_in,
                      lst   in out binds );

    --
    procedure append( key in varchar2, val in bind );
    procedure append( val in bind, lst in out binds );

    --
    procedure append( key in varchar2, val in binds );
    procedure append( val in binds, lst in out binds );

    --
    procedure change( key   in varchar2,
                      idx   in number,
                      value in varchar2, 
                      type  in typeof    default type_string,
                      scope in reference default scope_in );

    --
    procedure remove( key in varchar2,
                      idx in number );

    --
    procedure replace( val in binds, lst in out binds );

    --
    procedure clear( key in varchar2 );
    procedure clear( lst in out binds );

    --
    procedure allow( key in varchar2,
                     act in varchar2,
                     lvl in guard default priv_readwrite );

    --
    procedure deny( key in varchar2,
                    act in varchar2 );

    --
    procedure save( key in varchar2, lst in binds );

    --
    ex_unknown  exception;
    es_unknown  constant varchar2( 256 ) := 'Unknown error encountered';
    ec_unknown  constant number := -20001;
    pragma      exception_init( ex_unknown, -20001 );

    ex_denied   exception;
    es_denied   constant varchar2( 256 ) := 'Request denied, insufficient privileges';
    ec_denied   constant number := -20002;
    pragma      exception_init( ex_denied, -20002 );

    ex_no_grant exception;
    es_no_grant constant varchar2( 256 ) := 'Privileges not granted';
    ec_no_grant constant number := -20003;
    pragma      exception_init( ex_no_grant, -20003 );

end binding;
/

show errors

--
create or replace package body binding as

    --
    ctx constant varchar2( 7 ) := 'hivectx';

    --
    procedure log_err_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_error( :p0 ); end;'
          using 'binding:' || txt;

        exception when others then null;

    end log_err_;

    --
    procedure log_wrn_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_warn( :p0 ); end;'
          using 'binding:' || txt;

        exception when others then null;

    end log_wrn_;

    --
    procedure log_inf_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_info( :p0 ); end;'
          using 'binding:' || txt;

        exception when others then null;

    end log_inf_;

    --
    procedure log_trc_( txt varchar2 ) is
    begin

        execute immediate 'begin impl.log_trace( :p0 ); end;'
          using 'binding:' || txt;

        exception when others then null;

    end log_trc_;

    --
    function token_( l varchar2, i number, d varchar2 := ',' ) return varchar2 is

       s number;
       e number;

    begin

       if ( i = 1 ) then

           s := 1;

       else

           -- positive number
           s := instr( l, d, 1, i - 1 );

           if ( s = 0 ) then

               return null;

           else

               s := s + length( d );

           end if;

       end if;

       e := instr( l, d, s, 1 );

       if ( e = 0 ) then

           return substr( l, s );

       else

           return substr( l, s, e - s );

       end if;

    end token_;

    --
    function param_( n in varchar2 ) return varchar2 is

        v varchar2( 4000 );

    begin

        v := sys_context( ctx, substr( n, 1, 30 ), 4000 );

        if ( v is null ) then

            --
            log_trc_( 'param_ did not find session level: ' || n );

            --
            select a.value into v
              from param$ a
             where a.name = n;

        else

            --
            log_trc_( 'param_ found session level: ' || n );

        end if;

        --
        log_trc_( 'param_( ' || n || ' ) returns: ' || nvl( v, '{null}' ) );
        return v;

        --
        exception
            when no_data_found then
                log_trc_( 'param_ did not find: ' || n || ' returning NULL' );
                return null;

            when others then
                log_err_( 'param_ error: ' || sqlerrm ); 
                raise;

    end param_;

    --
    function exist_( k in varchar2 ) return boolean is

        c number := none;

    begin

        select count(0) into c
          from filter$ a
         where a.key = k;

        log_trc_( 'exist_( ' || k || ' ) returns: ' 
               || case when ( c > none ) then 'true' else 'false' end );

        return ( c > none );

    end exist_;

    --
    function priv_( k in varchar2, a in varchar2 ) return boolean is

        c number := none;

    begin

        select count(0) into c
          from priv$ a
         where a.key = k
           and a.id# = oid( a );

        log_trc_( 'priv_( ' || k || ', ' || a || ' ) returns: ' 
               || case when ( c > none ) then 'true' else 'false' end );

        return ( c > none );

    end priv_;

    --
    function public_( k in varchar2, l in guard ) return boolean is

        g number := none;
        i number := oid( 'PUBLIC' );

    begin

        select a.lvl into g
          from priv$ a
         where a.key = k
           and id# = i;

        log_trc_( 'public_ determined grant: ' || to_char( g ) );

        log_trc_( 'public_( ' || k || ', ' || to_char( i ) || ' ) returns: ' 
               || case when ( bitand( g, l ) > 0 ) then 'true' else 'false' end );

        return ( bitand( g, l ) > 0 );

        exception
            when no_data_found then
                log_inf_( 'public_ did not find: ' || k || ', ' || to_char( i ) );
                return false;

            when others then
                log_err_( 'public_ error: ' || sqlerrm ); 
                raise;

    end public_;

    --
    function allowed_( k in varchar2, a in varchar2, l in guard ) return boolean is

        g number := none;
        i number := oid( a );

    begin

        if ( exist_( k ) ) then

            if ( public_( k, l ) ) then

                return true; -- public priv granted

            else

                if ( ( priv_( k, a ) ) and ( i is not null ) ) then

                    for rec in ( select a.lvl
                                   from priv$ a,
                                        ( select distinct
                                                 oid( b.granted_role ) rid,
                                                 oid( b.grantee ) gid
                                            from dba_role_privs b
                                         connect by b.grantee = prior b.granted_role
                                           start with 1 = case when ( instr( a, '"' ) > 0 )
                                                               then case when b.grantee = replace( a, '"', '' )
                                                                         then 1
                                                                         else 0
                                                                    end
                                                               else case when b.grantee = upper( a )
                                                                         then 1
                                                                         else 0
                                                                    end
                                                          end ) b
                                   where a.key = k and ( a.id# = b.rid or a.id# = b.gid ) ) loop

                        g := bitor( g, rec.lvl );

                    end loop;

                end if;

            end if;

        else

            g := priv_readwrite;    -- new filters always have read/write

        end if;

        log_trc_( 'allowed_ determined grant: ' || to_char( g ) );

        log_trc_( 'allowed_( ' || k || ', ' || a || ', ' || to_char( i ) || ' ) returns: ' 
               || case when ( bitand( g, l ) > 0 ) then 'true' else 'false' end );

        return ( bitand( g, l ) > 0 );

    end allowed_;

    --
    function get( key in varchar2 ) return binds is

        lst binds;
        k varchar2( 4000 ) := key;

    begin

        if ( allowed_( k, dbms_standard.login_user, priv_read ) ) then

            for rec in ( select a.value,
                                a.type,
                                a.scope
                           from filter$ a
                          where a.key = k ) loop

                append( rec.value, rec.type, rec.scope, lst );

            end loop;

        else

            raise_application_error( ec_denied, es_denied );

        end if;

        log_trc_( 'get returns ' || lst.count || ' bind(s) for key: ' || key );

        return lst;

        exception 
            when others then 
                log_err_( 'get ' || key || ', error: ' || sqlerrm );
                raise;

    end get;

    --
    function get( idx in number, lst in binds ) return bind is

        val bind;

    begin

        if ( lst.count >= idx ) then

            val := lst( idx );

        end if;

        if ( val is not null ) then

            log_trc_( 'get found index ' || to_char( idx ) || ' in binds list' );

        else

            log_trc_( 'get did not find index ' || to_char( idx ) || ' in binds list' );

        end if;

        return val;

        exception 
            when others then 
                log_err_( 'get index: ' || to_char( idx ) || ', error: ' || sqlerrm );
                raise;

    end get;

    --
    function count( key in varchar2 ) return number is

        c number := none;
        k varchar2( 4000 ) := key;

    begin

        if ( allowed_( key, dbms_standard.login_user, priv_read ) ) then

            select count(0) into c
              from filter$ a
             where a.key = k;

        else

            raise_application_error( ec_denied, es_denied );

        end if;

        log_trc_( 'count( ' || key || ' ) returns ' || to_char( c ) );

        return c;

        exception 
            when others then 
                log_err_( 'count: ' || key || ', error: ' || sqlerrm );
                raise;

    end count;

    --
    function count( lst in binds ) return number is
    begin

        log_trc_( 'count( <lst> ) returns ' || to_char( lst.count ) );
        return lst.count;

        exception 
            when others then 
                log_err_( 'count error: ' || sqlerrm );
                raise;

    end count;

    --
    function new( value in varchar2, 
                  type  in typeof    default type_string,
                  scope in reference default scope_in ) return bind is
    begin

        log_trc_( 'new( ' || value || ', ' || to_char( type ) || ', ' || to_char( scope ) || ' ) called' );
        return bind( value, type, scope );

        exception 
            when others then 
                log_err_( 'new value: ' || value || ', error: ' || sqlerrm );
                raise;

    end new;

    --
    procedure append( key   in varchar2,
                      value in varchar2, 
                      type  in typeof    default type_string,
                      scope in reference default scope_in ) is

        lst binds;

    begin

        log_trc_( 'append( ' || key || ', ' || value || ', ' || to_char( type ) || ', ' || to_char( scope ) || ' ) called' );
        lst := get( key );

        append( value, type, scope, lst );
        save( key, lst );

        exception 
            when others then 
                log_err_( 'append key: ' || key || ', error: ' || sqlerrm );
                raise;

    end append;

    --
    procedure append( value in     varchar2, 
                      type  in     typeof    default type_string,
                      scope in     reference default scope_in,
                      lst   in out binds ) is
    begin

        log_trc_( 'append( ' || value || ', ' || to_char( type ) || ', ' || to_char( scope ) || ', <lst> ) called' );
        append( bind( value, type, scope ), lst );

        exception 
            when others then 
                log_err_( 'append value: ' || value || ', error: ' || sqlerrm );
                raise;

    end append;

    --
    procedure append( key in varchar2, val in bind ) is

        lst binds;

    begin

        log_trc_( 'append( ' || key || ', <val> ) called' );
        lst := get( key );

        append( val, lst );
        save( key, lst );

        exception 
            when others then 
                log_err_( 'append key: ' || key || ', error: ' || sqlerrm );
                raise;

    end append;

    --
    procedure append( val in bind, lst in out binds ) is
    begin

        log_trc_( 'append( <val>, <lst> ) called' );
        if ( lst is null ) then

            lst := binds();

        end if;

        lst.extend;
        lst( lst.count ) := val;

        exception 
            when others then 
                log_err_( 'append <val>, error: ' || sqlerrm );
                raise;

    end append;

    --
    procedure append( key in varchar2, val in binds ) is

        lst binds;

    begin

        log_trc_( 'append( ' || key || ', <val> ) called' );
        lst := get( key );

        append( val, lst );
        save( key, lst );

        exception 
            when others then 
                log_err_( 'append key: ' || key || ', error: ' || sqlerrm );
                raise;

    end append;

    --
    procedure append( val in binds, lst in out binds ) is
    begin

        log_trc_( 'append( <val>, <lst> ) called' );

        for i in 1 .. val.count loop

            append( val( i ), lst );

        end loop;

        exception 
            when others then 
                log_err_( 'append <val>, error: ' || sqlerrm );
                raise;

    end append;

    --
    procedure change( key   in varchar2,
                      idx   in number,
                      value in varchar2, 
                      type  in typeof    default type_string,
                      scope in reference default scope_in ) is

        lst binds;

    begin

        log_trc_( 'change( ' || key || ', ' 
                             || to_char( idx ) || ', ' 
                             || value || ', ' 
                             || to_char( type ) || ', ' 
                             || to_char( scope ) || ' ) called' );

        lst := get( key );

        if ( lst.count >= idx ) then

            lst( idx ).value := value;
            lst( idx ).type  := type;
            lst( idx ).scope := scope;

        else

            append( value, type, scope, lst );

        end if;

        save( key, lst );

        exception 
            when others then 
                log_err_( 'change ' || key || ', ' 
                                    || to_char( idx ) || ', ' 
                                    || value || ', ' 
                                    || to_char( type ) || ', ' 
                                    || to_char( scope ) || ', error: ' || sqlerrm );
                raise;

    end change;

    --
    procedure remove( key in varchar2,
                      idx in number ) is

        lst binds;
        val binds := binds();

    begin

        log_trc_( 'remove( ' || key || ', ' 
                             || to_char( idx ) || ' ) called' );

        lst := get( key );

        if ( lst.count >= idx ) then

            for i in 1 .. lst.count loop

                if ( i != idx ) then

                    val.extend;
                    val( val.count ) := lst( i );

                end if;

            end loop;

        else

            val := lst;

        end if;

        save( key, val );

        exception 
            when others then 
                log_err_( 'remove ' || key || ', ' 
                                    || to_char( idx ) 
                                    || ', error: ' || sqlerrm );
                raise;

    end remove;

    --
    procedure replace( val in binds, lst in out binds ) is

        rpl binds;

    begin

        log_trc_( 'replace( <val>, <lst> )' );

        for i in 1 .. val.count loop

            append( val( i ), rpl );

        end loop;

        lst := rpl;

        exception 
            when others then 
                log_err_( 'replace, error: ' || sqlerrm );
                raise;

    end replace;

    --
    procedure clear( key in varchar2 ) is

        pragma autonomous_transaction;
        k varchar2( 4000 ) := key;

    begin

        log_trc_( 'clear( ' || key || ' ) called' );

        if ( allowed_( key, dbms_standard.login_user, priv_write ) ) then

            --
            delete from filter$ a
             where a.key = k;

            --
            delete from priv$ a
             where a.key = k;

            commit;

        else

            raise_application_error( ec_denied, es_denied );

        end if;

        exception
            when others then
                rollback;
                log_err_( 'clear key: ' || key || ', error: ' || sqlerrm );
                raise;

    end clear;

    --
    procedure clear( lst in out binds ) is
    begin

        log_trc_( 'clear( <lst> ) called' );
        lst.delete;

        exception 
            when others then 
                log_err_( 'clear, error: ' || sqlerrm );
                raise;

    end clear;

    --
    procedure allow( key in varchar2,
                     act in varchar2,
                     lvl in guard default priv_readwrite ) is

        pragma autonomous_transaction;

        i number := none;
        k varchar2( 4000 ) := key;
        a varchar2( 4000 ) := act;
        l number           := lvl;

    begin

        log_trc_( 'allow( ' || key || ', '
                            || act || ', '
                            || to_char( lvl ) || ' ) called' );

        if ( allowed_( k, dbms_standard.login_user, priv_write ) ) then

            if ( l > none ) then

                i := oid( a );

                if ( i is not null ) then

                    if ( not priv_( k, a ) ) then

                        log_trc_( 'allow inserted new item: ' || key || ', '
                                                              || act || ', '
                                                              || to_char( lvl ) );

                        insert into priv$ a
                        (
                            a.key,
                            a.id#,
                            a.lvl
                        )
                        values
                        (
                            k,
                            i,
                            l
                        );

                    else

                        log_trc_( 'allow updated existing item: ' || key || ', '
                                                                  || act || ', '
                                                                  || to_char( lvl ) );

                        update priv$ a
                           set a.lvl = l
                         where a.key = k
                           and a.id# = i;

                    end if;

                    commit;

                end if;

            else

                deny( k, a );

            end if;

        else

            log_wrn_( 'allow denied access: ' || key || ', '
                                              || act );

            raise_application_error( ec_denied, es_denied );

        end if;

        exception
            when others then
                rollback;
                log_err_( 'allow key: ' || key || ', ' || act || ', error: ' || sqlerrm );
                raise;

    end allow;

    --
    procedure deny( key in varchar2,
                    act in varchar2 ) is

        pragma autonomous_transaction;
        i number := none;
        k varchar2( 4000 ) := key;
        a varchar2( 4000 ) := act;

    begin

        log_trc_( 'deny( ' || key || ', '
                           || act || ' ) called' );

        if ( allowed_( k, dbms_standard.login_user, priv_write ) ) then

            if ( priv_( k, act ) ) then

                i := oid( a );

                if ( i is not null ) then

                    log_trc_( 'deny removed existing item:  ' || key || ', '
                                                              || act );

                    delete from priv$ a
                     where a.key = k
                       and a.id# = i;

                    commit;

                end if;

            else

                log_inf_( 'deny found not grant for: ' || key || ', '
                                                       || act );

                raise_application_error( ec_no_grant, es_no_grant );

            end if;

        else

            log_wrn_( 'deny denied access: ' || key || ', '
                                             || act );

            raise_application_error( ec_denied, es_denied );

        end if;

        exception
            when others then
                rollback; 
                log_err_( 'deny key: ' || key || ', ' || act || ', error: ' || sqlerrm );
                raise;

    end deny;

    --
    procedure save( key in varchar2, lst in binds ) is

        pragma autonomous_transaction;
        c number := 0;
        k varchar2( 4000 ) := key;

    begin

        log_trc_( 'save( ' || key || ', <lst> ) called' );

        --
        select count(0) into c
          from filter$ a
         where a.key = k;

        if ( c > 0 ) then

            if ( not allowed_( k, dbms_standard.login_user, priv_write ) ) then

                log_wrn_( 'save denied access: ' || key || ', '
                                                 || dbms_standard.login_user );

                raise_application_error( ec_denied, es_denied );

            end if;

        end if;

        --
        delete from filter$ a
         where a.key = k;

        log_inf_( 'save removed ' || to_char( sql%rowcount ) || ' row(s) for ' || key );

        --
        log_trc_( 'save processing ' || to_char( lst.count ) || ' bind(s) for ' || key );

        for i in 1 .. lst.count loop

            insert into filter$ a
            (
                a.key,
                a.seq,
                a.value,
                a.type,
                a.scope
            )
            values
            (
                k,
                i,
                lst( i ).value,
                nvl( lst( i ).type, none ),
                nvl( lst( i ).scope, none )
            );

            if ( i = 1 ) then

                declare

                    n number := none;
                    b varchar2( 4000 );

                    t number := 0;
                    o varchar2( 4000 ) := null;

                    a varchar2( 4000 );
                    p number := 0;

                begin

                    b := param_( 'default_bind_access' );

                    if ( b is not null ) then

                        log_trc_( 'save adding default access to: ' || b );

                        --
                        while true loop

                            --
                            t := t + 1;
                            o := token_( b, t );

                            --
                            exit when o is null;

                            a := token_( o, 1, ':' );
                            p := nvl( to_number( token_( o, 2, ':' ) ), priv_read );

                            --
                            select oid( replace( a, '%user%', dbms_standard.login_user ) ) into n
                              from dual;

                            --
                            if ( n is not null ) then

                                begin

                                    insert into priv$ a
                                    (
                                        a.key,
                                        a.id#,
                                        a.lvl
                                    )
                                    values
                                    (
                                        k,
                                        n,
                                        p
                                    );

                                    log_trc_( 'save inserted privilege: ' || k || ', ' 
                                                                          || to_char( n ) || ', ' 
                                                                          || to_char( p ) );

                                    exception
                                        when dup_val_on_index then

                                            log_inf_( 'save privilege exists: ' || k || ', ' 
                                                                                || to_char( n ) || ', ' 
                                                                                || to_char( p ) );
                                            null;

                                end;

                            end if;

                        end loop;

                    end if;

                    -- assign ownership to creator, if not already privliged
                    if ( not priv_( k, dbms_standard.login_user ) ) then

                        log_inf_( 'save assigning ownership: ' || k || ', ' 
                                                               || dbms_standard.login_user );

                        n := oid( null );

                        if ( n is not null ) then

                            begin

                                insert into priv$ a
                                (
                                    a.key,
                                    a.id#,
                                    a.lvl
                                )
                                values
                                (
                                    k,
                                    n,
                                    priv_readwrite
                                );

                                log_trc_( 'save inserted ownership: ' || k || ', ' 
                                                                      || to_char( n ) || ', ' 
                                                                      || to_char( priv_readwrite ) );

                                exception
                                    when dup_val_on_index then

                                        log_inf_( 'save ownership exists: ' || k || ', ' 
                                                                            || to_char( n ) || ', ' 
                                                                            || to_char( priv_readwrite ) );
                                        null;

                            end;

                        end if;

                    end if;

                end;

            end if;

        end loop;

        commit;

        exception
            when others then
                rollback;
                log_err_( 'save key ' || key || ' falied: ' || sqlerrm );
                raise;

    end save;

end binding;
/

show errors

--
-- ... done!
--
