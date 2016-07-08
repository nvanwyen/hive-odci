--------------------------------------------------------------------------------
--
-- 2016-04-7, NV - bind.typ.sql
--

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

end binding;
/

show errors

--
create or replace package body binding as

    --
    function priv_( k in varchar2, a in varchar2 ) return boolean is

        c number := none;

    begin

        select count(0) into c
          from priv$ a
         where a.key = k
           and a.id# = oid( a );

        return ( c > none );

    end priv_;

    --
    function allowed_( k in varchar2, a in varchar2, l in guard ) return boolean is

        g number := none;
        i number := oid( a );

    begin

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
                           where a.id# = b.rid or a.id# = b.gid ) loop

                g := bitor( g, rec.lvl );

            end loop;

        end if;

        return ( bitand( g, l ) > 0 );

    end allowed_;

    --
    function get( key in varchar2 ) return binds is

        lst binds;

    begin

        if ( allowed_( key, dbms_standard.login_user, priv_read ) ) then

            for rec in ( select a.value,
                                a.type,
                                a.scope
                           from filter$ a
                          where a.key = key ) loop

                append( rec.value, rec.type, rec.scope, lst );

            end loop;

        else

            raise_application_error( ec_denied, es_denied );

        end if;

        return lst;

    end get;

    --
    function get( idx in number, lst in binds ) return bind is

        val bind;

    begin

        if ( lst.count >= idx ) then

            val := lst( idx );

        end if;

        return val;

    end get;

    --
    function count( key in varchar2 ) return number is

        c number := none;

    begin

        if ( allowed_( key, dbms_standard.login_user, priv_read ) ) then

            select count(0) into c
              from filter$ a
             where a.key = key;

        else

            raise_application_error( ec_denied, es_denied );

        end if;

        return c;

    end count;

    --
    function count( lst in binds ) return number is
    begin

        return lst.count;

    end count;

    --
    function new( value in varchar2, 
                  type  in typeof    default type_string,
                  scope in reference default scope_in ) return bind is
    begin

        return bind( value, type, scope );

    end new;

    --
    procedure append( key   in varchar2,
                      value in varchar2, 
                      type  in typeof    default type_string,
                      scope in reference default scope_in ) is

        lst binds;

    begin

        lst := get( key );

        append( value, type, scope, lst );
        save( key, lst );

    end append;

    --
    procedure append( value in     varchar2, 
                      type  in     typeof    default type_string,
                      scope in     reference default scope_in,
                      lst   in out binds ) is
    begin

        append( bind( value, type, scope ), lst );

    end append;

    --
    procedure append( key in varchar2, val in bind ) is

        lst binds;

    begin

        lst := get( key );

        append( val, lst );
        save( key, lst );

    end append;

    --
    procedure append( val in bind, lst in out binds ) is
    begin

        if ( lst is null ) then

            lst := binds();

        end if;

        lst.extend;
        lst( lst.count ) := val;

    end append;

    --
    procedure append( key in varchar2, val in binds ) is

        lst binds;

    begin

        lst := get( key );

        append( val, lst );
        save( key, lst );

    end append;

    --
    procedure append( val in binds, lst in out binds ) is
    begin

        for i in 1 .. val.count loop

            append( val( i ), lst );

        end loop;

    end append;

    --
    procedure change( key   in varchar2,
                      idx   in number,
                      value in varchar2, 
                      type  in typeof    default type_string,
                      scope in reference default scope_in ) is

        lst binds;

    begin

        lst := get( key );

        if ( lst.count >= idx ) then

            lst( idx ).value := value;
            lst( idx ).type  := type;
            lst( idx ).scope := scope;

        else

            append( value, type, scope, lst );

        end if;

        save( key, lst );

    end change;

    --
    procedure remove( key in varchar2,
                      idx in number ) is

        lst binds;
        val binds := binds();

    begin

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

    end remove;

    --
    procedure replace( val in binds, lst in out binds ) is

        rpl binds;

    begin

        for i in 1 .. val.count loop

            append( val( i ), rpl );

        end loop;

        lst := rpl;

    end replace;

    --
    procedure clear( key in varchar2 ) is

        pragma autonomous_transaction;

    begin

        if ( allowed_( key, dbms_standard.login_user, priv_write ) ) then

            --
            delete from filter$ a
             where a.key = key;

            --
            delete from priv$ a
             where a.key = key;

            commit;

        else

            raise_application_error( ec_denied, es_denied );

        end if;

        exception
            when others then
                rollback;
                raise;

    end clear;

    --
    procedure clear( lst in out binds ) is
    begin

        lst.delete;

    end clear;

    --
    procedure allow( key in varchar2,
                     act in varchar2,
                     lvl in guard default priv_readwrite ) is

        pragma autonomous_transaction;
        id number := none;

    begin

        if ( allowed_( key, dbms_standard.login_user, priv_write ) ) then

            if ( lvl > none ) then

                id := oid( act );

                if ( id is not null ) then

                    if ( not priv_( key, act ) ) then

                        insert into priv$ a
                        (
                            a.key,
                            a.id#,
                            a.lvl
                        )
                        values
                        (
                            key,
                            id,
                            lvl
                        );

                    else

                        update priv$ a
                           set a.lvl = lvl
                         where a.key = key
                           and a.id# = id;

                    end if;

                    commit;

                end if;

            else

                deny( key, act );

            end if;

        else

            raise_application_error( ec_denied, es_denied );

        end if;

        exception
            when others then
                rollback; raise;

    end allow;

    --
    procedure deny( key in varchar2,
                    act in varchar2 ) is

        pragma autonomous_transaction;
        id number := none;

    begin

        if ( allowed_( key, dbms_standard.login_user, priv_write ) ) then

            if ( not priv_( key, act ) ) then

                id := oid( act );

                if ( id is not null ) then

                    delete from priv$ a
                     where a.key = key
                       and a.id# = id;

                    commit;

                end if;

            end if;

        else

            raise_application_error( ec_denied, es_denied );

        end if;

        exception
            when others then
                rollback; raise;

    end deny;

    --
    procedure save( key in varchar2, lst in binds ) is

        pragma autonomous_transaction;
        c number := 0;

    begin

        --
        select count(0) into c
          from filter$ a
         where a.key = key;

        if ( c > 0 ) then

            if ( not allowed_( key, dbms_standard.login_user, priv_write ) ) then

                raise_application_error( ec_denied, es_denied );

            end if;

        end if;

        --
        delete from filter$ a
         where a.key = key;

        --
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
                key,
                i,
                lst( i ).value,
                nvl( lst( i ).type, none ),
                nvl( lst( i ).scope, none )
            );

            if ( i = 1 ) then

                declare

                    id number := none;

                begin

                    if ( not priv_( key, dbms_standard.login_user ) ) then

                        id := oid( null );

                        if ( id is not null ) then

                            insert into priv$ a
                            (
                                a.key,
                                a.id#,
                                a.lvl
                            )
                            values
                            (
                                key,
                                id,
                                priv_readwrite
                            );

                        end if;

                    end if;

                end;

            end if;

        end loop;

        commit;

        exception
            when others then
                rollback;
                raise;

    end save;

end binding;
/

show errors

--
-- ... done!
--
