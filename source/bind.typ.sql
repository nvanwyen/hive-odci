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

    --
    unknown         constant number    := 0;

    --
    scope_in        constant reference := 1;
    scope_out       constant reference := 2;
    scope_inout     constant reference := 3;

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
    procedure save( key in varchar2, lst in binds );

end binding;
/

show errors

--
create or replace package body binding as

    --
    function get( key in varchar2 ) return binds is

        lst binds;

    begin

        for rec in ( select a.value,
                            a.type,
                            a.scope
                       from filter$ a
                      where a.key = key ) loop

            append( rec.value, rec.type, rec.scope, lst );

        end loop;

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

        c number := 0;

    begin

        select count(0) into c
          from filter$ a
         where a.key = key;

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

        --
        delete from filter$ a
         where a.key = key;

        commit;

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
    procedure save( key in varchar2, lst in binds ) is

        pragma autonomous_transaction;

    begin

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
                nvl( lst( i ).type, unknown ),
                nvl( lst( i ).scope, unknown )
            );

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
