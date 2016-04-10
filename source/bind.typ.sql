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
    value   varchar2( 4000 ),
    type    number
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
    subtype reference is integer;
    subtype typeof    is integer;

    --
    unknown   constant integer   := 0;

    --
    ref_in    constant reference := 1;
    ref_out   constant reference := 2;
    ref_inout constant reference := 3;

    --
    type_bool       constant typeof :=  1;
    type_date       constant typeof :=  2;
    type_float      constant typeof :=  3;
    type_int        constant typeof :=  4;
    type_long       constant typeof :=  5;
    type_null       constant typeof :=  6;
    type_rowid      constant typeof :=  7;
    type_short      constant typeof :=  8;
    type_string     constant typeof :=  9;
    type_time       constant typeof := 10;
    type_timestamp  constant typeof := 11;
    type_url        constant typeof := 12;

    --
    function get return binds;
    function get( idx in number ) return bind;

    --
    function count return number;

    --
    function new( value in varchar2, 
                  type  in typeof default type_string ) return bind;

    --
    function append( value in varchar2, 
                     type  in typeof default type_string ) return binds;

    --
    function append( obj in bind ) return binds;

    --
    function append( lst in binds ) return binds;

    --
    function replace( lst in binds ) return binds;

    --
    function load( key in varchar2 ) return binds;
    function extend( key in varchar2 ) return binds;

    --
    procedure save( key in varchar2 );

    --
    procedure clear;
    procedure purge( key in varchar2, clean in boolean default false );

end binding;
/

show errors

--
create or replace package body binding as

    -- session binds
    list binds := binds();

    --
    ctx constant varchar2( 30 ) := 'filter';

    --
    function get return binds is
    begin

        return list;

    end get;

    --
    function get( idx in number ) return bind is

        bnd bind;

    begin

        --
        if ( ( idx > 0 ) and ( list.count >= idx ) ) then

            bnd := list( idx );

        end if;

        --
        return bnd;

    end get;

    --
    function count return number is
    begin

        return list.count;

    end count;

    --
    function new( value in varchar2, 
                  type  in typeof default type_string ) return bind is
    begin

        --
        return bind( value, type );

    end new;

    --
    function append( value in varchar2, 
                     type  in typeof default type_string ) return binds is
    begin

        return append( new( value, type ) );

    end append;

    --
    function append( obj in bind ) return binds is
    begin

        list.extend( 1 );
        list( list.count ) := obj;

        return get;

    end append;

    --
    function append( lst in binds ) return binds is
    begin

        for i in 1 .. lst.count loop

            list.extend( 1 );
            list( list.count ) := lst( i );

        end loop;

        return get;

    end append;

    --
    function replace( lst in binds ) return binds is
    begin

        list.delete;
        return append( lst );

    end replace;

    --
    function load( key in varchar2 ) return binds is

        lst binds := binds();

    begin

        --
        for rec in ( select a.value,
                            a.type
                       from filter$ a
                      where a.key = key
                      order by seq asc ) loop

            lst.extend( 1 );
            lst( lst.count ) := bind( rec.value, rec.type );

        end loop;

        --
        return replace( lst );

    end load;

    --
    function extend( key in varchar2 ) return binds is
    begin

        --
        for rec in ( select a.value,
                            a.type
                       from filter$ a
                      where a.key = key
                      order by seq asc ) loop

            list.extend( 1 );
            list( list.count ) := bind( rec.value, rec.type );

        end loop;

        return get;

    end extend;

    --
    procedure save( key in varchar2 ) is

         pragma autonomous_transaction;

    begin

        --
        delete filter$ a where a.key = key;

        --
        for i in 1 .. list.count loop

            --
            insert into filter$ a ( a.key, a.seq, a.value, a.type )
            values ( key, i, list( i ).value, list( i ).type );

        end loop;

        --
        commit;

        exception
            when others then
                rollback;
                raise;

    end save;

    --
    procedure clear is
    begin

        list.delete;

    end clear;

    --
    procedure purge( key in varchar2, clean in boolean default false ) is

         pragma autonomous_transaction;

    begin

        --
        delete filter$ a where a.key = key;

        --
        commit;

        --
        if ( clean ) then clear; end if;

        exception
            when others then
                rollback;
                raise;

    end purge;

end binding;
/

show errors

--
create or replace context filter using binding;

--
-- ... done!
--
