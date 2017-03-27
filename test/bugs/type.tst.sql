--
alter session set current_schema = hive;

--
create or replace type bar as object
(
    a varchar2( 4000 ),
    b number,
    c number,

    constructor function bar( x varchar2, y number ) return self as result,
    constructor function bar( x varchar2 ) return self as result
);
/

show errors

--
create or replace type body bar as

    constructor function bar( x varchar2, y number ) return self as result is
    begin

        self.a := x;
        self.b := y;

        self.c := 2;

        return;

    end bar;

    constructor function bar( x varchar2 ) return self as result is
    begin

        self.a := x;

        self.b := 1;
        self.c := 2;

        return;

    end bar;

end;
/

show errors

--
create or replace type foo as table of bar;
/

show errors

--
create or replace function tst( n in number ) return foo as

    f foo := foo();

begin

    for i in 1 .. n loop

        f.extend;
        f( f.count ) := bar( sys_guid() );

    end loop;

    return f;

end tst;
/

show errors

--
create or replace procedure tst2( n in number ) as

    f foo := tst( n );

begin

    for i in 1 .. f.count loop

        dbms_output.put_line( to_char( i )
                           || ': a=' || f( i ).a
                           || ', b=' || f( i ).b
                           || ', b=' || f( i ).c );

    end loop;

end tst2;
/

show errors

--
select foo( bar( 'a', 1, 2 ),
            bar( 'b', 3, 4 ),
            bar( 'c', 5, 6 ) ) from dual;

--
select tst( 3 ) from dual;

--
set serveroutput on
exec tst2( 7 );

--
drop type foo;
drop type bar;
drop function tst;
drop procedure tst2;

exit
