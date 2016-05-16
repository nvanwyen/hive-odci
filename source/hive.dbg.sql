--------------------------------------------------------------------------------
--
-- 2016-05-12, NV - hive.dbg.sql
--

--
prompt ... running hive.dbg.sql

prompt
prompt !!! WARNING !!! ; Debug information being included in HIVE
prompt 

--
alter session set current_schema = hive;

--
drop table dbg$log$;
drop table dbg$record$;
drop table dbg$attribute$;
drop table dbg$info$elem$;
drop table dbg$info$;

--
create table dbg$log$
(
    stamp   timestamp,
    txt     varchar2( 4000 ),
    msg     varchar2( 4000 )
)
/

--
create table dbg$record$
(
    txt           varchar2( 4000 ),
    key           number,
    num           number,
    idx           number,
    code          number,
    val_varchar2  varchar2( 4000 ),
    val_number    number,
    val_date      date,
    val_timestamp timestamp,
    val_clob      clob,
    val_blob      blob
)
/

create table dbg$attribute$
(
    txt           varchar2( 4000 ),
    idx           number,
    name          varchar2( 256 ),
    code          number,
    prec          number,
    scale         number,
    len           number,
    csid          number,
    csfrm         number
)
/

create table dbg$info$elem$
(
    txt           varchar2( 4000 ),
    code          number,
    idx           number,
    prec          number,
    scale         number,
    len           number,
    csid          number,
    csfrm         number,
    attr_type     anytype,
    attr_name     varchar2( 256 )
)
/

create table dbg$info$
(
    txt           varchar2( 4000 ),
    code          number,
    prec          number,
    scale         number,
    len           number,
    csid          number,
    csfrm         number,
    schema_name   varchar2( 30 ),
    type_name     varchar2( 30 ),
    version       varchar2( 30 ),
    attr_count    number
)
/

--
create or replace procedure debug_clear is

    pragma autonomous_transaction;

begin

    delete from dbg$record$;
    delete from dbg$attribute$;
    delete from dbg$info$elem$;
    delete from dbg$info$;

    commit;

end debug_clear;
/

show errors

--
create or replace procedure debug_log( txt in varchar2,
                                       msg in varchar2 ) is

    pragma autonomous_transaction;

begin

    insert into dbg$log$ a
    (
        a.stamp,
        a.txt,
        a.msg
    )
    values
    (
        current_timestamp,
        txt,
        msg
    );

    commit;

    exception
        when others then rollback;

end debug_log;
/

show errors

--
create or replace procedure debug_records( txt in varchar2,
                                           key in number,
                                           num in number,
                                           rec in records ) as

    pragma autonomous_transaction;

begin

    if ( rec.count > 0 ) then

        for i in 1 .. rec.count loop

            insert into dbg$record$ r
            (
                r.txt,
                r.key,
                r.num,
                r.idx,
                r.code,
                r.val_varchar2,
                r.val_number,
                r.val_date,
                r.val_timestamp,
                r.val_clob,
                r.val_blob
            )
            values
            (
                txt,
                key,
                num,
                i,
                rec( i ).code,
                rec( i ).val_varchar2,
                rec( i ).val_number,
                rec( i ).val_date,
                rec( i ).val_timestamp,
                rec( i ).val_clob,
                rec( i ).val_blob
            );

            commit;

        end loop;

    end if;

    exception
        when others then rollback;

end debug_records;
/

show errors


create or replace procedure debug_attributes( txt in varchar2,
                                              atr in attributes ) as

    pragma autonomous_transaction;

begin

    if ( atr.count > 0 ) then

        for i in 1 .. atr.count loop

            insert into dbg$attribute$ a
            (
                a.txt,
                a.idx,
                a.name,
                a.code,
                a.prec,
                a.scale,
                a.len,
                a.csid,
                a.csfrm
            )
            values
            (
                txt,
                i,
                atr( i ).name,
                atr( i ).code,
                atr( i ).prec,
                atr( i ).scale,
                atr( i ).len,
                atr( i ).csid,
                atr( i ).csfrm
            );

            commit;

        end loop;

    end if;


    exception
        when others then rollback;

end debug_attributes;
/

show errors

--
create or replace procedure debug_info_elem( txt in varchar2,
                                             idx in number,
                                             typ in anytype ) is

    pragma autonomous_transaction;

    code        number;
    prec        number;
    scale       number;
    len         number;
    csid        number;
    csfrm       number;
    attr_type   anytype;
    attr_name   varchar2( 256 );

begin

        code := typ.getattreleminfo( idx,
                                     prec,
                                     scale,
                                     len,
                                     csid,
                                     csfrm,
                                     attr_type,
                                     attr_name );

        insert into dbg$info$elem$ a
        (
            a.txt,
            a.code,
            a.idx,
            a.prec,
            a.scale,
            a.len,
            a.csid,
            a.csfrm,
            a.attr_type,
            a.attr_name
        )
        values
        (
            txt,
            code,
            idx,
            prec,
            scale,
            len,
            csid,
            csfrm,
            attr_type,
            attr_name
        );

        commit;

    exception
        when others then
            rollback;

end debug_info_elem;
/

show errors

--
create or replace procedure debug_info( txt in varchar2,
                                        typ in anytype,
                                        atr in boolean default true ) is

    pragma autonomous_transaction;

    code        number;
    prec        number;
    scale       number;
    len         number;
    csid        number;
    csfrm       number;
    schema_name varchar2( 30 );
    type_name   varchar2( 30 );
    version     varchar2( 30 );
    attr_count  number;

begin

    code := typ.getinfo( prec,
                         scale,
                         len,
                         csid,
                         csfrm,
                         schema_name,
                         type_name,
                         version,
                         attr_count );

    insert into dbg$info$ a
    (
        a.txt,
        a.code,
        a.prec,
        a.scale,
        a.len,
        a.csid,
        a.csfrm,
        a.schema_name,
        a.type_name,
        a.version,
        a.attr_count
    )
    values
    (
        txt,
        code,
        prec,
        scale,
        len,
        csid,
        csfrm,
        schema_name,
        type_name,
        version,
        attr_count
    );

    commit;

    --
    if ( atr ) then

        if ( attr_count > 0 ) then

            for idx in 1 .. attr_count loop

                debug_info_elem( txt, idx, typ );

            end loop;

        end if;

    end if;

    exception
        when others then
            rollback;

end debug_info;
/

show errors
