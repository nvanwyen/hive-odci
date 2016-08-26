Hive-ODCI - Users Guide
=======================

Hive-ODCI is an [Oracle Data Cartridge Interface][0] for dynamically accessing 
Hadoop/Hive data-stores through an Oracle 12c database. In other words 
Hive-ODCI makes Hadoop/Hive tables accessible as first-class, native, objects 
directly using PL/SQL, SQL, VIEWS, DML, DDL, etc.... in an Oracle 12c database.

----------

Author
------------------------------
Metasystem Technologies Inc. (MTI)
[www.mtihq.com][1]  

Nicholas Van Wyen
nvanwyen@mtihq.com

License
------------------------------
**Copyright (c) 2006 - 2016 Nicholas Van Wyen, MTI**
**All rights reserved.**

> Redistribution and use in source and binary forms, with or without
> modification, are permitted provided that the following conditions
> are met:
>> 1. Redistributions of source code must retain the above copyright
>> notice, this list of conditions and the following disclaimer.
>> 2. Redistributions in binary form must reproduce the above copyright
>> notice, this list of conditions and the following disclaimer in the
>> documentation and/or other materials provided with the distribution.
>> 3. The name of the author may not be used to endorse or promote products
>> derived from this software without specific prior written permission.

> THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
> IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
> OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
> IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
> INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
> NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
> DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
> THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
> (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
> THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Releases
------------------------------
All releases can be found on Github 
https://github.com/nvanwyen/hive-odci/releases, along with the [latest 
release][2] release.

The project home is publicly available on Github at 
https://github.com/nvanwyen/hive-odci

Installation and Removal
------------------------------
See INSTALL.md for instructions

Concepts
------------------------------
Hive-ODCI is a pass-through interface allowing SQL access from within
an Oracle RDBMS to access information in an external Hive/Hadoop
data-store. Hive-ODCI provides PL/SQL interfaces using ODCI to
accomplish this functionality, making the access viable as native
object in Oracle.

Hive-ODCI is accessed via the ```HIVE``` schema, and controlled through
RBAC permissions in Oracle. A client (user or application) is granted
privileges through the ```HIVE_USER``` role or direct system privileges
by the DBA.

The client uses the PL/SQL objects to query, or execute DML/DDL in the
remote Hive datastore, using the ```HIVE_T`` object type or one of the
PL/SQL packages.

The ```HIVE``` schema installs Java classes in the database, which
perform the JDBC execution on the clients behalf, and ```PIPLINED```
the results back through the calling interface. 

The client can dictate most levels of functionality at run-time, with
predefined session data, bind-variables, etc... as customization for
each call.

Take for example the following concepts. The Hive/Hadoop data-store
is remotely accessible on a separate server. The Hive-ODCI Java
classes access the remote data via the JDBC Driver loaded during
installation.

The client accesses the Hive-ODCI interface using the PL/SQL objects
provided and/or a first-class ```VIEW```, controlled by RBAC, to 
access the data for its community.

```
................................................................................
.                                                                              .
.                              +--------+       +--------+                     .
.                              |  Hive  |------>| Hadoop |                     .
.                              +--------+       +--------+                     .
.       server                      |                                          .
.       ----------------------------|-----------------------------------       .
.       jvm                         |     jdbc                       --+       .
.                              +--------+                              |       .
.                              |  hive  |                              |       .
.                              +--------+                              |       .
.                                   |                                  |       .
.       ----------------------------|--------------------------------- | d     .
.       pl/sql                      |                                  | a     .
.                                   |                                  | t     .
.              +---------+     +--------+     +---------+              | a     .
.              | session | --- | hive_t | --- | binding |              | b     .
.              +---------+     +--------+     +---------+              | a     .
.                                  | |                                 | s     .
.       ---------------------------|-|-------------------------------  | e     .
.       sql                        | |                                 |       .
.             ...... HIVE ......   | |   ...... SCHEMA ......          |       .
.                              .   | |   .                             |       .
.                 +--------+   .   | |   .    +--------+               |       .
.                 | hive_q |-------+ +--------|  view  |               |       .
.                 +--------+   .         .    +--------+               |       .
.                     |        .         .        |                  --+       .
.       --------------|---------------------------|------ SELECT -------       .
.       client        |        +---------+        |                            .   
.                     +--------|   app   |--------+                            .
.                              +---------+                                     .
.                                                                              .
................................................................................
```


Tutorial
------------------------------
Because the majority of us *learn by example*, let's jump right into
a tutorial of how to use Hive-ODCI. All information provided here,
such as objects, packages, procedure, roles, etc... are detailed 
below so feel free to jump back-and forth for references when you
encounter something you don't understand.

## Scenario
Let's define a real-world scenario, where we have an Oracle database
that has multiple Petabytes of data, some of which is *static*, 
meaning it changes very little if at all once data has been added.
We'll call ours the ```USER_LOG``` which exists in the ```SCOTT```
schema. This table has billions upon billions of records consuming,
multiple GB of storage space, which has been collected over the years,
logging the activity metrics of our user community. Data over 30 days
old is used in monthly reporting, but never changes once it's been
added to the table.

Our tables looks like this ...
```
    SQL> desc SCOTT.USER_LOG

     Name           Null?    Type
     -------------- -------- ---------------
     STAMP          NOT NULL DATE
     ACCOUNT                 VARCHAR2(30)
     MESSAGE                 VARCHAR2(4000)
```
Because we have tons of room available in our Hadoop cluster we decide
that we want to move the data there, so it can be indexed and searched.
But hold on, we have a problem, we still have an application that reads
the table and creates reports for upper management and they are not
going to change their application to read from 2 different places using
2 different methods (oh, what to do).

### Hive-ODCI to the rescue
The reporting application also contains PL/SQL to create the reports
and has a VIEW used in displaying the details of the report generated.

They look something like this ...
```
    --
    procedure user_log_report( p_report out xmltype ) is
    begin
    
        for rec in ( select account,
                            message
                       from scott.user_log
                      order by account ) loop

            if ( rec.stamp > sysdate - 90 ) then
    
                p_report := ... -- do something special

            else

                if ( rec.account = user ) then

                    p_report := ... -- if current user then ...
    
                end if;

            end if;

            if ( rec.message like '%ABC%' ) then

                p_report := ... -- write a particular format

            else
  
                p_report := ... -- write another format

            end if;

        end loop;
    
    end user_log_report;

    --
    view user_log_monthly
    as
    select stamp,
           account,
           message
      from scott.user_log
     where stamp between sysdate - 30
                     and sysdate;
```

Assume we have already moved our data over to Haddop and created a
Hive table, of the same name. Our remote table looks like this ...
```
    $ beeline -u jdbc:hive2://hive.corp.com:10000 \
              -n oracle \
              -w welcome1.passwd
    
    0: jdbc:hive2://localhost:10000> desc user_log;
    +-------------------------+------------+----------+--+
    |        col_name         | data_type  | comment  |
    +-------------------------+------------+----------+--+
    | stamp                   | date       |          |
    | account                 | string     |          |
    | message                 | string     |          |
    +-------------------------+------------+----------+--+
    3 rows selected (0.17 seconds)

```

#### Hive-ODCI configuration
Now that we have our table in Hadoop/Hive and we can connect via
beeline, and see it there, let's setup Hive-ODCI as an access point
to that table.

Because all of our clients will be accessing the same Hadoop/Hive
data-store, we can setup a common connection strategy for Hive-ODCI
via the parameters
```
    param_( 'hive_jdbc_url',   'jdbc:hive2://hive.corp.com:10000' );
    param_( 'hive_jdbc_url.1', 'user=oracle' );
    param_( 'hive_jdbc_url.2', 'password=welcome1' );
```

If we have clients which need a different connection strategy or
use different parameters, they can change these at the session
level to meet their needs specifically, but for now we'll assume
everyone is using the same thing.


#### Hive-ODCI object creation
As denoted above we have PL/SQL code and a VIEW which is now 
invalid because the ```SCOTT.USER_LOG``` table longer exists.

So, let's put it back using Hive-ODCI. First let's create a new
VIEW which replaces the table.
```
    --
    grant execute on hive_q to scott;

    --
    create or replace view scott.user_log
        (
            stamp, 
            account, 
            message 
        )
    as
    select * 
      from table( hive_q( q'[ select stamp, 
                                     account, 
                                     message 
                                from user_log
                               order by stamp ]' ) ) 
    /
    
```

Whew, that was easy. We now have a column-by-column replacement
of the old table with a newly created remote table, that provides
the same data types as before.
```
    SQL> desc SCOTT.USER_LOG

     Name           Null?    Type
     -------------- -------- ---------------
     STAMP          NOT NULL DATE
     ACCOUNT                 VARCHAR2(4000)
     MESSAGE                 VARCHAR2(4000)
```

Let's see if it worked ...
```
    SQL> alter procedure scott.user_log_report compile;
    
    Procedure altered.
```

Excellent, so let's take a look at replacing the view. In this
case, we only care about data from the last 30 days, no need to
make Hadoop/Hive do more work than it has to. Let's use bindings
to restrict the data at the Hadoop/Hive layer instead of at the
Oracle layer.
```
    --
    grant execute on hive_bind  to scott;
    grant execute on hive_binds to scott;

    --
    create or replace view scott.user_log_monthly
        (
            stamp, 
            account, 
            message 
        )
    as
    select * 
      from table( hive_q( q'[ select stamp, 
                                     account, 
                                     message 
                                from user_log
                               where stamp between ? and ? ]',
                  hive_binds( hive_bind( to_char( sysdate - 30, 
                                                  'yyyy-mm-dd' ),
                                         1 /* type_date */,
                                         1 /* ref_in */ ),
                              hive_bind( to_char( sysdate, , 
                                                  'yyyy-mm-dd' ),
                                         1 /* type_date */,
                                         1 /* ref_in */ ) ) )
    /
```

Let's break this one down, as it is a little more complex than the
other. The ```hive_q``` function takes 3 parameters, 2 of which are
defaulted to ```NULL```. The first is a ```hive_binds``` object which
is simply an array of ```hive_bind``` objects or individual bind data.
The second parameter is the ```hive_session``` (e.g. URL, User, ...),
but since we are using a common connection strategy we will ignore
this argument, not pass it in, let it continue to be ```NULL``` 

The ```hive_bind``` object also takes 3 arguments, the first is the
data to be used as the bind. The second is the type of data to be
bound (e.g. string, date, number, etc...). And the third is the scope of
the bind, most useful for DML/DDL operations (e.g. IN, OUT, IN/OUT).

In our case we only care about scope references of IN, hence the
```1 /* ref_in */```. Since both bind operators are ``DATE`` variables
they both have ```1 /* type_date */```.

The actual bound data is based on ```SYSDATE```, but since we don't know
up front the ```NLS_DATE_FORMAT``` setting for the session, we simply
guarantee the format by wrapping it in a ```TO_CHAR()``` function and
specify the format Hadoop/Hive is expecting.

The Oracle RBAC controls dictate who can SELECT from the VIEWS, just as
before. So, if you have custom roles which need access to the VIEWS,
they can be granted in the same way.
```
    grant select on scott.user_log to my_app_role;
    grant select on scott.user_log_monthly to public;
```

Now wala! We have a VIEW that will retrieve the last 30 days worth
of data from Hapdoop/Hive.

Note we have **not** changed any code in our application or in our
PL/SQL procedure. Everything remains exactly as it was before, but
our data exists only in Hadoop/Hive. So, you can go to your meeting
now and be the hero.

Guidelines
------------------------------

## For Developers


## For Administrators



API 
------------------------------
The Application programming Interface (API) for Hive-ODCI is accessible
through the PL/SQL objects created during installation. The objects
include Views, Packages, Procedure, Types and Functions each providing
a unique set of functionality based on need.

The following is a list of objects which can be used 

## Packages

## Procedures

## Functions

## Types


Parameters
------------------------------


Roles
------------------------------



FAQ 
------------------------------



  [0]: https://docs.oracle.com/database/121/ADDCI/toc.htm
  [1]: http://www.mtihq.com
  [2]: https://github.com/nvanwyen/hive-odci/releases/latest
