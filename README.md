Hive-ODCI
===================

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
modification, are permitted provided that the following conditions
are met:
>> 1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.
>> 2. Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.
>> 3. The name of the author may not be used to endorse or promote products
   derived from this software without specific prior written permission.

> THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Releases
------------------------------
All releases can be found on Github 
https://github.com/nvanwyen/hive-odci/releases, along with the [latest 
release][4] release.

The project home is publicly available on Github at 
https://github.com/nvanwyen/hive-odci

Installation
------------------------------
The scripts are intended for Linux distributions only, and tested on RHEL 6. If 
your target installation is Windows, then you will need to perform additional 
prior to beginning. Please see [Windows](#Windows) section.

After downloading the distribution and check sum file, check to make sure check 
the integrity first. For example,
```
$ ls -l hive-odci-v0.1.5.19.sha hive-odci-v0.1.5.19.tgz
-rw-r--r-- 1 user mti      66 2016 hive-odci-v0.1.5.19.sha
-rw-r--r-- 1 user mti 1818992 2016 hive-odci-v0.1.5.19.tgz

$ sha1sum -c hive-odci-v0.1.5.19.sha
hive-odci-v0.1.5.19.tgz: OK
```

Extract the contents of the distribution file.
```
$ tar -xvf hive-odci-v0.1.5.19.tgz
LICENSE
README
VERSION
source/
...
```

Go the ```./source``` directory of the install home and using the ```SYSDBA```  
account run the ```install_hive.sql``` script using [SQL*Plus][2].
```
$ sqlplus "/ as sysdba" @install_hive.sql
```

A log file will be created in the current directory in the format 
```<sid>_remove_hive_odci.<date_time>.log ```. Please check this log file for 
errors before proceeding. The end of the log should look similar to the 
following.
```
... show post installation object errors

no rows selected

Installation successful

PL/SQL procedure successfully completed.

Run: jdbc/load-jdbc.sh "sys"
```
If the PL/SQL object installation was successful (as show above), then it's 
time to install the JDBC Driver of your choice in the database. 

Copy all the JAR files needed for your Driver into the ```./jdbc``` directory.
You may want to remove the existing ```jdbc/hive.jar``` file which came with the
distribution.

Then run the load script ```jdbc/load-jdbc.sh```. It will prompt you for a
User/Password if one was not provided on the command line. if you are using the
```SYSDBA``` account ```SYS``` locally on your Linux machine, then the password
will be ignored by the utility, so fell free to use anything.

````
jdbc/load-jdbc.sh "sys" "xxx"
````
At the end of the load operation, the log output should have ```0 errors``` 
reported, for example ...
```
...
Classes skipped: 0
Synonyms Created: 0
Errors: 0
```
> Note: While the Hive-ODCI should be able to work with any JDBC driver, it has 
been tested with the following:
>  * CDH 6 Apache Hive JDBC Standalone
>  * MTI CDH 5, JDBC Thin Driver
>  * Progress Data Direct JDBC Hive Driver

### Windows
If you are installing the distribution on or from Windows platform you must do
the following pre-steps before running the ```install_hive.sql``` script.

#### Rename and modify the ```hive.par.sql.in``` file
The ```hive.par.sql.in``` file is an input file modified with the current 
version information by the ```ver``` bash shell script.

Copy or move the file, to get rid of the ```.in``` extension, for example ...
```
copy /b hive.par.sql.in hive.par.sql
```
The ```install_hive.sql``` script looks for the file name ```hive.par.sql```,
during the installation and if it's missing will fail.

Then open the file ```hive.par.sql``` in a text editor (capable of reading
UNIX formatted files), and find the version parameter, so it can be set manually,

```
param_( 'version', '%version%' );
```
Modify this line to reflect the version as defined in the ```../VERSION``` 
file. For example.
```
param_( 'version', 'v0.1.5.19' );
```
> **Important** -- Make sure to use the correct version data from the file, as 
this may impact updates and patches at a later time if not correctly specified.

#### Load the JDBC Driver
You will not be able to use the ```jdbc/load-jdbc.sh``` bash script in Windows 
(unless you have [Cygwin ][2] or something similar installed). Therefore, you 
will have load each JAR file manually using the Oracle provided [loadjava][3] 
utility..

For each JAR file you have to load use the following as a template for 
executing.
```
loadjava -force ^
         -genmissing ^
         -order ^
         -verbose ^
         -resolve ^
         -recursivejars ^
         -resolver "((* hive) (* sys) (* public))" ^
         -user "$sys" ^
         -schema hive ^
         hive.jar
```
Setup
------------------------------
Once Hive-ODCI has been successfully installed and a compliant JDBC Driver 
loaded you must setup the parameters to use that Driver. The following is 
**not** an exhaustive overview of the parameters used by Hive-ODCI, just those 
commonly needed to get started.

#### * ```hive_jdbc_driver```
This parameter is the class name of the driver Hive-ODCI will load using 
```Class.forName()```. This must match the documentation of the driver you 
choose to use.. Common driver classes are
>  * CDH 6 Apache Hive JDBC Standalone: ```com.cloudera.hive.jdbc.HS1Driver```
>  * MTI CDH 5, JDBC Thin Driver: ```org.mti.hive.jdbc.thin.HiveDriver```
>  * Progress Data Direct JDBC Hive Driver: ```com.ddtek.jdbc.hive.HiveDriver```

#### * ```hive_jdbc_url```
This parameter is the URL of the driver, which may or may not contain 
[additional URL parameters](#hive_jdbc_url.x).  Make sure the driver protocol, 
host, port, etc... are correct for your environment. Examples, would be ...
>  * CDH 6 Apache Hive JDBC Standalone: ```jdbc:hive2://<host>:<port>/<db>```
>  * MTI CDH 5, JDBC Thin Driver: ```jdbc:hive2://<host>:<port>/<db>```
>  * Progress Data Direct JDBC Hive Driver: ```jdbc:datadirect:hive://<host>:<port>/<db>```

#### * ```hive_jdbc_url.x```
Use these parameters, specified as ```hive_jdbc_url.1``` ... 
```hive_jdbc_url.x``` in consecutive order (a gap in the numbering sequence 
will cause Hive-ODCI to stop reading the parameters assuming that it has 
reached the end of the list.) These parameters are specific to the driver you 
are using and may change from type to type. Some common examples, would be ...
```
param_( 'hive_jdbc_url.1', 'ssl=0' );
param_( 'hive_jdbc_url.2', 'UID=oracle' );
param_( 'hive_jdbc_url.3', 'PWD=welcome1' );
```
As they are read Hive-ODCI will append them in order to the URL, each
delineated by a semi-colon. 
For example ```jdbc:hive2://hive.mtihq.com:1000;ssl=0;UID=oracle;PWD=welcome1```

#### * ```hive_user``` and ```hive_pass```
The ```Driver,.getConnection()``` call in Java  is overloaded to accept a URL 
(see [above](#hive_jdbc_url)) only or optionally with a User, Password. These 
parameters are used for those Drivers which expect the User/Password to be 
provided through the ```Driver.getConnection()``` call. If these parameters are 
NULL, they are simply ignored (e.g. not provided to the call)
```
param_( 'hive_user', 'oracle' );
param_( 'hive_pass', 'welcome1' );
```

#### * ```java_property.x```
Like the [URL parameters](#hive_jdbc_url.x), the ```java_property.x``` 
parameters are a consecutive list starting at ```java_property.1``` through 
```java_property.x``` (and like the [URL parameters](#hive_jdbc_url.x) a gap in 
the numbering sequence will cause Hive-ODCI to stop reading the parameters 
assuming that it has reached the end of the list). These parameters are used in 
the ```System.setProperty( name, value )``` call with the Java Stored 
Procedure, to setup the Java Environment for the Driver.  For example, you may 
need to setup your Kerberos Authentication parameters, like so ....
```
param_( 'java_property.1', 'java.security.krb5.realm=MTI.COM' );
param_( 'java_property.2', 'java.security.krb5.realm=kdc.mti.com' );
param_( 'java_property.3', 'java.security.krb5.conf=/etc/krb5.conf' );
param_( 'java_property.4', 'java.security.auth.login.index=Client' );
param_( 'java_property.5', 'java.security.auth.login.config=/etc/jdbc.conf' );
param_( 'java_property.6', 'sun.security.krb5.debug=true' );
```
> Note: Using these parameters may require additional grants not initially 
anticipated and provided in the installation. You may need to provide grants 
for any operations not already provided using  ```dbms_java.grant_permission( 
'HIVE', 'SYS:<property>', '...', '...' )```

Removal
------------------------------
Removal of the Hive-ODCI functionality is similar to the 
[Installation](#Installation) procedures. Go the ```./source``` directory of 
the install home and using the ```SYSDBA```  account run the 
```remove_hive.sql``` script using [SQL*Plus][2]. This will drop the schema, 
including all associated objects, the public synonyms and the tablespaces used 
when installing Hive-ODCI.

> Note: The procedure for removal remains the same on the Windows platform.  
There are no additional steps, before or after, which need to be done for this 
operating system.

Users Guide
------------------------------
Hive-ODCI has been released with a comprehensive Users Guide which can be found 
at ```./doc``` off of the installation root directory. This guide provides a 
more in-depth look at Hive-ODCI, its design, configuration, usage, examples, 
Frequently Asked Questions (FAQ) and .Known Issues.

### Need help?
For more information please contact your friends at MTI or Nicholas Van Wyen 
directly as shown in the [Authors](#Authors) section above.


  [0]: https://docs.oracle.com/database/121/ADDCI/toc.htm
  [1]: http://www.mtihq.com
  [2]: https://www.cygwin.com
  [3]: http://docs.oracle.com/database/121/JJDEV/chtwo.htm#JJDEV02000
  [4]: https://github.com/nvanwyen/hive-odci/releases/latest
