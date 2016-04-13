#!/bin/bash

# loadjava -verbose -user "sys/Password99 as sysdba" -schema hive hive-jdbc-1.0.1-standalone.jar 2>&1 | tee hive-jdbc.`date +%Y%m%d%H%M%S`.log
# loadjava -verbose -user "sys/Password99 as sysdba" -schema hive hadoop-core-1.2.1.jar          2>&1 | tee hadoop-core.`date +%Y%m%d%H%M%S`.log

# loadjava -verbose -force -user "sys/Password99 as sysdba" -schema hive hive-jdbc-1.0.1-standalone.jar 2>&1 | tee hive-jdbc.`date +%Y%m%d%H%M%S`.log
# loadjava -verbose -force -user "sys/Password99 as sysdba" -schema hive hadoop-core-1.2.1.jar          2>&1 | tee hadoop-core.`date +%Y%m%d%H%M%S`.log

# loadjava -verbose -force -resolve -user "sys/Password99 as sysdba" -schema hive hive-jdbc-1.0.1-standalone.jar 2>&1 | tee hive-jdbc.`date +%Y%m%d%H%M%S`.log
# loadjava -verbose -force -resolve -user "sys/Password99 as sysdba" -schema hive hadoop-core-1.2.1.jar          2>&1 | tee hadoop-core.`date +%Y%m%d%H%M%S`.log

# loadjava -verbose -force -resolve -resolver "((* hive) (* sys) (* public))" -user "sys/Password99 as sysdba" -schema hive hive-jdbc-1.0.1-standalone.jar 2>&1 | tee hive-jdbc.`date +%Y%m%d%H%M%S`.log
# loadjava -verbose -force -resolve -resolver "((* hive) (* sys) (* public))" -user "sys/Password99 as sysdba" -schema hive hadoop-core-1.2.1.jar          2>&1 | tee hadoop-core.`date +%Y%m%d%H%M%S`.log

# loadjava -verbose -force -genmissing -user "sys/Password99 as sysdba" -schema hive hive-jdbc-1.0.1-standalone.jar 2>&1 | tee hive-jdbc.`date +%Y%m%d%H%M%S`.log
# loadjava -verbose -force -genmissing -user "sys/Password99 as sysdba" -schema hive hadoop-core-1.2.1.jar          2>&1 | tee hadoop-core.`date +%Y%m%d%H%M%S`.log

# --> loadjava -order -verbose -force -resolve -resolver "((* hive) (* sys) (* public))" -genmissing -genmissingjar ./hadoop-core-missing.jar -fileout ./hadoop-core-load.log -user "sys/Password99 as sysdba"  -schema hive hadoop-core-1.2.1.jar          2>&1 | tee hadoop-core.`date +%Y%m%d%H%M%S`.log
# --> loadjava -order -verbose -force -resolve -resolver "((* hive) (* sys) (* public))" -genmissing -genmissingjar ./libthrift-missing.jar   -fileout ./libthrift-load.log   -user "sys/Password99 as sysdba"  -schema hive libthrift-0.9.1.jar            2>&1 | tee libthrift.`date +%Y%m%d%H%M%S`.log
# --> loadjava -order -verbose -force -resolve -resolver "((* hive) (* sys) (* public))" -genmissing -genmissingjar ./hive-jdbc-missing.jar   -fileout ./hive-jdbc-load.log   -user "sys/Password99 as sysdba"  -schema hive hive-jdbc-1.0.1-standalone.jar 2>&1 | tee hive-jdbc.`date +%Y%m%d%H%M%S`.log

# loadjava -order -verbose -resolve -resolver "((* hive) (* sys) (* public))" -genmissing -genmissingjar ./hadoop-core-missing.jar -fileout ./hadoop-core-load.log -user "sys/Password99 as sysdba"  -schema hive hadoop-core-1.2.1.jar          2>&1 | tee hadoop-core.`date +%Y%m%d%H%M%S`.log
# loadjava -order -verbose -resolve -resolver "((* hive) (* sys) (* public))" -genmissing -genmissingjar ./libthrift-missing.jar   -fileout ./libthrift-load.log   -user "sys/Password99 as sysdba"  -schema hive libthrift-0.9.1.jar            2>&1 | tee libthrift.`date +%Y%m%d%H%M%S`.log
# loadjava -order -verbose -resolve -resolver "((* hive) (* sys) (* public))" -genmissing -genmissingjar ./hive-jdbc-missing.jar   -fileout ./hive-jdbc-load.log   -user "sys/Password99 as sysdba"  -schema hive hive-jdbc-1.0.1-standalone.jar 2>&1 | tee hive-jdbc.`date +%Y%m%d%H%M%S`.log


# loadjava -order \
#          -verbose \
#          -resolve \
#          -resolver "((* hive) (* sys) (* public))" \
#          -genmissing \
#          -user "sys/Password99 as sysdba" \
#          -schema hive \
#          hadoop-core-1.2.1.jar \
#          libthrift-0.9.1.jar \
#          hive-jdbc-1.0.1-standalone.jar \
#     2>&1 | tee loadjava.`date +%Y%m%d%H%M%S`.log

# loadjava -order \
#          -verbose \
#          -nativecompile \
#          -resolve \
#          -resolver "((* hive) (* sys) (* public))" \
#          -user "sys/Password99 as sysdba" \
#          -schema hive \
#          libthrift-0.9.1.jar \
#          hadoop-mapreduce-client-core-0.23.1.jar \
#          hadoop-mapreduce-client-common-0.23.9.jar \
#          hadoop-common-0.23.9.jar \
#          hadoop-core-1.2.1.jar \
#          hive-jdbc-1.0.1-standalone.jar \
#     2>&1 | tee loadjava.`date +%Y%m%d%H%M%S`.log

# loadjava -order \
#          -verbose \
#          -noverify \
#          -nativecompile \
#          -recursivejars \
#          -jarasresource \
#          -resolve \
#          -resolver "((* hive) (* sys) (* public))" \
#          -user "sys/Password99 as sysdba" \
#          -schema hive \
#          hadoop-core-1.2.1.jar \
#          hive-jdbc-1.0.1-standalone.jar \
#     2>&1 | tee loadjava.`date +%Y%m%d%H%M%S`.log

# loadjava -order \
#          -verbose \
#          -recursivejars \
#          -user "sys/Password99 as sysdba" \
#          -schema hive \
#          hadoop-core-1.2.1.jar \
#          hive-jdbc-1.0.1-standalone.jar \
#     2>&1 | tee loadjava.`date +%Y%m%d%H%M%S`.log


# loadjava -verbose \
#          -resolver "((* hive) (* sys) (* public))" \
#          -user "sys/Password99 as sysdba" \
#          -schema hive \
#          hadoop-auth-2.6.0.jar \
#          hadoop-common-2.6.0.jar \
#          hive-jdbc-0.14.0.2.2.9.7-2-standalone.jar \
#     2>&1 | tee loadjava.`date +%Y%m%d%H%M%S`.log

#
# now that we are completely feedup with trying to get the OOTB JAR files
# loaded, we have created our own "thin" jar containing those classes as
# identified through missing_classes.sql
#

loadjava -order \
         -verbose \
         -resolve \
         -recursivejars \
         -resolver "((* hive) (* sys) (* public))" \
         -user "sys/Password99 as sysdba" \
         -schema hive \
         hive-jdbc-thin.jar \
    2>&1 | tee --append loadjava.`date +%Y%m%d%H%M%S`.log && \
loadjava -order \
         -verbose \
         -genmissing \
         -resolver "((* hive) (* sys) (* public))" \
         -user "sys/Password99 as sysdba" \
         -schema hive \
         hive-jdbc-thin.jar \
    2>&1 | tee --append loadjava.`date +%Y%m%d%H%M%S`.log

#
sqlplus -S "/ as sysdba" << !
    --
    exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'putProviderProperty.HiveSaslPlain', '' );
    exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'insertProvider.HiveSaslPlain', '' );
    exec dbms_java.grant_permission( 'HIVE', 'SYS:java.security.SecurityPermission', 'insertProvider.HiveSaslPlain', '' );
!

exit $?
