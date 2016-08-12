#!/bin/bash

#
jdk=$1

#
function update_jdk6()
{
    perl $ORACLE_HOME/javavm/install/update_javavm_binaries.pl ${jdk}
    ( cd $ORACLE_HOME/rdbms/lib && make -f ins_rdbms.mk ioracle )

    # re-initialize JVM (needs to be done, when moving back to JDK 6 from JDK 7
    sqlplus -S "/ as sysdba" << !
        alter system set "_ash_enable" = false scope=memory;
        alter system set statistics_level=basic scope=memory;

        truncate table wrh$_sql_plan;
        truncate table wrh$_active_session_history;
        truncate table wri$_optstat_tab_history;
        truncate table wri$_optstat_ind_history;
        truncate table wri$_optstat_histhead_history;
        truncate table wri$_optstat_histgrm_history;
        truncate table wri$_optstat_aux_history;

        @?/javavm/install/rmjvm.sql
        @?/javavm/install/initjvm.sql

        alter system set "_ash_enable" = true  scope=memory;
        alter system set statistics_level=typical scope=memory;
!
}

#
function update_jdk7()
{
    perl $ORACLE_HOME/javavm/install/update_javavm_binaries.pl ${jdk}
    ( cd $ORACLE_HOME/rdbms/lib && make -f ins_rdbms.mk ioracle )
}

#
function get_version()
{
    #
    sqlplus -S "/ as sysdba" << !
        --  
        col ver for a12 head "jdk version"
        select dbms_java.get_jdk_version ver from dual;
!
}

#
case ${jdk} in

    6) dbshut $ORACLE_HOME
       update_jdk6
       dbstart $ORACLE_HOME
       ;;

    7) dbshut $ORACLE_HOME
       update_jdk7
       dbstart $ORACLE_HOME
       ;;

    ?) get_version
       exit 0
       ;;

    *) echo "Must be JDK 6 or 7"
       echo "Usage $0 [6 | 7 | ?]"
       exit 1
esac

#
sqlplus -S "/ as sysdba" << !
    --  
    @?/javavm/install/update_javavm_db.sql
!

#
get_version
exit 0
