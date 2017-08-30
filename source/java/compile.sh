#!/bin/bash

# Hive-ODCI - Copyright (C) 2006-2016 Metasystems Technologies Inc. (MTI)
# Nicholas Van Wyen
# 
# This library is free software; you can redistribute it and/or modify it 
# under the terms of the GNU Lesser General Public License as published by 
# the Free Software Foundation; either version 2.1 of the License, or (at 
# your option) any later version.
# 
# This library is distributed in the hope that it will be useful, but 
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
# or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
# License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License 
# along with this library; if not, write to the
# 
#                 Free Software Foundation, Inc.
#                 59 Temple Place, Suite 330,
#                 Boston, MA 02111-1307 USA

#
out=hive-odci.jar

#
dir=`dirname $0`

#
if [ "${dir}" = "." ] ; then

    dir=`pwd`

fi

#
cd ${dir}
dir="."

#
jva=$@

#
javac=`which javac` # $ORACLE_HOME/jdk/bin/javac
jar=`which jar`     # $ORACLE_HOME/jdk/bin/jar
awk=`which awk`     # /usr/bin/awk

#
function color_echo()
{
    local color=$1;
    local exp=$2;
    local opt=$3;

    if ! [[ $color =~ '^[0-9]$' ]] ; then

        case $(echo $color | tr '[:upper:]' '[:lower:]') in
              black) color=0 ;;
                red) color=1 ;;
              green) color=2 ;;
             yellow) color=3 ;;
               blue) color=4 ;;
            magenta) color=5 ;;
               cyan) color=6 ;;
            white|*) color=7 ;; # white or "other"
        esac

    fi

    tput setaf $color;
    echo ${opt} "$exp";
    tput sgr0;
}

#
function chkerr()
{
    o=$1
    e=$2

    if [ ${e} -ne 0 ] ; then

        color_echo red "Error [${e}] in $o"
        exit 1

    fi
}

#
function compile()
{
    f=$1

    echo -n "Compiling " ; echo -n "$f " ; echo -n "... "   # use color?

    $javac ${JFLAGS} -cp ${cls} ${f}
    chkerr ${f} $?

    mkdir -p ${obj}
    cp `echo ${f} | sed s/\.java//g`.class ${obj}/

    if [ ${?} ] ; then

        color_echo green "OK"

    fi
}

#
if [ ! -f ${javac} ] ; then

    color_echo red "Cannot locate Java Compiler [${javac}]!"
    exit 1

else

    ver=$(${javac} -version 2>&1 | ${awk} '{print $2}')

    if [ "${ver:0:3}" = "1.7" ] ; then

        echo -n "Using Java compiler Version: " ; color_echo red "${ver}" -n
        echo ""

    else

        echo -n "Java version " ; color_echo red "${ver}" -n ; echo " is incomptaible"
        color_echo red "Must be 1.7_x"
        exit 1

    fi

fi

#
if [ ! -f ${jar} ] ; then

    colorecho red "Cannot locate Java Archiver [${jar}]!"
    exit 1

fi

#
if [[ -z ${jva} ]] ; then

    #
    if [ -f class.order ] ; then

        #
        for j in $(cat class.order) ; do

            jva="${jva} ${j}"

        done ;

    else

        # static ordered list
        jva="${jva} log.java"
        jva="${jva} tuple.java"
        jva="${jva} hive_types.java"
        jva="${jva} hive_exception.java"
        jva="${jva} hive_parameter.java"
        jva="${jva} hive_properties.java"
        jva="${jva} callback_handler.java"
        jva="${jva} hive_hint.java"
        jva="${jva} hive_rule.java"
        jva="${jva} hive_session.java"
        jva="${jva} hive_bind.java"
        jva="${jva} hive_bindings.java"
        jva="${jva} hive_connection.java"
        jva="${jva} hive_attribute.java"
        jva="${jva} hive_context.java"
        jva="${jva} hive_manager.java"
        jva="${jva} hive.java"

    fi

    #
    if [[ -z ${jva} ]] ; then

        color_echo red "No java files found!"
        exit 1

    fi

fi

#
src=${dir}

if [ ! -d ${src} ] ; then

    color_echo red "Source directory ${src} not found"
    exit 1

fi

#
bin=${dir}/../jdbc/

if [ ! -d ${bin} ] ; then

    color_echo red "Binary directory ${bin} not found"
    exit 1

fi

#
lib=${dir}/../jdbc/

if [ ! -d ${lib} ] ; then

    color_echo red "Library directory ${lib} not found"
    exit 1

fi

#
if [[ -z "${ORACLE_HOME}" ]] ; then

    color_echo red "Oracle Home (\$ORACLE_HOME) is not set!"
    exit 1

fi

#
if [ ! -f ${ORACLE_HOME}/jdbc/lib/ojdbc7.jar ] ; then

    color_echo red "Cannot find required file [\$ORACLE_HOME/jdbc/lib/ojdbc7.jar]!"
    exit 1

fi

#
if [ ! -d ${ORACLE_HOME}/jdk/jre/lib ] ; then

    color_echo red "Cannot find JDK directory [\$ORACLE_HOME/jdk/jre/lib]!"
    exit 1

fi

#
bse=oracle
obj=${bse}/mti/odci
cls=${CLASSPATH}:${jdk}:${ORACLE_HOME}/jdbc/lib/ojdbc7.jar:.:

# #
# CLASSPATH=${CLASSPATH}:${cls}:.:

#
JFLAGS="${JFLAGS} -Xlint:unchecked"
JFLAGS="${JFLAGS} -Xlint:-deprecation"
JFLAGS="${JFLAGS} -XDignore.symbol.file"

#
if [ ! -f $javac ] ; then

    color_echo red "Java compiler [javac] cannot be found"
    exit 1

fi

#
if [ ! -f $jar ] ; then

    color_echo red "Java archiver [jar] cannot be found"
    exit 1
fi

#
cd $src
mkdir -p $obj

#
for j in ${jva} ; do

    #
    compile ${j}

done ;

#
for c in ${jva} ; do

    mv `echo ${c} | sed s/\.java//g`.class ${bin}/

done;

#
rm -fR $bse

#
cd $bin

#
echo -n "Building JAR " ; color_echo magenta "${out} " -n ; echo -n "... "
rm -f ${out} 2>/dev/null
( cd ${bin} && ${jar} cvf ${out} *.class ) 2>&1 1>/dev/null
chkerr ${out} $?
color_echo green "done"

#
rm -f *.class 2>/dev/null
echo

exit $?
