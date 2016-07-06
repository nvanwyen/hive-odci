#/!bin/bash

java=`which java`

if [[ -z ${java} ]] ; then

    echo "Java JVM [java] not found!"
    exit 1

fi

CP=":."

for j in `ls *.jar` ; do

    CP="${CP}:${j}"

done

echo "Using CLASSPATH=${CP}" ; echo

#${java} -cp ${CP} -Djava.security.auth.login.config=./JDBCDriverLogin.conf hiveodic ${@}S

# ${java} -cp ${CP} \
#         -Dsun.security.krb5.debug=true \
#         hiveodic ${@}

# ${java} -cp ${CP} \
#         -Dsun.security.krb5.debug=true \
#         -Djava.security.auth.login.config=./JDBCDriverLogin.conf \
#         hiveodic ${@}

${java} -cp ${CP} \
        -Dsun.security.krb5.debug=true \
        -Djava.security.krb5.realm=MTIHQ.COM \
        -Djava.security.krb5.kdc=kdc.mtihq.com \
        -Djava.security.krb5.conf=./krb5.conf \
        -Djava.security.auth.login.config=./jdbc.conf \
        hiveodic ${@}

rc=$?
exit $rc
