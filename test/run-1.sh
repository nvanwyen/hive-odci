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

${java} -cp ${CP} hiveodic
rc=$?
echo ; echo "run complete"
exit $rc
