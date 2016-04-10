#/!bin/bash

javac=`which javac`

if [[ -z ${javac} ]] ; then

    echo "Java compiler [javac] not found!"
    exit 1

fi

CP=":."

for j in `ls *.jar` ; do

    CP="${CP}:${j}"

done

${javac} -cp ${CP} HiveJdbcClientExample.java
exit $?
