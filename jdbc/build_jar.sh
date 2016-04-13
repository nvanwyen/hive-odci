#/bin/bash

# simplest of all jar creations ...

cd /projects/cbp/hive/jdbc

echo -n "Building ... "

(
    cd hive-jdbc-thin/ && \
    jar -cf ../hive-jdbc-thin.jar * && \
    mv ../hive-jdbc-thin.jar ../../source/jdbc/ &&
    echo "done"
)

nm=`ls -lh ../source/jdbc/hive-jdbc-thin.jar | awk '{print $9}'`
sz=`ls -lh ../source/jdbc/hive-jdbc-thin.jar | awk '{print $5}'`

echo "Built: ${nm} (${sz})"

exit $?
