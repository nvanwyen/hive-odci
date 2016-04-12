#/bin/bash

# simplest of all jar creations ...

cd hive-jdbc-thin/ && \
jar -cf ../hive-jdbc-thin.jar * && \
mv ../hive-jdbc-thin.jar ../../source/jdbc/

exit $?
