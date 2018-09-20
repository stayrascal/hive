rm -rf /usr/local/Cellar/apache-hive-1.2.1-bin
echo "rm -rf /usr/local/Cellar/apache-hive-1.2.1-bin"

tar -zxvf packaging/target/apache-hive-1.2.1-bin.tar.gz -C /usr/local/Cellar
echo "tar zxvf packaging/target/apache-hive-1.2.1-bin.tar.gz -C /usr/local/Cellar"

cp ./conf/hive-site.xml /usr/local/Cellar/apache-hive-1.2.1-bin/conf
echo "cp ./conf/hive-site.xml /usr/local/Cellar/apache-hive-1.2.1-bin/conf"

cp ./conf/hive-env.sh /usr/local/Cellar/apache-hive-1.2.1-bin/conf
echo "cp ./conf/hive-env.sh /usr/local/Cellar/apache-hive-1.2.1-bin/conf"

cp ./lib/mysql-connector-java-8.0.12.jar /usr/local/Cellar/apache-hive-1.2.1-bin/lib
echo "cp ./lib/mysql-connector-java-8.0.12.jar /usr/local/Cellar/apache-hive-1.2.1-bin/lib"