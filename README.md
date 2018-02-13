# Distributed Machine Learning Studio

Distributed Machine Learning Studio is Drag-and-Drop style ML Plaform that based on [Apache Spark](http://spark.apache.org) computation engine. DMLS supply similar experience as Microsoft [Azure Machine Learning Studio](https://studio.azureml.net/) and Aliyun [PAI](https://data.aliyun.com/product/learn), it benifits non-cloud user doing ML task on Big Data.

## Instructions of setting up

### Initialize metadata store

### Get IP address of host
IP_ADDR=$(ifconfig en0 | grep "inet" | awk '{ print $2}' | awk 'NR==2{print}')
sudo hostname quickstart.cloudera

docker run -itd --name=mysql -p 3306:3306 fluxcapacitor/sql-mysql
docker ps -a | grep fluxcapacitor/sql-mysql | awk '{print $1}' | xargs -I {} docker cp dmls_metadata.sql {}:/
docker exec -it $(docker ps -a | grep fluxcapacitor/sql-mysql | awk '{print $1}') bash -c 'mysql -uroot -ppassword < /dmls_metadata.sql'

### Start Web REST project

docker run -itd --name=tomcat --net=host -p 8080:8080 tomcat
docker cp dmls-rest.war $(docker ps -a | grep tomcat | awk '{print $1}'):/usr/local/tomcat/webapps
docker exec -it $(docker ps -a | grep tomcat | awk '{print $1}') bash

### Deploy web UI project
docker cp dmls-ui $(docker ps -a | grep tomcat | awk '{print $1}'):/usr/local/tomcat/webapps


### Start up HDFS
docker run -itd --name=metastore --net=host -e MYSQL_MASTER_SERVICE_HOST=192.168.154.81 -e MYSQL_MASTER_SERVICE_PORT_MYSQL_NATIVE=192.168.154.81 fluxcapacitor/metastore-1.2.1
docker exec -it $(docker ps -a | grep fluxcapacitor/metastore-1.2.1 | awk '{print $1}') bash


### Add hostname quickstart.cloudera
docker run --hostname=quickstart.cloudera --privileged=true -t -i -p 127.0.0.1:7180:7180 -p 8020:8020 registry.cn-hangzhou.aliyuncs.com/mysky528/cdh_quickstart /usr/bin/docker-quickstart

### Cloudera Manager is not started by default, start it
sh /home/cloudera/cloudera-manager --express
