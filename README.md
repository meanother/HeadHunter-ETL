# HeadHunter kafka ETL

## create kafka inst

```bash
adduser apache
adduser apache sudo

wget https://downloads.apache.org/kafka/2.5.0/kafka_2.12-2.5.0.tgz
wget https://downloads.apache.org/zookeeper/zookeeper-3.6.1/apache-zookeeper-3.6.1-bin.tar.gz
tar -zxf
```

### Zookeeper.service

vim zoo.cfg
```bash
tickTime=2000
dataDir=/home/apache/template
clientPort=2181
host.name=35.230.42.114
maxClientCnxns=60
```

systemd zookeeper.service

```buildoutcfg
[Unit]
Requires=network.target
After=network.target

[Service]
Type=forking
User=apache
ExecStart=/home/apache/zookeeper/bin/zkServer.sh start /home/apache/zookeeper/conf/zoo.cfg
ExecStop=/home/apache/zookeeper/bin/zkServer.sh stop /home/apache/zookeeper/conf/zoo.cfg
ExecReload=/home/apache/zookeeper/bin/zkServer.sh restart /home/apache/zookeeper/conf/zoo.cfg
Restart=on-abnormal
TimeoutSec=30
Restart=on-failure

[Install]
WantedBy=multi-user.target
```


### kafka.service

server.properties

```buildoutcfg
broker.id=0
listeners=PLAINTEXT://:9092
advertised.listeners=PLAINTEXT://35.230.42.114:9092
num.network.threads=3
zookeeper.connect=35.230.42.114:2181
message.max.bytes=1994857600
max.message.bytes=1994857600
#message.max.bytes=1702390132
```

systemd kafka.service

```buildoutcfg
[Unit]
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=apache
ExecStart=/bin/sh -c '/home/apache/kafka/bin/kafka-server-start.sh /home/apache/kafka/config/server.properties > /home/apache/kafka.log 2>&1'
ExecStop=/home/apache/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```


#### Create topic
```bash
kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic HeadHunterETL
```


sudo systemctl daemon-reload



#### make install python 3.7

```buildoutcfg
wget https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tgz ; \
tar xvf Python-3.7.* ; \
cd Python-3.7.3 ; \
mkdir ~/.python ; \
./configure --enable-optimizations --prefix=/home/www/.python ; \
make -j8 ; \
sudo make altinstall
```


#### create spark stand-alone app

```buildoutcfg
wget https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
sudo tar xvf spark-2.4.5-bin-hadoop2.7.tgz -C /home/apache/spark
sudo ./home/apache/spark/sbin/start-master.sh

sudo ./home/apache/spark/sbin/start-slave.sh spark://serverpromise:7077
or
sudo ./home/apache/spark/sbin/start-slave.sh --cores 4 --memory 4g spark://35.230.42.114:7077

```