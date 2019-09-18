
HADOOP_VERSION ?= 2.8.3
HADOOP_BIN ?= /usr/local/Cellar/hadoop/$(HADOOP_VERSION)/sbin

KAFKA ?= kafka_2.12-2.3.0
KAFKA_SERVER ?= localhost:9092

KAFKA_TOPIC ?= flink-service

HBASE_VERSION ?= 1.3.5
HBASE_BIN = /usr/local/Cellar/hbase/$(HBASE_VERSION)/libexec/bin
HBASE_TABLE ?= flink-service
HBASE_CF ?= inc
HBASE_ROW ?= bla

build:
	mvn clean package

start_hadoop:
	$(HADOOP_BIN)/start-all.sh

stop_hadoop:
	$(HADOOP_BIN)/stop-all.sh

start_kafka:
	$(KAFKA)/bin/zookeeper-server-start.sh $(KAFKA)/config/zookeeper.properties &
	$(KAFKA)/bin/kafka-server-start.sh $(KAFKA)/config/server.properties &

start_hbase:
	cp $(HBASE_BIN)/../conf/hbase-site.xml src/main/resources
	$(HBASE_BIN)/start-hbase.sh

stop_hbase:
	$(HBASE_BIN)/stop-hbase.sh

hbase_shell:
	$(HBASE_BIN)/hbase shell

hbase_tables:
	echo "list" | $(HBASE_BIN)/hbase shell -n

hbase_create_table:
	echo "create '$(HBASE_TABLE)',{NAME => '$(HBASE_CF)'}" | $(HBASE_BIN)/hbase shell -n

hbase_drop_table:
	echo "disable '$(HBASE_TABLE)'" | $(HBASE_BIN)/hbase shell -n
	echo "drop '$(HBASE_TABLE)'" | $(HBASE_BIN)/hbase shell -n

hbase_get:
	echo "get '$(HBASE_TABLE)', '$(HBASE_ROW)', {COLUMN => '$(HBASE_CF)'}" | $(HBASE_BIN)/hbase shell -n

create_kafa_topic:
	$(KAFKA)/bin/kafka-topics.sh --create \
		--bootstrap-server $(KAFKA_SERVER) \
		--replication-factor 1 \
		--partitions 1 \
		--topic $(KAFKA_TOPIC)

list_kafka_topics:
	$(KAFKA)/bin/kafka-topics.sh --list --bootstrap-server $(KAFKA_SERVER)

send_kafka_messages:
	$(KAFKA)/bin/kafka-console-producer.sh --broker-list $(KAFKA_SERVER) --topic $(KAFKA_TOPIC)