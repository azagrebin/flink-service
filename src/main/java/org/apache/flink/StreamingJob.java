/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.generated.IncrementSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		//env.enableCheckpointing(5000);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		DataStream<IncrementSchema> events = env
				.addSource(new FlinkKafkaConsumer<>("flink-service", new SimpleStringSchema(), properties))
				.map(message -> (IncrementSchema) OBJECT_MAPPER.readerFor(IncrementSchema.class).readValue(message))
				.returns(IncrementSchema.class);

		events.addSink(createHBaseSinkFunction());

		events.print();

		StreamingFileSink<IncrementSchema> sink = StreamingFileSink
				.forRowFormat(new Path("hdfs://localhost/flink-service"), new SimpleStringEncoder<IncrementSchema>("UTF-8"))
				.build();

		events.addSink(sink);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	private static Put toPut(IncrementSchema increment) {
		Put put = new Put(Bytes.toBytes(increment.getKey()));
		put.addColumn(Bytes.toBytes("inc"), Bytes.toBytes("id"), Bytes.toBytes(increment.getIncrement()));
		return put;
	}

	private static HBaseSinkFunction<IncrementSchema> createHBaseSinkFunction() {
		Configuration config = HBaseConfiguration.create();
		String path = StreamingJob.class
				.getClassLoader()
				.getResource("hbase-site.xml")
				.getPath();
		config.addResource(new org.apache.hadoop.fs.Path(path));

		return new HBaseSinkFunction<>("flink-service", config, StreamingJob::toPut, 1, 1, 100L);
	}
}

// {"key":"bla", "increment":"5"}