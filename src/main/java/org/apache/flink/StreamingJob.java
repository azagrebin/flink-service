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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.generated.IncrementSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
		env.enableCheckpointing(3000);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		DataStream<IncrementSchema> events = env
				.addSource(new FlinkKafkaConsumer<>("flink-service", new SimpleStringSchema(), properties))
				.map(message -> (IncrementSchema) OBJECT_MAPPER.readerFor(IncrementSchema.class).readValue(message))
				.returns(IncrementSchema.class);

		events.keyBy(IncrementSchema::getKey).addSink(createHBaseSinkFunction());

		events.print();

		StreamingFileSink<IncrementSchema> sink = StreamingFileSink
				.forRowFormat(new Path("hdfs://localhost/flink-service"), new SimpleStringEncoder<IncrementSchema>("UTF-8"))
				.build();

		events.addSink(sink);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	private static AggregatingHBaseSinkFunction<IncrementSchema, String, Integer, Integer, Void> createHBaseSinkFunction() {
		Configuration config = HBaseConfiguration.create();
		String path = StreamingJob.class
				.getClassLoader()
				.getResource("hbase-site.xml")
				.getPath();
		config.addResource(new org.apache.hadoop.fs.Path(path));

		return new AggregatingHBaseSinkFunction<>(
				"flink-service",
				config,
				StreamingJob::toPut,
				StreamingJob::toCommit,
				1, 1, 100L,
				IncAggregateFunction.INSTANCE,
				IncrementSchema::getKey,
				StringSerializer.INSTANCE,
				IntSerializer.INSTANCE,
				IntSerializer.INSTANCE,
				VoidSerializer.INSTANCE);
	}

	static List<Mutation> toPut(String key, IncrementSchema increment) {
		Put put = new Put(Bytes.toBytes(increment.getKey()));
		put.addColumn(Bytes.toBytes("inc"), Bytes.toBytes(increment.getId()), Bytes.toBytes(increment.getIncrement()));
		return Collections.singletonList(put);
	}

	public static RowMutations toCommit(String key, Integer agg) {
		RowMutations rowMutations = new RowMutations(Bytes.toBytes(key));
		Delete delete = new Delete(Bytes.toBytes(key));
		delete.addFamily(Bytes.toBytes("inc"));
		Put put = new Put(Bytes.toBytes(key));
		put.addColumn(Bytes.toBytes("inc"), Bytes.toBytes("value"), Bytes.toBytes(agg));
		try {
			rowMutations.add(delete);
			rowMutations.add(put);
		} catch (IOException e) {
			throw new RuntimeException("Unexpected commit mutation creation failure", e);
		}
		return rowMutations;
	}

	private enum IncAggregateFunction implements AggregateFunction<IncrementSchema, Integer, Integer> {
		INSTANCE;

		@Override
		public Integer createAccumulator() {
			return 0;
		}

		@Override
		public Integer add(IncrementSchema value, Integer accumulator) {
			return accumulator + value.getIncrement();
		}

		@Override
		public Integer getResult(Integer accumulator) {
			return accumulator;
		}

		@Override
		public Integer merge(Integer a, Integer b) {
			return a + b;
		}
	}
}

// {"key":"bla", "increment":"5"}