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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.Random;

public class KinesisEventGenerator {
    private final static Random random = new Random();

    public static void main(String[] args) {
        int numberOfKeys = args.length > 0 ? Integer.parseInt(args[0]) : 10;
        int id = 0;

        String streamName = "";
        String regionStr = "";

        Region region = Region.of(regionStr);
        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(this.region));

        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                streamName,
                streamName,
                kinesisClient,
                DynamoDbAsyncClient.builder().region(region).build(),
                CloudWatchAsyncClient.builder().region(region).build(),
                new SampleRecordProcessorFactory());

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig().retrievalSpecificConfig(new PollingConfig(streamName, kinesisClient))
        );

        try (Producer<String, String> producer = createProducer()) {
            while (true) {
                String key = "key" + random.nextInt(numberOfKeys);
                int inc = random.nextInt(100);
                String message = String.format("{\"key\":\"%s\", \"increment\":\"%d\", \"id\":\"%d\"}", key, inc, id++);
                producer.send(new ProducerRecord<>(
                        "flink-service",
                        key,
                        message));
                System.out.println(message);
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    System.out.println("Interrupted");
                    return;
                }
            }
        }
    }

    private static void publishRecord(String streamName, String key, String message) {
        PutRecordRequest request = PutRecordRequest.builder()
                .partitionKey(key)
                .streamName(streamName)
                .data(SdkBytes.fromByteArray(RandomUtils.nextBytes(10)))
                .build();
        try {
            kinesisClient.putRecord(request).get();
        } catch (InterruptedException e) {
            log.info("Interrupted, assuming shutdown.");
        } catch (ExecutionException e) {
            log.error("Exception while sending data to Kinesis. Will try again next cycle.", e);
        }
    }
}
