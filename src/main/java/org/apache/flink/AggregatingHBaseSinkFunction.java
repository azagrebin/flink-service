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

import org.apache.flink.addons.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.generated.IncrementSchema;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RowMutations;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class AggregatingHBaseSinkFunction<IN, K, ACC, OUT, CONTEXT>
        extends AggregatingTwoPhaseCommitSinkFunction<IN, K, ACC, OUT, CONTEXT>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final String hTableName;
    private final byte[] serializedConfig;

    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxMutations;
    private final long bufferFlushIntervalMillis;

    private final SerializableBiFunction<K, IN, List<Mutation>> inputMutationRetriever;
    private final SerializableBiFunction<K, OUT, RowMutations> outputMutationRetriever;

    private transient HbaseBufferedMutator mutator;

    public AggregatingHBaseSinkFunction(
            String hTableName,
            org.apache.hadoop.conf.Configuration conf,
            SerializableBiFunction<K, IN, List<Mutation>> inputMutationRetriever,
            SerializableBiFunction<K, OUT, RowMutations> outputMutationRetriever,
            long bufferFlushMaxSizeInBytes,
            long bufferFlushMaxMutations,
            long bufferFlushIntervalMillis,
            AggregateFunction<IN, ACC, OUT> aggregateFunction,
            KeySelector<IN, K> keySelector,
            TypeSerializer<K> keyTypeSerializer,
            TypeSerializer<ACC> accTypeSerializer,
            TypeSerializer<OUT> outTypeSerializer,
            TypeSerializer<CONTEXT> contextTypeSerializer) {
        super(
                aggregateFunction,
                keySelector,
                keyTypeSerializer,
                accTypeSerializer,
                outTypeSerializer,
                contextTypeSerializer);
        this.inputMutationRetriever = inputMutationRetriever;
        this.outputMutationRetriever = outputMutationRetriever;
        this.hTableName = hTableName;
        // Configuration is not serializable
        this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(conf);
        this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
        this.bufferFlushMaxMutations = bufferFlushMaxMutations;
        this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.mutator = new HbaseBufferedMutator(
                hTableName,
                HBaseConfigurationUtil.deserializeConfiguration(serializedConfig, HBaseConfiguration.create()),
                bufferFlushMaxSizeInBytes,
                bufferFlushMaxMutations,
                bufferFlushIntervalMillis);
        mutator.open();
    }

    @Override
    public void close() {
        mutator.close();
    }

    @Override
    protected void preCommit(Map<K, OUT> transaction) {

    }

    @Override
    protected void commit(Map<K, OUT> transaction) {
        for (Map.Entry<K, OUT> e : transaction.entrySet()) {
            try {
                mutator.mutateRow(outputMutationRetriever.apply(e.getKey(), e.getValue()));
            } catch (Throwable t) {
                throw new RuntimeException("Failed to commit changes to Hbase", t);
            }
        }
    }

    @Override
    protected void abort(Map<K, OUT> transaction) {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        super.snapshotState(context);
        mutator.flush();
    }

    @Override
    void processElement(K key, IN value, Context context) throws Exception {
        mutator.add(inputMutationRetriever.apply(key, value));
    }
}
