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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.generated.IncrementSchema;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.HashMap;
import java.util.Map;

public abstract class AggregatingTwoPhaseCommitSinkFunction<IN, K, ACC, OUT, CONTEXT>
        extends TwoPhaseCommitSinkFunction<IN, Map<K, OUT>, CONTEXT> {
    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;
    private final KeySelector<IN, K> keySelector;
    private final TypeSerializer<ACC> accTypeSerializer;

    private transient AggregatingState<IN, OUT> state;

    protected AggregatingTwoPhaseCommitSinkFunction(
            AggregateFunction<IN, ACC, OUT> aggregateFunction,
            KeySelector<IN, K> keySelector,
            TypeSerializer<K> keyTypeSerializer,
            TypeSerializer<ACC> accTypeSerializer,
            TypeSerializer<OUT> outTypeSerializer,
            TypeSerializer<CONTEXT> contextTypeSerializer) {
        super(new MapSerializer<>(keyTypeSerializer, outTypeSerializer), contextTypeSerializer);
        this.aggregateFunction = aggregateFunction;
        this.keySelector = keySelector;
        this.accTypeSerializer = accTypeSerializer;
    }

    @Override
    protected Map<K, OUT> beginTransaction() {
        return new HashMap<>();
    }

    @Override
    protected void invoke(Map<K, OUT> transaction, IN value, Context context) throws Exception {
        state.add(value);
        K key = keySelector.getKey(value); //(K)((IncrementSchema)value).getKey();
        transaction.put(key, state.get());
        processElement(key, value, context);
    }

    abstract void processElement(K key, IN value, Context context) throws Exception;

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        super.initializeState(context);
        AggregatingStateDescriptor<IN, ACC, OUT> descriptor =
                new AggregatingStateDescriptor<>("AggregateState", aggregateFunction, accTypeSerializer);
        state = context.getKeyedStateStore().getAggregatingState(descriptor);
    }
}
