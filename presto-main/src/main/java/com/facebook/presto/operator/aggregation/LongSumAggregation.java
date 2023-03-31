/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.facebook.presto.type.BigintOperators;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class LongSumAggregation
        extends SqlAggregationFunction
{
    public static final LongSumAggregation LONG_SUM_AGGREGATION = new LongSumAggregation();
    private static final String NAME = "sum";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(LongSumAggregation.class, "input", NullableLongState.class, long.class);
    private static final MethodHandle BLOCK_INPUT_FUNCTION = methodHandle(LongSumAggregation.class, "blockInput", NullableLongState.class, Block.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(LongSumAggregation.class, "combine", NullableLongState.class, NullableLongState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(LongSumAggregation.class, "output", NullableLongState.class, BlockBuilder.class);

    public LongSumAggregation()
    {
        super(NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BIGINT),
                ImmutableList.of(parseTypeSignature(StandardTypes.BIGINT)));
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(LongSumAggregation.class.getClassLoader());

        AccumulatorStateSerializer<NullableLongState> stateSerializer = StateCompiler.generateStateSerializer(NullableLongState.class, classLoader);
        AccumulatorStateFactory<NullableLongState> stateFactory = StateCompiler.generateStateFactory(NullableLongState.class, classLoader);
        Type intermediateType = stateSerializer.getSerializedType();

        List<Type> inputTypes = ImmutableList.of(type);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, BIGINT.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                createBlockInputParameterMetadata(type),
                INPUT_FUNCTION,
                BLOCK_INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        NullableLongState.class,
                        stateSerializer,
                        stateFactory)),
                BIGINT);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);
        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType), BIGINT,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(INPUT_CHANNEL, type));
    }

    private static List<ParameterMetadata> createBlockInputParameterMetadata(Type type)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, type));
    }

    public static void input(NullableLongState state, long value)
    {
        state.setNull(false);
        state.setLong(BigintOperators.add(state.getLong(), value));
    }

    public static void blockInput(NullableLongState state, Block value)
    {
        long sum = 0;
        boolean hasNonNull = false;
        int positions = value.getPositionCount();
        if (value.mayHaveNull()) {
            int index = 0;
            while (index < positions) {
                while (index < positions && !value.isNull(index)) {
                    sum += BIGINT.getLong(value, index);
                    index++;
                }
                if (index > 0) {
                    hasNonNull = true;
                }
                while (index < positions && value.isNull(index)) {
                    index++;
                }
            }
        }
        else {
            hasNonNull = true;
            for (int i = 0; i < positions; ++i) {
                sum += BIGINT.getLong(value, i);
            }
        }
        if (hasNonNull) {
            state.setNull(false);
            state.setLong(BigintOperators.add(state.getLong(), sum));
        }
    }

    public static void combine(NullableLongState state, NullableLongState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(otherState.getLong());
            return;
        }

        state.setLong(BigintOperators.add(state.getLong(), otherState.getLong()));
    }

    public static void output(NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(BIGINT, state, out);
    }

    @Override
    public String getDescription()
    {
        return "";
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        return generateAggregation(BIGINT);
    }
}
