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
package com.facebook.presto.operator;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.plan.AggregationNode;

import static com.google.common.base.Preconditions.checkArgument;

class Aggregator
{
    private static final Logger log = Logger.get(Aggregator.class);
    private final Accumulator aggregation;
    private final AggregationNode.Step step;
    private final int intermediateChannel;
    private final boolean optimizeSingleGroupInputPage;
    private int blockInputCount;
    private int normalInputCount;

    Aggregator(AccumulatorFactory accumulatorFactory, AggregationNode.Step step, UpdateMemory updateMemory, boolean optimizeSingleGroupInputPage)
    {
        if (step.isInputRaw()) {
            intermediateChannel = -1;
            aggregation = accumulatorFactory.createAccumulator(updateMemory);
        }
        else {
            checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
            intermediateChannel = accumulatorFactory.getInputChannels().get(0);
            aggregation = accumulatorFactory.createIntermediateAccumulator();
        }
        this.step = step;
        this.optimizeSingleGroupInputPage = optimizeSingleGroupInputPage;
        this.blockInputCount = 0;
        this.normalInputCount = 0;
    }

    public Type getType()
    {
        if (step.isOutputPartial()) {
            return aggregation.getIntermediateType();
        }
        else {
            return aggregation.getFinalType();
        }
    }

    public void processPage(Page page)
    {
        if (step.isInputRaw()) {
            if (aggregation.hasAddBlockInput() && optimizeSingleGroupInputPage) {
                ++blockInputCount;
                aggregation.addBlockInput(page);
            }
            else {
                ++normalInputCount;
                aggregation.addInput(page);
            }
        }
        else {
            aggregation.addIntermediate(page.getBlock(intermediateChannel));
        }
    }

    public void evaluate(BlockBuilder blockBuilder)
    {
        if (step.isOutputPartial()) {
            aggregation.evaluateIntermediate(blockBuilder);
        }
        else {
            aggregation.evaluateFinal(blockBuilder);
        }
        log.info("block input: " + blockInputCount + " normal input: " + normalInputCount);
    }

    public long getEstimatedSize()
    {
        return aggregation.getEstimatedSize();
    }
}
