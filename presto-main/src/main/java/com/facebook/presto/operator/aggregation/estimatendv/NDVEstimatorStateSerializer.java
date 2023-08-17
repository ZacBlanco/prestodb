package com.facebook.presto.operator.aggregation.estimatendv;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;

import java.util.HashMap;
import java.util.Map;

public class NDVEstimatorStateSerializer
        implements AccumulatorStateSerializer<NDVEstimatorState>
{

    private final Type type = new ArrayType(BigintType.BIGINT);

    public NDVEstimatorStateSerializer() {}

    @Override
    public Type getSerializedType()
    {
        return type;
    }

    @Override
    public void serialize(NDVEstimatorState state, BlockBuilder out)
    {
        BlockBuilder entryBuilder = out.beginBlockEntry();
        state.serialize(entryBuilder);
        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, NDVEstimatorState state)
    {
        Type rowTypes = getSerializedType();
        Block freqBlock = (Block) rowTypes.getObject(block, index);
        Map<Long, Long> freq = new HashMap<>();
        Long key = null;
        for (int i = 0; i < freqBlock.getPositionCount(); i++) {
            if (i % 2 == 1) {
                freq.put(key, freqBlock.getLong(i));
            }
            else {
                key = freqBlock.getLong(i);
            }
        }
        state.setFreqDict(freq);
    }
}
