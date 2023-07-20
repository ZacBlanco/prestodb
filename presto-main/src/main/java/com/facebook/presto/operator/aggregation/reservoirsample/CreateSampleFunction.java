package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.operator.aggregation.arrayagg.ArrayAggregationStateFactory;
import com.facebook.presto.operator.aggregation.arrayagg.SetUnionFunction;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.function.aggregation.AggregationMetadata;
import com.facebook.presto.spi.function.aggregation.GroupedAccumulator;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.AbstractMinMaxAggregationFunction.createParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.*;
import static com.facebook.presto.spi.function.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class CreateSampleFunction
        extends SqlAggregationFunction {

    public static final CreateSampleFunction Reservoir_Sample = new CreateSampleFunction();
    private static final String NAME = "reservoir_sample";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(CreateSampleFunction.class, "input", Type.class, ReservoirSampleState.class, Block.class, int.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(CreateSampleFunction.class, "combine", Type.class, ReservoirSampleState.class, ReservoirSampleState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(CreateSampleFunction.class, "output", Type.class, ReservoirSampleState.class, BlockBuilder.class);

    protected CreateSampleFunction() {
        super(NAME, ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature("T")));
    }

    @Override
    public BuiltInAggregationFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager) {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type);
    }

    private static BuiltInAggregationFunctionImplementation generateAggregation(Type type) {
        DynamicClassLoader classLoader = new DynamicClassLoader(CreateSampleFunction.class.getClassLoader());
        AccumulatorStateSerializer<?> stateSerializer = new ReservoirSampleStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new ReservoirSampleStateFactory(type);
//        AccumulatorStateFactory<?> stateFactory = StateCompiler.generateStateFactory(ReservoirSampleState.class, classLoader);
        List<Type> inputTypes = ImmutableList.of(type);
        Type outputType = new ArrayType(type);
        Type intermediateType = stateSerializer.getSerializedType();
        List<AggregationMetadata.ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(type);
        Class<? extends AccumulatorState> stateInterface = ReservoirSampleState.class;

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                combineFunction,
                outputFunction,
                ImmutableList.of(new AggregationMetadata.AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                outputType);

        Class<? extends Accumulator> accumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                Accumulator.class,
                metadata,
                classLoader);
        Class<? extends GroupedAccumulator> groupedAccumulatorClass = AccumulatorCompiler.generateAccumulatorClass(
                GroupedAccumulator.class,
                metadata,
                classLoader);

        return new BuiltInAggregationFunctionImplementation(NAME, inputTypes, ImmutableList.of(intermediateType), outputType,
                true, false, metadata, accumulatorClass, groupedAccumulatorClass);

    }

    private static List<AggregationMetadata.ParameterMetadata> createInputParameterMetadata(Type type) {
        return ImmutableList.of(new AggregationMetadata.ParameterMetadata(STATE), new AggregationMetadata.ParameterMetadata(BLOCK_INPUT_CHANNEL, type), new AggregationMetadata.ParameterMetadata(BLOCK_INDEX));
    }

    @Override
    public String getDescription() {
        return "return an array of values";
    }

    public static void input(Type type, ReservoirSampleState state, Block value, int position) {
        state.add(value, position);
    }

    public static void combine(Type type, ReservoirSampleState state, ReservoirSampleState otherState) {
        state.merge(otherState);
    }

    public static void output(Type elementType, ReservoirSampleState state, BlockBuilder out) {

        BlockBuilder entryBuilder = out.beginBlockEntry();
        Block[] samples = state.getSamples();
        for (int pos = 0; pos < samples.length; pos++) {
            elementType.appendTo(samples[pos], 0, entryBuilder);
        }
        out.closeEntry();
    }
}


