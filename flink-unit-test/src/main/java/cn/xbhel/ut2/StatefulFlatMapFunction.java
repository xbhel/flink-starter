package cn.xbhel.ut2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

/**
 * @author xbhel
 */
public class StatefulFlatMapFunction implements CheckpointedFunction, FlatMapFunction<Long, Long> {

    private static final long serialVersionUID = 1L;
    private static final ListStateDescriptor<Long> MAX_VALUE_STATE_DESC =
            new ListStateDescriptor<>("maxValueState", Types.LONG);

    private transient ListState<Long> maxValueState;

    @Override
    public void flatMap(Long value, Collector<Long> out) throws Exception {
        Long maxValue = getMaxValue(value, maxValueState);
        maxValueState.update(Lists.newArrayList(maxValue));
        out.collect(maxValue);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // do nothing
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        maxValueState = context.getOperatorStateStore().getListState(MAX_VALUE_STATE_DESC);
    }

    static Long getMaxValue(Long value, ListState<Long> state) throws Exception {
        Iterable<Long> longIterable = state.get();
        if (longIterable != null) {
            for (Long preMaxValue: longIterable) {
                value = Math.max(preMaxValue, value);
            }
        }
        return value;
    }
}
