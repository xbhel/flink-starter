package cn.xbhel.ut2;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

/**
 * @author xbhel
 */
public class KeyedStatefulFlatMapFunction extends RichFlatMapFunction<Long, Long> {

    private static final long serialVersionUID = 1L;
    private static final ListStateDescriptor<Long> MAX_VALUE_STATE_DESC =
            new ListStateDescriptor<>("maxValueState", Types.LONG);

    private transient ListState<Long> maxValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        maxValueState = getRuntimeContext().getListState(MAX_VALUE_STATE_DESC);
    }

    @Override
    public void flatMap(Long value, Collector<Long> out) throws Exception {
        Long maxValue = StatefulFlatMapFunction.getMaxValue(value, maxValueState);
        maxValueState.update(Lists.newArrayList(maxValue));
        out.collect(maxValue);
    }
}
