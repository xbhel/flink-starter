package cn.xbhel.ut2;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author xbhel
 */
public class StatefulFlatMapFunction extends RichFlatMapFunction<Long, Long> {

    private static final long serialVersionUID = 1L;

    private ValueState<Long> previousOutput;

    @Override
    public void open(Configuration parameters) throws Exception {
        previousOutput = getRuntimeContext().getState(
                new ValueStateDescriptor<>("previousOutput", Types.LONG)
        );
    }

    @Override
    public void flatMap(Long value, Collector<Long> out) throws Exception {
        Long largerValue = value;
        if(previousOutput.value() != null) {
            largerValue = Math.max(previousOutput.value(), value);
        }
        previousOutput.update(largerValue);
        out.collect(largerValue);
    }
}
