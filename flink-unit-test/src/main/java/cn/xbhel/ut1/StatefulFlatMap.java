package cn.xbhel.ut1;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author xbhel
 */
public class StatefulFlatMap extends RichFlatMapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<String> previousInput;

    @Override
    public void open(Configuration parameters) throws Exception {
        previousInput = getRuntimeContext().getState(
                new ValueStateDescriptor<>("previousInput", Types.STRING)
        );
    }

    @Override
    public void flatMap(String in, Collector<String> collector) throws Exception {
        String out = "hello " + in;
        if (previousInput.value() != null) {
            out = out + " " + previousInput.value();
        }
        previousInput.update(in);
        collector.collect(out);
    }
}
