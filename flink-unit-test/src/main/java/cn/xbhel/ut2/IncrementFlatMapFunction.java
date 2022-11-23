package cn.xbhel.ut2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author xbhel
 */
public class IncrementFlatMapFunction implements FlatMapFunction<Long, Long> {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(Long value, Collector<Long> out) throws Exception {
        out.collect(value + 1);
    }
}
