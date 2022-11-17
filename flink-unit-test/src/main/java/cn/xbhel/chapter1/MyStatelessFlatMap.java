package cn.xbhel.chapter1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author xbhel
 */
public class MyStatelessFlatMap implements FlatMapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(String value, Collector<String> collector) throws Exception {
        String out = "hello " + value;
        collector.collect(out);
    }
}
