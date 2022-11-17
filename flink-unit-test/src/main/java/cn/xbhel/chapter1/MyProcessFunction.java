package cn.xbhel.chapter1;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author xbhel
 */
public class MyProcessFunction extends KeyedProcessFunction<String, String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(String in, Context context, Collector<String> collector) throws Exception {
        context.timerService().registerProcessingTimeTimer(50);
        String out = "hello " + in;
        collector.collect(out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect(String.format("Timer triggered at timestamp %d", timestamp));
    }
}
