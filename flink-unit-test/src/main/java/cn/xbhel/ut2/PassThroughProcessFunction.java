package cn.xbhel.ut2;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author xbhel
 */
public class PassThroughProcessFunction extends ProcessFunction<Integer, Integer> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(Integer value,
                               ProcessFunction<Integer, Integer>.Context context,
                               Collector<Integer> out) throws Exception {
        out.collect(value);
    }
}
