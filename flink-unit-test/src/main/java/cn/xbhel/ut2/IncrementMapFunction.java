package cn.xbhel.ut2;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author xbhel
 */
public class IncrementMapFunction implements MapFunction<Long, Long> {

    private static final long serialVersionUID = 1L;

    @Override
    public Long map(Long value) throws Exception {
        return value + 1;
    }
}
