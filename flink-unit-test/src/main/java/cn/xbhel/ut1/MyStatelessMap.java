package cn.xbhel.ut1;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author xbhel
 */
public class MyStatelessMap implements MapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String map(String value) throws Exception {
        return "hello " + value;
    }
}
