package cn.xbhel.ut2;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

/**
 * @author xbhel
 */
@ExtendWith(MockitoExtension.class)
class IncrementFlatMapFunctionTest {

    @Test
    void testIncrement(@Mock Collector<Long> collector) throws Exception {
        // 实例化
        IncrementFlatMapFunction increment = new IncrementFlatMapFunction();
        // 调用实现的方法
        increment.flatMap(2L, collector);
        // 验证使用正确的输出调用了收集器
        Mockito.verify(collector, times(1)).collect(3L);
    }

}