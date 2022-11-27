package cn.xbhel.ut2;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xbhel
 */
class PassThroughProcessFunctionTest {

    @Test
    void testPassThrough() throws Exception {
        // 实例化 UDF
        PassThroughProcessFunction processFunction = new PassThroughProcessFunction();

        // 包装 UDF 至相应的 TestHarness 中
        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                ProcessFunctionTestHarnesses.forProcessFunction(processFunction);

        // 推送元素及与该元素关联的时间戳至算子中
        testHarness.processElement(1, 10);

        // 获取输出并断言
        assertThat(testHarness.extractOutputValues())
                .isEqualTo(Collections.singletonList(1));
    }

}