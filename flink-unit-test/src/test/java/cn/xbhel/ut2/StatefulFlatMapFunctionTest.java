package cn.xbhel.ut2;

import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author xbhel
 */
class StatefulFlatMapFunctionTest {

    private OneInputStreamOperatorTestHarness<Long, Long> testHarness;
    private StatefulFlatMapFunction statefulFlatMapFunction;

    @BeforeEach
    void setupTestHarness() throws Exception {
        // 实例化 UDF
        statefulFlatMapFunction = new StatefulFlatMapFunction();
        // 将 UDF 包装到相应的 TestHarness 中
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction));
        // 可选地配置执行环境
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);
        // 打开 testHarness(也会调用 RichFunctions 的 open 方法)
        testHarness.open();
    }

    @Test
    void testingStatefulFlatMapFunction() throws Exception {
        // 推送元素及与该元素关联的时间戳至算子中
        testHarness.processElement(2L, 100L);

        // 通过使用水印推进算子的事件时间来触发事件时间 timer
        testHarness.processWatermark(100L);

        // 通过直接提前算子的处理时间来触发处理时间 timer
        testHarness.setProcessingTime(100L);

        // 获取输出列表用于断言
        System.out.println(testHarness.getOutput());

        // 获取侧边流的输出用于断言（只在 ProcessFunction 可以）
        //assertThat(testHarness.getSideOutput(new OutputTag<>("invalidRecords")), hasSize(0))
    }

}