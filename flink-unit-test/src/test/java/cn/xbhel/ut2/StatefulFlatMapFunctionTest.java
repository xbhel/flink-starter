package cn.xbhel.ut2;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.assertj.core.util.Arrays;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xbhel
 */
class StatefulFlatMapFunctionTest {

    private OneInputStreamOperatorTestHarness<Long, Long> testHarness;

    @BeforeEach
    void setupTestHarness() throws Exception {
        // 实例化 UDF
        StatefulFlatMapFunction statefulFlatMapFunction = new StatefulFlatMapFunction();
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

        // 获取输出列表用于断言（包含 watermark）
        // 在 flink 中水印（watermark）就是通过推送一条记录实现的，这条记录只有时间戳。
        assertThat(testHarness.getOutput().toArray())
                .isEqualTo(Arrays.array(
                        new StreamRecord<>(2L, 100L),
                        new Watermark(100L)
                ));

        // 推送元素及与该元素关联的时间戳至算子中
        testHarness.processElement(6L, 110);

        // 获取输出列表用于断言，直接获取值
        assertThat(testHarness.extractOutputValues())
                .isEqualTo(Lists.newArrayList(2L, 6L));

        // 获取 operator 状态用于断言
        ListState<Long> maxValueState = testHarness.getOperator()
                .getOperatorStateBackend()
                .getListState(new ListStateDescriptor<>("maxValueState", Types.LONG));
        assertThat(maxValueState.get()).isEqualTo(Lists.newArrayList(6L));

        // 获取侧边流的输出用于断言（仅在 ProcessFunction 可用）
        //assertThat(testHarness.getSideOutput(new OutputTag<>("invalidRecords")), hasSize(0))
    }
}