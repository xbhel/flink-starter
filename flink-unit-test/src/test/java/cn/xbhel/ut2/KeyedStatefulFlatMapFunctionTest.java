package cn.xbhel.ut2;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xbhel
 */
class KeyedStatefulFlatMapFunctionTest {

    private OneInputStreamOperatorTestHarness<Long, Long> testHarness;
    private KeyedStatefulFlatMapFunction keyedStatefulFlatMapFunction;

    @BeforeEach
    void setupTestHarness() throws Exception {
        // 实例化 UDF
        keyedStatefulFlatMapFunction = new KeyedStatefulFlatMapFunction();
        // 将 UDF 包装到相应的 TestHarness 中
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new StreamFlatMap<>(keyedStatefulFlatMapFunction), (el) -> "1", Types.STRING);
        // 打开 testHarness(也会调用 RichFunctions 的 open 方法)
        testHarness.open();
    }

    @Test
    void testingStatefulFlatMapFunction() throws Exception {
        // 推送元素及与该元素关联的时间戳至算子中
        testHarness.processElement(2L, 100L);

        // 获取输出列表用于断言
        assertThat(testHarness.extractOutputValues())
                .isEqualTo(Lists.newArrayList(2L));

        // 推送元素及与该元素关联的时间戳至算子中
        testHarness.processElement(6L, 110L);

        // 获取 keyed 状态用于断言，keyed 状态我们可以直接通过算子的 getRuntimeContext 获取
        // 当然也可以使用 testHarness 获取
        ListState<Long> maxValueState = keyedStatefulFlatMapFunction.getRuntimeContext()
                .getListState(new ListStateDescriptor<>("maxValueState", Types.LONG));
        assertThat(maxValueState.get())
                .describedAs("test keyed-state")
                .isEqualTo(Lists.newArrayList(6L));
    }
}