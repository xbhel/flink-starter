package cn.xbhel.ut1;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xbhel
 */
class StatefulFlatMapTest {

    @Test
    void testFlatMap() throws Exception {
        StatefulFlatMap statefulFlatMap = new StatefulFlatMap();
        // OneInputStreamOperatorTestHarness 有两个泛型参数：第一个是输入类型；第二个是输出类型
        OneInputStreamOperatorTestHarness<String, String> testHarness =
                // 注意我们要使用 KeyedOneInputStreamOperatorTestHarness
                // 因为我们使用了 ValueState，该状态只能在 KeyedStream 上使用
                // KeyedOneInputStreamOperatorTestHarness 需要三个参数：测试的算子对象；key selector；key 的类型
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new StreamFlatMap<>(statefulFlatMap), x -> "1", Types.STRING);
        testHarness.open();

        //test first record
        testHarness.processElement("world", 10);
        ValueState<String> previousInput =
                statefulFlatMap.getRuntimeContext().getState(
                        new ValueStateDescriptor<>("previousInput", Types.STRING));
        String stateValue = previousInput.value();
        assertThat(testHarness.extractOutputStreamRecords())
                .isEqualTo(Lists.newArrayList(new StreamRecord<>("hello world", 10)));
        assertThat(stateValue).isEqualTo("world");

        //test second record
        testHarness.processElement("parallel", 20);
        assertThat(testHarness.extractOutputStreamRecords())
                .isEqualTo(Lists.newArrayList(
                        new StreamRecord<>("hello world", 10),
                        new StreamRecord<>("hello parallel world", 20)));
        assertThat(stateValue).isEqualTo("parallel");
    }

}