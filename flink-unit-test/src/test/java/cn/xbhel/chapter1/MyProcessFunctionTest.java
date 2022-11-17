package cn.xbhel.chapter1;

import cn.xbhel.chapter1.MyProcessFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MyProcessFunctionTest {

    @Test
    void testProcessElement() throws Exception {
        MyProcessFunction myProcessFunction = new MyProcessFunction();
        OneInputStreamOperatorTestHarness<String, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(myProcessFunction), x -> "1", Types.STRING);
        // Function time is initialized to 0
        testHarness.open();
        // 区别 10 是与元素关联的时间戳而不一定是 “当前时间”，当注册的是处理时间 timer 就不是。
        testHarness.processElement("world", 10);

        assertThat(testHarness.extractOutputStreamRecords())
                .isEqualTo(Lists.newArrayList(new StreamRecord<>("hello world", 10)));
    }

    @Test
    void testOnTimer() throws Exception {
        MyProcessFunction myProcessFunction = new MyProcessFunction();
        OneInputStreamOperatorTestHarness<String, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(myProcessFunction), x -> "1", Types.STRING);
        testHarness.open();
        testHarness.processElement("world", 10);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(1);

        // Function time is set to 50 => processTime
        testHarness.setProcessingTime(50);
        assertThat(testHarness.extractOutputStreamRecords())
                .isEqualTo(Lists.newArrayList(
                        new StreamRecord<>("hello world", 10),
                        new StreamRecord<>("Timer triggered at timestamp 50")
                ));
    }
}