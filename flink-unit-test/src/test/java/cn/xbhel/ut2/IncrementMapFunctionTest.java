package cn.xbhel.ut2;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xbhel
 */
class IncrementMapFunctionTest {

    @Test
    void testIncrement() throws Exception {
        // 实例化你的函数
        IncrementMapFunction incrementMapFunction = new IncrementMapFunction();
        // 调用实现的方法
        assertThat(incrementMapFunction.map(2L)).isEqualTo(3L);
    }

}