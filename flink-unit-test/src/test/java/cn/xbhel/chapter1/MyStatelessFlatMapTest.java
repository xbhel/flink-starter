package cn.xbhel.chapter1;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xbhel
 */
class MyStatelessFlatMapTest {

    @Test
    void testFlatMap() throws Exception {
        MyStatelessFlatMap statelessFlatMap = new MyStatelessFlatMap();
        List<String> out = new ArrayList<>();
        ListCollector<String> listCollector = new ListCollector<>(out);
        statelessFlatMap.flatMap("world", listCollector);
        assertThat(out).isEqualTo(Lists.newArrayList("hello world"));
    }

}