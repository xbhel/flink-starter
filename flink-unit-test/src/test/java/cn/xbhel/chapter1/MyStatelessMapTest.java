package cn.xbhel.chapter1;

import cn.xbhel.chapter1.MyStatelessMap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author xbhel
 */
class MyStatelessMapTest {

    @Test
    void testMap() throws Exception {
        MyStatelessMap statelessMap = new MyStatelessMap();
        String result = statelessMap.map("world");
        assertThat(result).isEqualTo("hello world");
    }

}