# Flink 单元测试

对于设计一个生产级别的应用，编写单元测试是最基本的任务之一，没有测试，一次简单的代码变更可能会导致生产上一系列的错误。对于所有的应用我们都应该为其编写单元测试，无论是清理数据和训练模型的简单作业，还是复杂的多租户实时数据处理系统。

在以下部分中，我们将介绍如何为 Apache Flink 应用编写单元测试。Apache Flink 提供了一个健壮的单元测试框架去确保我们应用在生产上的行为和开发阶段的预期一致。我们需要引入以下依赖去使用 Apache Flink 提供的测试框架：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>1.15.0</version>
    <scope>test</scope>
    <classifier>tests</classifier>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-runtime</artifactId>
    <version>1.15.0</version>
    <scope>test</scope>
    <classifier>tests</classifier>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-test-utils</artifactId>
    <version>1.15.0</version>
    <scope>test</scope>
</dependency>
```

不同的算子(operators) 编写单元测试的策略不同，可以将策略拆分成以下三种：

- 无状态算子(Stateless Operators)
- 有状态算子(Stateful Operators)
- 定时处理算子(Timed Process Operators)

## 无状态算子

为无状态算子编写单元测试非常简单，你只需遵守编写测试案例的基本形式，即创建被测类的实例并测试相应的方法。让我们举一个简单的 `Map` 算子的例子：

```java
public class MyStatelessMap implements MapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public String map(String value) throws Exception {
        return "hello" + value;
    }
}
```

`Map` 算子的测试案例应该是这样的：

```java
@Test
void testMap() throws Exception {
    MyStatelessMap statelessMap = new MyStatelessMap();
    String result = statelessMap.map("world");
    assertThat(result).isEqualTo("hello world");
}
```

是不是很简单，让我们再来看一下 `FlatMap` 算子：

```java
public class MyStatelessFlatMap implements FlatMapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(String value, Collector<String> collector) throws Exception {
        String out = "hello " + value;
        collector.collect(out);
    }
}

```

`FlatMap` 算子需要一个 `Collector` 对象和一个字符串值作为输入，对于测试案例，我们有两种选择：

1. 使用 Mock 框架 Mock `Collector` 对象，如 Mockito。
2. 使用 Flink 提供的 `ListCollector`。

我更喜欢第二种方法，因为它可以使用更少的代码行并且适用于大多数情况。

```java
@Test
void testFlatMap() throws Exception {
    MyStatelessFlatMap statelessFlatMap = new MyStatelessFlatMap();
    List<String> out = new ArrayList<>();
    ListCollector<String> listCollector = new ListCollector<>(out);
    statelessFlatMap.flatMap("world", listCollector);
    assertThat(out).isEqualTo(Lists.newArrayList("hello world"));
}
```

## 有状态的算子

为有状态的算子编写单元测试需要更多的努力，你需要检查算子的状态是否正确更新，以及是否与算子的输出一起正确清理。让我们以有状态的 `FlatMap` 算子为例：

```java
public class StatefulFlatMap extends RichFlatMapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    private ValueState<String> previousInput;

    @Override
    public void open(Configuration parameters) throws Exception {
        previousInput = getRuntimeContext().getState(
                new ValueStateDescriptor<>("previousInput", Types.STRING)
        );
    }

    @Override
    public void flatMap(String in, Collector<String> collector) throws Exception {
        String out = "hello " + in;
        if (previousInput.value() != null) {
            out = out + " " + previousInput.value();
        }
        previousInput.update(in);
        collector.collect(out);
    }
}
```

为上述类编写测试的难点是模拟配置(Configuration)以及应用程序的运行时上下文(RuntimeContext)，Flink 提供了一系列 `TestHarness` 测试工具类，以便让用户无需自己创建 mock 对象。使用 `KeyedOperatorHarness` 进行测试。测试案例如下：

```java
@Test
void testFlatMap() throws Exception {
    StatefulFlatMap statefulFlatMap = new StatefulFlatMap();
    // OneInputStreamOperatorTestHarness takes the input and output types as type parameters
    OneInputStreamOperatorTestHarness<String, String> testHarness =
        // KeyedOneInputStreamOperatorTestHarness takes three arguments:
        //   Flink operator object, key selector and key type
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
```

`TestHarness` 工具类提供了许多有用的方法，这里我们使用了其中三个：

1. `open`：使用相关参数调用 `FlatMap` 算子的 open 并初始化上下文。 
2. `processElement`：允许用户传递输入元素以及与该元素关联的时间戳。
3. `extractOutputStreamRecords`：从收集器 `Collector` 中获取输出记录及其时间戳。

使用 `TestHarness` 工具类在很大程度上简化了对有状态的算子进行单元测试。

## 定时处理算子

为处理算子( `Process` )或包含定时工作的处理算子编写测试与为有状态算子编写测试很相似，因为你也可以使用 `TestHarness`，但是，你需要注意另一个方面，即为事件提供时间戳并控制应用程序的当前时间，通过设置当前(处理/事件)时间，你能够去触发定时器，它将会调用这个算子的 `onTimer` 方法。

```java
public class MyProcessFunction extends KeyedProcessFunction<String, String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(String in, Context context, Collector<String> collector) throws Exception {
        context.timerService().registerProcessingTimeTimer(50);
        String out = "hello " + in;
        collector.collect(out);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect(String.format("Timer triggered at timestamp %d", timestamp));
    }
}
```

我们需要测试 `KeyedProcessFunction` 中的两个方法，即 `processElement` 和 `onTimer`，通过 `TestHarness`，我们能够控制算子的当前时间，因此，我们可以随意触发定时器而不是等待至特定时间。测试案例如下：

```java
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
```

测试 `CoProcess` 等多输入流算子的机制与上述机制类似，你应该为这些算子使用 `TestHarness` 的 `TwoInput` 变体，例如 `TwoInputStreamOperatorTestHarness`。

## 总结

本节我们展示了如何在 Apache Flink 中为**无状态、有状态和时间感知**的算子编写单元测试。













