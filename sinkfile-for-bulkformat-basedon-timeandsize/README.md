# 介绍

当我使用 Flink 的 `StreamingFileSink` 写入 **Bulk Format** 的 PartFile 时，默认它只支持 Checkpoint 策略去滚动 PartFile，并不支持基于时间和文件大小的方式去滚动（对于 Row Format 是支持的）。

当我尝试去继承 `CheckpointPolicy` 去实现一个基于时间和文件大小滚动的策略时，在运行时捕获到一个异常：“Bulk Part Writers does not support pause and resume operations”，发生在 `snapShotState` 阶段，这是由 `BulkPartWriter#persist` 抛出的，因为  `BulkPartWriter` 不支持此操作。

我阅读了 `StreamingFileSink` 的源码，发现一个 PartFile 的生命周期在一次 Checkpoint 的变化过程如下：

- invoke：event 进行写入。会在此阶段创建 Bucket和 in-progess 状态的 PartFile（一个临时文件），如果它们不存在的话，然后将数据写入对应 Bucket 的 PartFile 中。
- snapShotState：此时会更新 bucketState（包含 bucketId，checkpointId，in-progress 文件路径，target 文件路径，文件流的 position 等） 和 maxPartCounterState，并关闭 PartFile 的写入，PartFile 的状态移动到 pending。
- notifyCheckpointComplete：Checkpoint 完成通知。此时会对 pengding 状态的 PartFile 进行提交，并将其重命名为用户指定的名称，PartFile 的状态移动至 finished，数据对用户可见；而且会对 in-progress 状态的 PartFile 和非活动状态的 Bucket 进行清理。

因此如果仅仅是实现一个基于时间和文件大小的方式去滚动 PartFile，我可以去重写 `StreamingFileSink`，去 hack snapShotState（peding）和 notifyCheckpointComplete（finished）的调用，去控制 PartFile 状态的移动，让其满足相应条件再触发。但是最重要的是保证这个 `exactly-once` 语义，当触发 failover 策略和 restart 的时候保证数据的正确性。

我初步的想法是持有文件流的 position，当恢复时 truncate 相应多余的数据。请问你有没有其他好的方法/思路？

 



