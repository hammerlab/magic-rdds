
# `org.apache.spark.batch`

RDD batching is introduced for reason of overcoming OOMs when scheduling all tasks for RDD having
limited amount of memory per executor. This is a perfect fit for training model to still leverage
Spark parallelism, but avoid collecting on a driver; this is a tradeoff of computation time and
memory usage per executor and driver. Note that if batch size is larger or equal to total number of
input partitions, then batching is not applied.

Essentially, this functionality (at a basic level) allows to maximize executor _memory/core_ ratio
for each task at a time. This approach is also better than using parameter `spark.task.cpus`, as
latter would affect entire Spark application, where batching is applied to a specific RDD step.

Batching of tasks in Spark works as batches mapped to multiple stages. Batch resolution is left
outside of RDD.

Batching is split into two parts:
- 1 to N map stages and single reduce stage. Map
stage modifies values to add partition index that is used as key-type for shuffle, therefore we
serialize data per partition. Each map stage has dependency on itself, but does not pull data from
shuffle reader. This allows to block execution of other partitions until `i`th batch is finished.
- Once all map stages are complete, reduce stage is launched to collect all shuffle output and
remove partition index from values.
