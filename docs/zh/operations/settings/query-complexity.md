# 查询复杂性的限制 {#restrictions-on-query-complexity}

对查询复杂性的限制是设置的一部分。
它们被用来从用户界面提供更安全的执行。
几乎所有的限制只适用于选择。对于分布式查询处理，每个服务器上分别应用限制。

Restrictions on the «maximum amount of something» can take the value 0, which means «unrestricted».
大多数限制也有一个 ‘overflow\_mode’ 设置，这意味着超过限制时该怎么做。
它可以采用以下两个值之一: `throw` 或 `break`. 对聚合的限制(group\_by\_overflow\_mode)也具有以下值 `any`.

`throw` – Throw an exception (default).

`break` – Stop executing the query and return the partial result, as if the source data ran out.

`any (only for group_by_overflow_mode)` – Continuing aggregation for the keys that got into the set, but don’t add new keys to the set.

## 只读 {#query-complexity-readonly}

值为0时，可以执行任何查询。
如果值为1，则只能执行读取请求（如SELECT和SHOW）。 禁止写入和更改设置（插入，设置）的请求。
值为2时，可以处理读取查询（选择、显示）和更改设置（设置）。

启用只读模式后，您无法在当前会话中禁用它。

在HTTP接口中使用GET方法时, ‘readonly = 1’ 自动设置。 换句话说，对于修改数据的查询，您只能使用POST方法。 您可以在POST正文或URL参数中发送查询本身。

## max\_memory\_usage {#settings_max_memory_usage}

用于在单个服务器上运行查询的最大RAM量。

在默认配置文件中，最大值为10GB。

该设置不考虑计算机上的可用内存量或内存总量。
该限制适用于单个服务器中的单个查询。
您可以使用 `SHOW PROCESSLIST` 查看每个查询的当前内存消耗。
此外，还会跟踪每个查询的内存消耗峰值并将其写入日志。

不监视某些聚合函数的状态的内存使用情况。

未完全跟踪聚合函数的状态的内存使用情况 `min`, `max`, `any`, `anyLast`, `argMin`, `argMax` 从 `String` 和 `Array` 争论。

内存消耗也受到参数的限制 `max_memory_usage_for_user` 和 `max_memory_usage_for_all_queries`.

## max\_memory\_usage\_for\_user {#max-memory-usage-for-user}

用于在单个服务器上运行用户查询的最大RAM量。

默认值定义在 [设置。h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Settings.h#L244). 默认情况下，金额不受限制 (`max_memory_usage_for_user = 0`).

另请参阅说明 [max\_memory\_usage](#settings_max_memory_usage).

## max\_memory\_usage\_for\_all\_queries {#max-memory-usage-for-all-queries}

用于在单个服务器上运行所有查询的最大RAM数量。

默认值定义在 [设置。h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Settings.h#L245). 默认情况下，金额不受限制 (`max_memory_usage_for_all_queries = 0`).

另请参阅说明 [max\_memory\_usage](#settings_max_memory_usage).

## max\_rows\_to\_read {#max-rows-to-read}

可以在每个块（而不是每行）上检查以下限制。 也就是说，限制可以打破一点。
在多个线程中运行查询时，以下限制单独应用于每个线程。

运行查询时可从表中读取的最大行数。

## max\_bytes\_to\_read {#max-bytes-to-read}

运行查询时可以从表中读取的最大字节数（未压缩数据）。

## read\_overflow\_mode {#read-overflow-mode}

读取的数据量超过其中一个限制时该怎么办: ‘throw’ 或 ‘break’. 默认情况下，扔。

## max\_rows\_to\_group\_by {#max-rows-to-group-by}

从聚合接收的唯一密钥的最大数量。 此设置允许您在聚合时限制内存消耗。

## group\_by\_overflow\_mode {#group-by-overflow-mode}

当聚合的唯一键数超过限制时该怎么办: ‘throw’, ‘break’，或 ‘any’. 默认情况下，扔。
使用 ‘any’ 值允许您运行GROUP BY的近似值。 这种近似值的质量取决于数据的统计性质。

## max\_rows\_to\_sort {#max-rows-to-sort}

排序前的最大行数。 这允许您在排序时限制内存消耗。

## max\_bytes\_to\_sort {#max-bytes-to-sort}

排序前的最大字节数。

## sort\_overflow\_mode {#sort-overflow-mode}

如果排序前收到的行数超过其中一个限制，该怎么办: ‘throw’ 或 ‘break’. 默认情况下，扔。

## max\_result\_rows {#max-result-rows}

限制结果中的行数。 还检查子查询，并在运行分布式查询的部分时在远程服务器上。

## max\_result\_bytes {#max-result-bytes}

限制结果中的字节数。 与之前的设置相同。

## result\_overflow\_mode {#result-overflow-mode}

如果结果的体积超过其中一个限制，该怎么办: ‘throw’ 或 ‘break’. 默认情况下，扔。
使用 ‘break’ 类似于使用限制。

## max\_execution\_time {#max-execution-time}

最大查询执行时间（以秒为单位）。
此时，不会检查其中一个排序阶段，也不会在合并和最终确定聚合函数时进行检查。

## timeout\_overflow\_mode {#timeout-overflow-mode}

如果查询的运行时间长于 ‘max\_execution\_time’: ‘throw’ 或 ‘break’. 默认情况下，扔。

## min\_execution\_speed {#min-execution-speed}

以每秒行为单位的最小执行速度。 检查每个数据块时 ‘timeout\_before\_checking\_execution\_speed’ 到期。 如果执行速度较低，则会引发异常。

## timeout\_before\_checking\_execution\_speed {#timeout-before-checking-execution-speed}

检查执行速度是不是太慢（不低于 ‘min\_execution\_speed’），在指定的时间以秒为单位已过期之后。

## max\_columns\_to\_read {#max-columns-to-read}

单个查询中可从表中读取的最大列数。 如果查询需要读取更多列，则会引发异常。

## max\_temporary\_columns {#max-temporary-columns}

运行查询时必须同时保留在RAM中的最大临时列数，包括常量列。 如果有比这更多的临时列，它会引发异常。

## max\_temporary\_non\_const\_columns {#max-temporary-non-const-columns}

同样的事情 ‘max\_temporary\_columns’，但不计数常数列。
请注意，常量列在运行查询时经常形成，但它们需要大约零计算资源。

## max\_subquery\_depth {#max-subquery-depth}

子查询的最大嵌套深度。 如果子查询更深，则会引发异常。 默认情况下，100。

## max\_pipeline\_depth {#max-pipeline-depth}

最大管道深度。 对应于查询处理期间每个数据块经历的转换数。 在单个服务器的限制范围内计算。 如果管道深度较大，则会引发异常。 默认情况下，1000。

## max\_ast\_depth {#max-ast-depth}

查询语法树的最大嵌套深度。 如果超出，将引发异常。
此时，在解析过程中不会对其进行检查，而是仅在解析查询之后进行检查。 也就是说，在分析过程中可以创建一个太深的语法树，但查询将失败。 默认情况下，1000。

## max\_ast\_elements {#max-ast-elements}

查询语法树中的最大元素数。 如果超出，将引发异常。
与前面的设置相同，只有在解析查询后才会检查它。 默认情况下，50,000。

## max\_rows\_in\_set {#max-rows-in-set}

从子查询创建的IN子句中数据集的最大行数。

## max\_bytes\_in\_set {#max-bytes-in-set}

从子查询创建的IN子句中的集合使用的最大字节数（未压缩数据）。

## set\_overflow\_mode {#set-overflow-mode}

当数据量超过其中一个限制时该怎么办: ‘throw’ 或 ‘break’. 默认情况下，扔。

## max\_rows\_in\_distinct {#max-rows-in-distinct}

使用DISTINCT时的最大不同行数。

## max\_bytes\_in\_distinct {#max-bytes-in-distinct}

使用DISTINCT时哈希表使用的最大字节数。

## distinct\_overflow\_mode {#distinct-overflow-mode}

当数据量超过其中一个限制时该怎么办: ‘throw’ 或 ‘break’. 默认情况下，扔。

## max\_rows\_to\_transfer {#max-rows-to-transfer}

使用GLOBAL IN时，可以传递到远程服务器或保存在临时表中的最大行数。

## max\_bytes\_to\_transfer {#max-bytes-to-transfer}

使用GLOBAL IN时，可以传递到远程服务器或保存在临时表中的最大字节数（未压缩数据）。

## transfer\_overflow\_mode {#transfer-overflow-mode}

当数据量超过其中一个限制时该怎么办: ‘throw’ 或 ‘break’. 默认情况下，扔。

[原始文章](https://clickhouse.tech/docs/en/operations/settings/query_complexity/) <!--hide-->
