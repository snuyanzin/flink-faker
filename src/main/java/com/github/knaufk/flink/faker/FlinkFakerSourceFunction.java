package com.github.knaufk.flink.faker;

import static com.github.knaufk.flink.faker.FlinkFakerTableSourceFactory.UNLIMITED_ROWS;

import net.datafaker.Faker;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

public class FlinkFakerSourceFunction extends RichParallelSourceFunction<RowData> {

  private volatile boolean cancelled;
  private Faker faker;
  private FieldInfo[] fieldInfos;
  private long rowsPerSecond;
  private long numberOfRows;

  public FlinkFakerSourceFunction(FieldInfo[] fieldInfos, long rowsPerSecond, long numberOfRows) {
    this.fieldInfos = fieldInfos;
    this.rowsPerSecond = rowsPerSecond;
    this.numberOfRows = numberOfRows;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    faker = new FlinkFaker();
  }

  @Override
  public void run(final SourceContext<RowData> sourceContext) throws Exception {

    final long rowsForSubtask = getRowsForThisSubTask();
    final long rowsPerSecondForSubtask = getRowsPerSecondForSubTask();
    long rowsSoFar = 0;

    long nextReadTime = System.currentTimeMillis();
    while (!cancelled && rowsSoFar < rowsForSubtask) {
      for (long i = 0; i < rowsPerSecondForSubtask; i++) {
        if (!cancelled && rowsSoFar < rowsForSubtask) {
          RowData row = generateNextRow();
          sourceContext.collect(row);
          rowsSoFar++;
        }
      }
      nextReadTime += 1000;
      long toWaitMs = Math.max(0, nextReadTime - System.currentTimeMillis());
      Thread.sleep(toWaitMs);
    }
  }

  private long getRowsPerSecondForSubTask() {
    int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    long baseRowsPerSecondPerSubtask = rowsPerSecond / numSubtasks;
    return (rowsPerSecond % numSubtasks > indexOfThisSubtask)
        ? baseRowsPerSecondPerSubtask + 1
        : baseRowsPerSecondPerSubtask;
  }

  private long getRowsForThisSubTask() {
    if (numberOfRows == UNLIMITED_ROWS) {
      return Long.MAX_VALUE;
    } else {
      int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
      int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
      final long baseNumOfRowsPerSubtask = numberOfRows / numSubtasks;
      return (numberOfRows % numSubtasks > indexOfThisSubtask)
          ? baseNumOfRowsPerSubtask + 1
          : baseNumOfRowsPerSubtask;
    }
  }

  @Override
  public void cancel() {
    cancelled = true;
  }

  @VisibleForTesting
  RowData generateNextRow() {
    GenericRowData row = new GenericRowData(fieldInfos.length);
    for (int i = 0; i < fieldInfos.length; i++) {
      row.setField(i, FakerUtils.stringValueToType(fieldInfos[i], f -> faker.expression(f)));
    }
    return row;
  }
}
