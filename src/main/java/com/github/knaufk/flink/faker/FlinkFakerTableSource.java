package com.github.knaufk.flink.faker;

import static com.github.knaufk.flink.faker.FlinkFakerTableSourceFactory.UNLIMITED_ROWS;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;

public class FlinkFakerTableSource
    implements ScanTableSource, LookupTableSource, SupportsLimitPushDown {

  private final FieldInfo[] fieldInfos;
  private ResolvedSchema schema;
  private long rowsPerSecond;
  private long numberOfRows;

  public FlinkFakerTableSource(
      FieldInfo[] fieldInfos, ResolvedSchema schema, long rowsPerSecond, long numberOfRows) {
    this.fieldInfos = fieldInfos;
    this.schema = schema;
    this.rowsPerSecond = rowsPerSecond;
    this.numberOfRows = numberOfRows;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext scanContext) {
    boolean isBounded = numberOfRows != UNLIMITED_ROWS;
    return SourceFunctionProvider.of(
        new FlinkFakerSourceFunction(fieldInfos, rowsPerSecond, numberOfRows), isBounded);
  }

  @Override
  public DynamicTableSource copy() {
    return new FlinkFakerTableSource(fieldInfos, schema, rowsPerSecond, numberOfRows);
  }

  @Override
  public String asSummaryString() {
    return "FlinkFakerSource";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    return TableFunctionProvider.of(new FlinkFakerLookupFunction(fieldInfos, context.getKeys()));
  }

  @Override
  public void applyLimit(long limit) {
    this.numberOfRows = limit;
  }
}
