package com.datahondo.flink.streaming.audit;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

/**
 * Pass-through {@link RichMapFunction} that increments a named
 * {@link LongCounter} accumulator for every {@link Row} that flows through it.
 *
 * <p>Place this operator immediately after {@code tableEnv.toDataStream(resultTable)}
 * to count transform-output records, and again just before the Kafka sink to count
 * target-written records.
 *
 * <p>The accumulator name must be one of the constants defined in
 * {@link AuditAccumulators} so that the orchestrator can retrieve it via
 * {@code JobClient.getAccumulators()}.
 */
@Slf4j
public class AuditCountingMapFunction extends RichMapFunction<Row, Row> {

    private final String accumulatorName;
    private transient LongCounter counter;

    /**
     * @param accumulatorName Flink accumulator key, e.g.
     *                        {@link AuditAccumulators#TRANSFORM_OUT}
     */
    public AuditCountingMapFunction(String accumulatorName) {
        this.accumulatorName = accumulatorName;
    }

    @Override
    public void open(Configuration parameters) {
        counter = getRuntimeContext().getLongCounter(accumulatorName);
    }

    @Override
    public Row map(Row value) {
        counter.add(1L);
        return value;
    }
}
