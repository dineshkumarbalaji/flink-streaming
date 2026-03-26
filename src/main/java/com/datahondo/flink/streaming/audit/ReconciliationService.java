package com.datahondo.flink.streaming.audit;

import com.datahondo.flink.streaming.config.ReconciliationConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Computes and emits count-based reconciliation reports.
 *
 * <p>Input counts are supplied as a simple {@link Counts} value object populated
 * by the orchestrator after reading Flink {@code LongCounter} accumulators via
 * {@code JobClient.getAccumulators()}.
 *
 * <p>Reconciliation logic:
 * <ul>
 *   <li>A report is flagged as <em>reconciled</em> ({@code true}) when:
 *     <ul>
 *       <li>No rejected records exist (schemaRejectedCount == 0), AND</li>
 *       <li>targetWrittenCount &ge; transformedCount * (1 - tolerancePercent/100)</li>
 *     </ul>
 *   </li>
 *   <li>Every violated condition is described as a human-readable string in
 *       {@link ReconciliationReport#getDiscrepancies()}.</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReconciliationService {

    private final AuditSinkFactory sinkFactory;

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Computes a reconciliation report from the supplied counts and emits it
     * via the configured {@link ReconciliationSink}.
     *
     * @param runContext   the run's context (provides config and runId)
     * @param windowStart  start of the aggregation window
     * @param counts       accumulator values collected from the Flink job
     * @return the computed report (also emitted to the sink)
     */
    public ReconciliationReport reconcile(RunContext runContext,
                                          Instant windowStart,
                                          Counts counts) {
        ReconciliationConfig cfg = runContext.getReconciliationConfig();
        Instant windowEnd = Instant.now();
        String windowLabel = (cfg != null) ? cfg.getWindow() : "n/a";
        double tolerancePct = (cfg != null) ? cfg.getTolerancePercent() : 0.0;

        List<String> discrepancies = new ArrayList<>();
        checkDiscrepancies(counts, tolerancePct, discrepancies);

        ReconciliationReport report = ReconciliationReport.builder()
                .runId(runContext.getRunId())
                .jobName(runContext.getJobName())
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .windowLabel(windowLabel)
                .sourceReadCount(counts.sourceRead)
                .schemaRejectedCount(counts.schemaRejected)
                .transformedCount(counts.transformed)
                .targetWrittenCount(counts.targetWritten)
                .reconciled(discrepancies.isEmpty())
                .discrepancies(discrepancies)
                .build();

        if (runContext.isReconciliationEnabled() && cfg != null) {
            ReconciliationSink sink = sinkFactory.createReconciliationSink(cfg);
            sink.emit(report);
        }

        return report;
    }

    // ── Discrepancy logic ─────────────────────────────────────────────────────

    private void checkDiscrepancies(Counts counts, double tolerancePct, List<String> out) {
        if (counts.schemaRejected > 0) {
            out.add(String.format("Schema validation rejected %d record(s)", counts.schemaRejected));
        }

        if (counts.transformed > 0) {
            long gap = counts.transformed - counts.targetWritten;
            if (gap > 0) {
                double gapPct = 100.0 * gap / counts.transformed;
                if (gapPct > tolerancePct) {
                    out.add(String.format(
                            "Target lag: %d records transformed but only %d written "
                            + "(%.2f%% loss, tolerance=%.2f%%)",
                            counts.transformed, counts.targetWritten, gapPct, tolerancePct));
                }
            } else if (gap < 0) {
                out.add(String.format(
                        "Unexpected duplication: %d written > %d transformed",
                        counts.targetWritten, counts.transformed));
            }
        }

        long expectedInput = counts.sourceRead - counts.schemaRejected;
        if (expectedInput < 0) {
            out.add(String.format(
                    "Invalid state: rejectedCount(%d) > sourceReadCount(%d)",
                    counts.schemaRejected, counts.sourceRead));
        }
    }

    // ── Helper: build Counts from raw accumulator map ─────────────────────────

    /**
     * Extracts reconciliation counts from the raw accumulator map returned by
     * {@code JobClient.getAccumulators().get()}.
     *
     * <p>Unknown / missing accumulator keys default to 0.
     *
     * @param accumulators raw accumulator map from Flink
     * @param sourceReadKey     accumulator name for source-read count
     * @param sourceRejectedKey accumulator name for schema-rejected count
     * @param targetWrittenKey  accumulator name for target-written count
     */
    public static Counts countsFromAccumulators(Map<String, Object> accumulators,
                                                String sourceReadKey,
                                                String sourceRejectedKey,
                                                String targetWrittenKey) {
        return new Counts(
                extractLong(accumulators, sourceReadKey),
                extractLong(accumulators, sourceRejectedKey),
                extractLong(accumulators, AuditAccumulators.TRANSFORM_OUT),
                extractLong(accumulators, targetWrittenKey));
    }

    private static long extractLong(Map<String, Object> map, String key) {
        if (map == null || key == null) return 0L;
        Object val = map.get(key);
        if (val instanceof Long) return (Long) val;
        if (val instanceof Number) return ((Number) val).longValue();
        return 0L;
    }

    // ── Value object ──────────────────────────────────────────────────────────

    /**
     * Immutable snapshot of pipeline record counts for a reconciliation window.
     */
    public static final class Counts {
        public final long sourceRead;
        public final long schemaRejected;
        public final long transformed;
        public final long targetWritten;

        public Counts(long sourceRead, long schemaRejected,
                      long transformed, long targetWritten) {
            this.sourceRead     = sourceRead;
            this.schemaRejected = schemaRejected;
            this.transformed    = transformed;
            this.targetWritten  = targetWritten;
        }

        /** Convenience factory for test usage. */
        public static Counts of(long sourceRead, long schemaRejected,
                                long transformed, long targetWritten) {
            return new Counts(sourceRead, schemaRejected, transformed, targetWritten);
        }

        @Override
        public String toString() {
            return String.format("Counts{src=%d, rej=%d, xfm=%d, tgt=%d}",
                    sourceRead, schemaRejected, transformed, targetWritten);
        }
    }
}
