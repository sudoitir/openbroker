package io.github.sudoitir.openbroker.benchmarks;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * Runs the benchmarks and compares them with {@code budgets.json}.
 *
 * <ul>
 *   <li>A baseline without an {@code allocation} is not gated on it, though the measurement is still
 *       printed and recorded. That is for a benchmark whose allocation per operation is mostly the
 *       background threads of a client talking to a broker in another process, divided by a rate the
 *       broker sets, so it measures the machine and not the SDK.
 *   <li>Allocation ({@code gc.alloc.rate.norm}, bytes per operation) is a hard gate at the
 *       baseline plus the larger of 10% and 64 B. It is deterministic.
 *   <li>Throughput may not fall below 50% of the baseline; p99 latency may not exceed twice it.
 *       Shared runners are noisy, so these catch collapses, not drift.
 * </ul>
 *
 * A benchmark with no baseline prints its measured values and passes. {@code -Dbudgets.record=path}
 * writes the measured values as a fresh budgets file, {@code -Dbudgets.json=path} writes JMH's own
 * result file, and {@code -Dbudgets.include=regex} runs a subset. The exit status is non-zero on a
 * breach.
 */
public final class Budgets {

    private static final String ALLOCATION = "gc.alloc.rate.norm";
    private static final double ALLOCATION_RELATIVE = 0.10;
    private static final double ALLOCATION_ABSOLUTE = 64;
    private static final double THROUGHPUT_FLOOR = 0.5;
    private static final double LATENCY_CEILING = 2.0;
    private static final String NONE = "ObserverBench.sendWithoutObserver";
    private static final String COUNTING = "ObserverBench.sendWithCountingObserver";

    /** What one benchmark measured: ops/s, or the p99 in microseconds for a sampled one. */
    record Measured(String name, boolean latency, double score, double allocation) {}

    /** The baseline a benchmark is compared with. */
    record Baseline(double score, @Nullable Double allocation) {}

    private Budgets() {}

    public static void main(String[] args) throws Exception {
        OptionsBuilder options = new OptionsBuilder();
        options.include(System.getProperty("budgets.include", "io\\.github\\.sudoitir\\.openbroker\\..*Bench\\..*"))
                .forks(1)
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(2))
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(2))
                .addProfiler(GCProfiler.class);
        String json = System.getProperty("budgets.json");
        if (json != null) {
            options.resultFormat(ResultFormatType.JSON).result(json);
        }
        Collection<RunResult> results = new Runner(options.build()).run();

        List<Measured> measured = new ArrayList<>();
        for (RunResult result : results) {
            measured.add(measure(result));
        }
        String record = System.getProperty("budgets.record");
        if (record != null) {
            record(Path.of(record), measured);
        }
        Map<String, Baseline> baselines = baselines();
        List<String> breaches = new ArrayList<>();
        System.out.println();
        System.out.printf(
                "%-40s %14s %10s %12s %10s  %s%n", "benchmark", "score", "B/op", "base score", "base B/op", "verdict");
        for (Measured m : measured) {
            Baseline base = baselines.get(m.name());
            String verdict = verdict(m, base, breaches);
            System.out.printf(
                    "%-40s %14.1f %10.1f %12s %10s  %s%n",
                    m.name(),
                    m.score(),
                    m.allocation(),
                    base == null ? "-" : String.format("%.1f", base.score()),
                    base == null
                            ? "-"
                            : base.allocation() == null ? "not gated" : String.format("%.1f", base.allocation()),
                    verdict);
        }
        noneAddsNothing(measured, breaches);
        if (!breaches.isEmpty()) {
            System.out.println();
            breaches.forEach(b -> System.out.println("BREACH " + b));
            System.exit(1);
        }
        System.out.println("All budgets hold.");
    }

    private static Measured measure(RunResult result) {
        String full = result.getParams().getBenchmark();
        String name = full.substring(full.lastIndexOf('.', full.lastIndexOf('.') - 1) + 1);
        boolean latency = result.getParams().getMode().shortLabel().equals("sample");
        double score = latency
                ? result.getPrimaryResult().getStatistics().getPercentile(99)
                : result.getPrimaryResult().getScore();
        Result alloc = result.getSecondaryResults().get(ALLOCATION);
        if (alloc == null) {
            throw new IllegalStateException(name + " reported no " + ALLOCATION);
        }
        return new Measured(name, latency, score, alloc.getScore());
    }

    private static String verdict(Measured m, @Nullable Baseline base, List<String> breaches) {
        if (base == null) {
            return "no baseline (passes)";
        }
        List<String> problems = new ArrayList<>();
        if (base.allocation() != null) {
            double allocationLimit =
                    base.allocation() + Math.max(base.allocation() * ALLOCATION_RELATIVE, ALLOCATION_ABSOLUTE);
            if (m.allocation() > allocationLimit) {
                problems.add(String.format("allocation %.1f B/op over %.1f", m.allocation(), allocationLimit));
            }
        }
        if (m.latency()) {
            double ceiling = base.score() * LATENCY_CEILING;
            if (m.score() > ceiling) {
                problems.add(String.format("p99 %.1f us over %.1f", m.score(), ceiling));
            }
        } else {
            double floor = base.score() * THROUGHPUT_FLOOR;
            if (m.score() < floor) {
                problems.add(String.format("throughput %.1f ops/s under %.1f", m.score(), floor));
            }
        }
        problems.forEach(p -> breaches.add(m.name() + ": " + p));
        return problems.isEmpty() ? "ok" : "BREACH";
    }

    /**
     * {@code MessagingObserver.none()} adds no bytes, so it allocates no more than a counting observer,
     * within the allocation slack of a budget.
     */
    private static void noneAddsNothing(List<Measured> measured, List<String> breaches) {
        Measured none = find(measured, NONE);
        Measured counting = find(measured, COUNTING);
        if (none != null && counting != null && none.allocation() > counting.allocation() + ALLOCATION_ABSOLUTE) {
            breaches.add(String.format(
                    "%s allocates %.1f B/op, more than %s (%.1f)",
                    NONE, none.allocation(), COUNTING, counting.allocation()));
        }
    }

    private static @Nullable Measured find(List<Measured> measured, String name) {
        return measured.stream().filter(m -> m.name().equals(name)).findFirst().orElse(null);
    }

    private static Map<String, Baseline> baselines() throws IOException {
        Map<String, Baseline> baselines = new LinkedHashMap<>();
        try (InputStream in = Budgets.class.getResourceAsStream("/budgets.json")) {
            if (in == null) {
                return baselines;
            }
            JsonNode root = mapper().readTree(in).path("benchmarks");
            for (Map.Entry<String, JsonNode> e : root.properties()) {
                baselines.put(
                        e.getKey(),
                        new Baseline(
                                e.getValue().path("score").asDouble(),
                                e.getValue().has("allocation")
                                        ? e.getValue().path("allocation").asDouble()
                                        : null));
            }
        }
        return baselines;
    }

    private static void record(Path file, List<Measured> measured) throws IOException {
        Map<String, Object> benchmarks = new LinkedHashMap<>();
        for (Measured m : measured) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("score", Math.round(m.score() * 10) / 10.0);
            entry.put("unit", m.latency() ? "p99 us" : "ops/s");
            entry.put("allocation", Math.round(m.allocation() * 10) / 10.0);
            benchmarks.put(m.name(), entry);
        }
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("comment", "Recorded by Budgets with -Dbudgets.record. Allocation is bytes per operation.");
        root.put("benchmarks", benchmarks);
        Files.writeString(file, mapper().writeValueAsString(root) + System.lineSeparator());
        System.out.println("Recorded " + file);
    }

    private static ObjectMapper mapper() {
        return JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build();
    }
}
