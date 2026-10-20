package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.artemis.ArtemisKinds;
import io.github.sudoitir.openbroker.artemis.ArtemisLists;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Placement;
import io.github.sudoitir.openbroker.model.Resource;
import io.github.sudoitir.openbroker.model.ResourceId;
import io.github.sudoitir.openbroker.model.ResourcePath;
import io.github.sudoitir.openbroker.model.ResourceQuery;
import io.github.sudoitir.openbroker.model.StandardAttributes;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FiltersTest {

    private static final ResourceId CONNECTION = new ResourceId(ArtemisKinds.CONNECTION, ResourcePath.of("c-1"));

    @Test
    void anExactNameIsTheBrokersOwnFilter() {
        assertThat(Filters.options(
                        ArtemisKinds.CONNECTION,
                        ResourceQuery.of(ArtemisKinds.CONNECTION).named("c-1")))
                .isEqualTo("{\"field\":\"connectionID\",\"operation\":\"EQUALS\",\"value\":\"c-1\"}");
    }

    @Test
    void anIdValuedSelectorFiltersOnTheIdsName() {
        ResourceQuery sessionsOfC1 = ResourceQuery.of(ArtemisKinds.SESSION)
                .where(Map.of(StandardAttributes.PARENT.name(), CONNECTION.toString()));

        assertThat(Filters.options(ArtemisKinds.SESSION, sessionsOfC1))
                .isEqualTo("{\"field\":\"connectionID\",\"operation\":\"EQUALS\",\"value\":\"c-1\"}");
    }

    @Test
    void aPatternOrNoSelectorReadsEveryRow() {
        assertThat(Filters.options(
                        ArtemisKinds.QUEUE, ResourceQuery.of(ArtemisKinds.QUEUE).named("orders.*")))
                .isEqualTo(ArtemisLists.ALL);
        assertThat(Filters.options(ArtemisKinds.QUEUE, ResourceQuery.of(ArtemisKinds.QUEUE)))
                .isEqualTo(ArtemisLists.ALL);
    }

    @Test
    void whatTheBrokerCouldNotFilterIsFilteredHere() {
        Resource matching = session("s-1", CONNECTION);
        Resource other = session("s-2", new ResourceId(ArtemisKinds.CONNECTION, ResourcePath.of("c-2")));

        List<Resource> kept = Filters.keep(
                ResourceQuery.of(ArtemisKinds.SESSION)
                        .where(Map.of(StandardAttributes.PARENT.name(), CONNECTION.toString())),
                List.of(matching, other));

        assertThat(kept).containsExactly(matching);
    }

    private static Resource session(String id, ResourceId parent) {
        return new Resource(
                new ResourceId(ArtemisKinds.SESSION, ResourcePath.of(parent.name(), id)),
                Attributes.builder().put(StandardAttributes.PARENT, parent).build(),
                List.of(),
                List.of(Placement.instance(NodeId.of("n"), Attributes.empty())),
                Instant.EPOCH);
    }
}
