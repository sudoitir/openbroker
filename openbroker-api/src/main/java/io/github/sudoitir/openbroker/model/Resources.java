package io.github.sudoitir.openbroker.model;

import io.github.sudoitir.openbroker.Partial;
import java.util.List;

/** Reading resources and their kinds. */
public interface Resources {

    /**
     * Answers several queries together, in as few round trips per node as the system allows, and
     * records each node that failed.
     *
     * @return one page per query, in the order of the queries
     */
    Partial<List<ResourcePage>> read(List<ResourceQuery> queries);

    /** Answers one query. */
    default Partial<ResourcePage> read(ResourceQuery query) {
        return read(List.of(query)).map(List::getFirst);
    }

    /** The kinds this cluster offers. */
    List<Kind> kinds();
}
