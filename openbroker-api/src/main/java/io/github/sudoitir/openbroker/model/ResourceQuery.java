package io.github.sudoitir.openbroker.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A list request: which resources, filtered how, which attributes, in what order, where, and how
 * many, continuing from a previous page. Pagination is Kubernetes list chunking: pass the previous
 * page's continue token.
 *
 * @param resourceClass the class to list, when no kind is named
 * @param kind the kind to list
 * @param namePattern a glob over the resource name ({@code *} and {@code ?}), when present
 * @param fieldSelector attribute equality filters
 * @param projection the attributes wanted; empty means every attribute
 * @param orderBy sort keys the kind declares in {@link Kind#orderings()}
 * @param limit the page size; 0 means the driver's default
 */
public record ResourceQuery(
        Optional<ResourceClass> resourceClass,
        Optional<KindId> kind,
        Optional<String> namePattern,
        Map<String, String> fieldSelector,
        Set<String> projection,
        List<Ordering> orderBy,
        Scope scope,
        int limit,
        Optional<String> continueToken) {

    public ResourceQuery {
        Objects.requireNonNull(resourceClass, "resourceClass");
        Objects.requireNonNull(kind, "kind");
        if (resourceClass.isEmpty() == kind.isEmpty()) {
            throw new IllegalArgumentException("A query names either a resource class or a kind");
        }
        Objects.requireNonNull(namePattern, "namePattern");
        fieldSelector = Map.copyOf(fieldSelector);
        projection = Set.copyOf(projection);
        orderBy = List.copyOf(orderBy);
        Objects.requireNonNull(scope, "scope");
        if (limit < 0) {
            throw new IllegalArgumentException("limit must not be negative");
        }
        Objects.requireNonNull(continueToken, "continueToken");
    }

    public static ResourceQuery of(ResourceClass resourceClass) {
        return new ResourceQuery(
                Optional.of(resourceClass),
                Optional.empty(),
                Optional.empty(),
                Map.of(),
                Set.of(),
                List.of(),
                Scope.whole(),
                0,
                Optional.empty());
    }

    public static ResourceQuery of(KindId kind) {
        return new ResourceQuery(
                Optional.empty(),
                Optional.of(kind),
                Optional.empty(),
                Map.of(),
                Set.of(),
                List.of(),
                Scope.whole(),
                0,
                Optional.empty());
    }

    public ResourceQuery named(String pattern) {
        return new ResourceQuery(
                resourceClass,
                kind,
                Optional.of(pattern),
                fieldSelector,
                projection,
                orderBy,
                scope,
                limit,
                continueToken);
    }

    public ResourceQuery where(Map<String, String> fieldSelector) {
        return new ResourceQuery(
                resourceClass, kind, namePattern, fieldSelector, projection, orderBy, scope, limit, continueToken);
    }

    public ResourceQuery project(Set<String> projection) {
        return new ResourceQuery(
                resourceClass, kind, namePattern, fieldSelector, projection, orderBy, scope, limit, continueToken);
    }

    public ResourceQuery orderBy(List<Ordering> orderBy) {
        return new ResourceQuery(
                resourceClass, kind, namePattern, fieldSelector, projection, orderBy, scope, limit, continueToken);
    }

    public ResourceQuery in(Scope scope) {
        return new ResourceQuery(
                resourceClass, kind, namePattern, fieldSelector, projection, orderBy, scope, limit, continueToken);
    }

    public ResourceQuery limit(int limit) {
        return new ResourceQuery(
                resourceClass, kind, namePattern, fieldSelector, projection, orderBy, scope, limit, continueToken);
    }

    public ResourceQuery continueFrom(String token) {
        return new ResourceQuery(
                resourceClass, kind, namePattern, fieldSelector, projection, orderBy, scope, limit, Optional.of(token));
    }
}
