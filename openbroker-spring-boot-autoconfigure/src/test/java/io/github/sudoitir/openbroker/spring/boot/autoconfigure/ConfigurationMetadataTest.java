package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class ConfigurationMetadataTest {

    private static final String CONNECTIONS = "openbroker.connections.*.";

    private static Map<String, String> descriptions() throws IOException {
        Map<String, String> found = new TreeMap<>();
        try (InputStream in = ConfigurationMetadataTest.class
                .getClassLoader()
                .getResourceAsStream("META-INF/spring-configuration-metadata.json")) {
            JsonNode properties = JsonMapper.builder().build().readTree(in).get("properties");
            properties.forEach(p -> found.put(
                    p.get("name").asString(),
                    p.has("description") ? p.get("description").asString() : ""));
        }
        return found;
    }

    @Test
    void everyKeyIsDescribed() throws IOException {
        assertThat(descriptions())
                .allSatisfy(
                        (name, description) -> assertThat(description).as(name).isNotBlank());
    }

    @Test
    void listsExactlyTheKeysAConnectionBinds() throws IOException {
        Set<String> listed = new TreeSet<>();
        descriptions().keySet().stream()
                .filter(name -> name.startsWith(CONNECTIONS))
                .forEach(name -> listed.add(name.substring(CONNECTIONS.length())));

        assertThat(listed).isEqualTo(RecordBinder.keyPaths(Connection.class));
    }

    @Test
    void listsExactlyTheStartersOwnKeys() throws IOException {
        Set<String> listed = new TreeSet<>(descriptions().keySet());
        listed.removeIf(name -> name.startsWith("openbroker.connections"));
        Set<String> bindable = new TreeSet<>();
        RecordBinder.keyPaths(OpenBrokerProperties.class).forEach(path -> bindable.add("openbroker." + path));

        assertThat(listed).isEqualTo(bindable);
    }
}
