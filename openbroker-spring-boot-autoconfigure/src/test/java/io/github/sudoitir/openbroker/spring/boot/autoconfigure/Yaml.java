package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.mock.env.MockEnvironment;

/** An environment from the YAML of an {@code application.yml}. */
final class Yaml {

    private Yaml() {}

    static MockEnvironment environment(String yaml) {
        MockEnvironment environment = new MockEnvironment();
        environment.getPropertySources().addFirst(source(yaml));
        environment.setProperty("test.password", TestSecrets.PASSWORD);
        environment.setProperty("test.token", TestSecrets.TOKEN);
        environment.setProperty("test.bad", TestSecrets.BAD);
        return environment;
    }

    static PropertySource<?> source(String yaml) {
        try {
            return new YamlPropertySourceLoader()
                    .load("test", new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8)))
                    .getFirst();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
