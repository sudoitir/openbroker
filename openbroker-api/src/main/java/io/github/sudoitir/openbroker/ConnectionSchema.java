package io.github.sudoitir.openbroker;

import java.util.List;
import java.util.Optional;

/** The connection options a driver accepts, in the Dapr component-metadata style. */
public record ConnectionSchema(List<OptionSpec> options) {

    public ConnectionSchema {
        options = List.copyOf(options);
    }

    public Optional<OptionSpec> option(String key) {
        return options.stream().filter(o -> o.key().equals(key)).findFirst();
    }

    public boolean isSensitive(String key) {
        return option(key).map(OptionSpec::sensitive).orElse(true);
    }
}
