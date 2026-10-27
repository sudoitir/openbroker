package io.github.sudoitir.openbroker.spring.boot.autoconfigure;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.context.properties.source.ConfigurationPropertyState;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

/**
 * Binds the SDK's records component by component, over their own defaults.
 *
 * <p>Spring's constructor binding passes {@code null} for every component the configuration leaves
 * out, and the SDK's records reject {@code null}; a block that sets one key would fail. Here a component
 * that is not set keeps the value of the {@code base} record (the SDK's {@code defaults()}), and a record
 * component recurses with the base's value as its own base.
 *
 * <ul>
 *   <li>A key is the component's name in kebab case.
 *   <li>A record of one component (a node id) binds from a single value, through Spring's converters.
 *   <li>An {@code Optional<X>} binds {@code X} and wraps it.
 *   <li>A list of records binds element by element, and a list that is set replaces the base's.
 *   <li>A component typed by an interface cannot come from configuration: setting one fails.
 * </ul>
 */
final class RecordBinder {

    private static final String SDK_PACKAGE = "io.github.sudoitir.openbroker";

    private final Binder binder;
    private final Iterable<ConfigurationPropertySource> sources;
    private final String connection;

    RecordBinder(Environment environment, String connection) {
        this.binder = Binder.get(environment);
        this.sources = ConfigurationPropertySources.get(environment);
        this.connection = connection;
    }

    /** Binds {@code type} from under {@code prefix}; what is not set comes from {@code base}. */
    <T extends Record> T bind(String prefix, Class<T> type, T base) {
        return type.cast(bindRecord(ConfigurationPropertyName.of(prefix), type, base));
    }

    /** Whether any configuration property sits at or under {@code prefix}. */
    boolean isSet(String prefix) {
        return isSet(ConfigurationPropertyName.of(prefix));
    }

    private @Nullable Object bindValue(ConfigurationPropertyName name, ResolvableType type, @Nullable Object base) {
        Class<?> raw = type.toClass();
        if (raw == Optional.class) {
            return bindOptional(name, type.getGeneric(0), base);
        }
        if (isStructured(raw)) {
            return isSet(name) || base == null ? bindRecord(name, raw, base) : base;
        }
        if (Collection.class.isAssignableFrom(raw)
                && isStructured(type.getGeneric(0).toClass())) {
            return bindList(name, type.getGeneric(0).toClass(), base);
        }
        if (isCode(raw)) {
            if (isSet(name)) {
                throw new ConnectionConfigurationException(
                        connection, key(name), "cannot be set in configuration; set it in code");
            }
            return base;
        }
        return bindLeaf(name, type, base);
    }

    private @Nullable Object bindOptional(ConfigurationPropertyName name, ResolvableType inner, @Nullable Object base) {
        if (!isSet(name)) {
            return base != null ? base : Optional.empty();
        }
        Object baseInner = base instanceof Optional<?> present ? present.orElse(null) : null;
        return Optional.ofNullable(bindValue(name, inner, baseInner));
    }

    private @Nullable Object bindList(ConfigurationPropertyName name, Class<?> element, @Nullable Object base) {
        List<Object> bound = new ArrayList<>();
        for (int i = 0; isSet(name.append("[" + i + "]")); i++) {
            bound.add(bindRecord(name.append("[" + i + "]"), element, null));
        }
        return bound.isEmpty() ? base : List.copyOf(bound);
    }

    private @Nullable Object bindLeaf(ConfigurationPropertyName name, ResolvableType type, @Nullable Object base) {
        Class<?> raw = ClassUtils.resolvePrimitiveIfNecessary(type.toClass());
        ResolvableType target = raw == type.toClass() ? type : ResolvableType.forClass(raw);
        try {
            Object bound = binder.bind(name, Bindable.of(target)).orElse(null);
            if (bound == null) {
                return base;
            }
            if (bound instanceof Map<?, ?> map && base instanceof Map<?, ?> baseMap) {
                Map<Object, Object> merged = new LinkedHashMap<>(baseMap);
                merged.putAll(map);
                return merged;
            }
            return bound;
        } catch (BindException e) {
            throw new ConnectionConfigurationException(connection, key(name), "is not " + describe(raw), e);
        }
    }

    private @Nullable Object bindRecord(ConfigurationPropertyName name, Class<?> type, @Nullable Object base) {
        RecordComponent[] components = type.getRecordComponents();
        Object[] arguments = new Object[components.length];
        Class<?>[] types = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            types[i] = component.getType();
            ConfigurationPropertyName child = name.append(kebab(component.getName()));
            arguments[i] = bindValue(child, ResolvableType.forType(component.getGenericType()), read(component, base));
        }
        try {
            return type.getDeclaredConstructor(types).newInstance(arguments);
        } catch (InvocationTargetException e) {
            throw invalid(name, components, e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot build " + type.getName(), e);
        }
    }

    private ConnectionConfigurationException invalid(
            ConfigurationPropertyName name, RecordComponent[] components, @Nullable Throwable cause) {
        String message = cause == null || cause.getMessage() == null ? "is invalid" : cause.getMessage();
        for (RecordComponent component : components) {
            if (message.startsWith(component.getName())) {
                String problem = cause instanceof NullPointerException ? "is required" : message;
                return new ConnectionConfigurationException(
                        connection, key(name.append(kebab(component.getName()))), problem, cause);
            }
        }
        return new ConnectionConfigurationException(connection, key(name), message, cause);
    }

    private static @Nullable Object read(RecordComponent component, @Nullable Object base) {
        if (base == null) {
            return null;
        }
        try {
            return component.getAccessor().invoke(base);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot read " + component.getName(), e);
        }
    }

    private boolean isSet(ConfigurationPropertyName name) {
        for (ConfigurationPropertySource source : sources) {
            if (source.getConfigurationProperty(name) != null
                    || source.containsDescendantOf(name) == ConfigurationPropertyState.PRESENT) {
                return true;
            }
        }
        return false;
    }

    private static String key(ConfigurationPropertyName name) {
        return name.toString();
    }

    private static String describe(Class<?> type) {
        if (type.isEnum()) {
            return "one of "
                    + Arrays.stream(type.getEnumConstants())
                            .map(constant ->
                                    constant.toString().toLowerCase(Locale.ROOT).replace('_', '-'))
                            .toList();
        }
        return "a valid " + type.getSimpleName();
    }

    /**
     * A record of several components binds by component; an SDK record of a single component (a node id)
     * binds as a value. The starter's own records are always structured.
     */
    private static boolean isStructured(Class<?> type) {
        return type.isRecord()
                && (type.getRecordComponents().length > 1
                        || type.getPackageName().equals(RecordBinder.class.getPackageName()));
    }

    /** The SDK's interfaces (a send target, a hook) are code, not configuration. */
    private static boolean isCode(Class<?> type) {
        return type.getPackageName().startsWith(SDK_PACKAGE)
                && (type.isInterface() || Modifier.isAbstract(type.getModifiers()));
    }

    static String kebab(String camel) {
        StringBuilder out = new StringBuilder();
        for (char c : camel.toCharArray()) {
            if (Character.isUpperCase(c)) {
                out.append('-').append(Character.toLowerCase(c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    /** The key paths {@code type} binds, relative to its prefix: what the configuration metadata lists. */
    static Set<String> keyPaths(Class<?> type) {
        Set<String> paths = new TreeSet<>();
        collect("", ResolvableType.forClass(type), paths);
        return paths;
    }

    private static void collect(String path, ResolvableType type, Set<String> paths) {
        Class<?> raw = type.toClass();
        if (raw == Optional.class) {
            collect(path, type.getGeneric(0), paths);
        } else if (isStructured(raw)) {
            for (RecordComponent component : raw.getRecordComponents()) {
                String child = (path.isEmpty() ? "" : path + ".") + kebab(component.getName());
                collect(child, ResolvableType.forType(component.getGenericType()), paths);
            }
        } else if (Collection.class.isAssignableFrom(raw)
                && isStructured(type.getGeneric(0).toClass())) {
            collect(path + "[]", type.getGeneric(0), paths);
        } else if (!isCode(raw)) {
            paths.add(path);
        }
    }
}
