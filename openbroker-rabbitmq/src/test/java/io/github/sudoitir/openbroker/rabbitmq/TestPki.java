package io.github.sudoitir.openbroker.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.util.Base64;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * A throwaway certificate authority with a broker certificate for {@code localhost} and the nodes' network aliases
 * and a client certificate, made with the JDK's {@code keytool} into PKCS12 stores in a temporary directory. The
 * broker's key and certificates are also written as PEM files ({@code broker.key}, {@code broker.pem},
 * {@code ca.pem}), which is the format RabbitMQ reads.
 */
public final class TestPki {

    /** The password of every store. */
    public static final String PASSWORD = "changeit";

    /** The subject name of the client certificate, which a user with that name is matched to over EXTERNAL. */
    public static final String CLIENT_NAME = "client";

    private static final String BROKER_SAN = "san=dns:localhost,dns:rabbit-1,dns:rabbit-2,dns:rabbit-3,ip:127.0.0.1";

    private final Path dir;

    /** Makes the authority and both certificates. */
    public TestPki() {
        try {
            dir = Files.createTempDirectory("openbroker-pki");
            keytool(
                    "-genkeypair",
                    "-alias",
                    "ca",
                    "-dname",
                    "CN=test-ca",
                    "-keyalg",
                    "RSA",
                    "-keysize",
                    "2048",
                    "-validity",
                    "2",
                    "-ext",
                    "bc:c",
                    "-keystore",
                    "ca.p12");
            keytool("-exportcert", "-alias", "ca", "-rfc", "-file", "ca.pem", "-keystore", "ca.p12");
            issue("broker", "CN=localhost", BROKER_SAN);
            issue(CLIENT_NAME, "CN=" + CLIENT_NAME, "san=dns:" + CLIENT_NAME);
            for (String side : new String[] {"broker-trust", "client-trust"}) {
                keytool("-importcert", "-noprompt", "-alias", "ca", "-file", "ca.pem", "-keystore", side + ".p12");
            }
            writeBrokerKey();
        } catch (IOException e) {
            throw new IllegalStateException("Could not make the certificates", e);
        }
    }

    private void issue(String name, String dn, String san) throws IOException {
        keytool(
                "-genkeypair",
                "-alias",
                name,
                "-dname",
                dn,
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-validity",
                "2",
                "-ext",
                san,
                "-keystore",
                name + ".p12");
        keytool("-certreq", "-alias", name, "-file", name + ".csr", "-keystore", name + ".p12");
        keytool(
                "-gencert",
                "-alias",
                "ca",
                "-infile",
                name + ".csr",
                "-outfile",
                name + ".pem",
                "-rfc",
                "-ext",
                san,
                "-validity",
                "2",
                "-keystore",
                "ca.p12");
        keytool("-importcert", "-noprompt", "-alias", "ca", "-file", "ca.pem", "-keystore", name + ".p12");
        keytool("-importcert", "-noprompt", "-alias", name, "-file", name + ".pem", "-keystore", name + ".p12");
    }

    /** Writes the broker's private key as an unencrypted PKCS#8 PEM, which Erlang's TLS reads. */
    private void writeBrokerKey() throws IOException {
        try {
            KeyStore keys = load("broker.p12");
            PrivateKey key = (PrivateKey) keys.getKey("broker", PASSWORD.toCharArray());
            String body = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                    .encodeToString(key.getEncoded());
            Files.writeString(
                    file("broker.key"), "-----BEGIN PRIVATE KEY-----\n" + body + "\n-----END PRIVATE KEY-----\n");
        } catch (GeneralSecurityException e) {
            throw new IOException("Could not export the broker key", e);
        }
    }

    private void keytool(String... args) throws IOException {
        String[] command = new String[args.length + 5];
        command[0] = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        System.arraycopy(args, 0, command, 1, args.length);
        command[args.length + 1] = "-storetype";
        command[args.length + 2] = "PKCS12";
        command[args.length + 3] = "-storepass";
        command[args.length + 4] = PASSWORD;
        Process process = new ProcessBuilder(command)
                .directory(dir.toFile())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        try {
            if (process.waitFor() != 0) {
                throw new IOException("keytool failed: " + output);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private KeyStore load(String name) throws IOException, GeneralSecurityException {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(file(name))) {
            store.load(in, PASSWORD.toCharArray());
        }
        return store;
    }

    /** The file {@code name} in the directory the certificates are in. */
    public Path file(String name) {
        return dir.resolve(name);
    }

    /** A TLS context that trusts the authority and, when {@code client} is true, presents the client certificate. */
    public SSLContext context(boolean client) {
        try {
            TrustManagerFactory trustManagers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(load("client-trust.p12"));
            KeyManagerFactory keyManagers = null;
            if (client) {
                keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagers.init(load(CLIENT_NAME + ".p12"), PASSWORD.toCharArray());
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(
                    keyManagers == null ? null : keyManagers.getKeyManagers(),
                    trustManagers.getTrustManagers(),
                    new SecureRandom());
            return context;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("Could not build the TLS context", e);
        }
    }
}
