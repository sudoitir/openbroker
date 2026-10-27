package io.github.sudoitir.openbroker.kafka.messaging;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * A throwaway certificate authority with a broker certificate for {@code localhost} and a client
 * certificate, made with the JDK's {@code keytool} into PKCS12 stores in a temporary directory.
 */
final class TestPki {

    static final String PASSWORD = "changeit";

    private final Path dir;

    TestPki() {
        try {
            dir = Files.createTempDirectory("openbroker-pki");
            Files.writeString(dir.resolve("credentials"), PASSWORD);
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
            issue("broker", "CN=localhost", "san=dns:localhost,ip:127.0.0.1");
            issue("client", "CN=client", "san=dns:client");
            for (String side : new String[] {"broker-trust", "client-trust"}) {
                keytool("-importcert", "-noprompt", "-alias", "ca", "-file", "ca.pem", "-keystore", side + ".p12");
            }
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
        String output = new String(process.getInputStream().readAllBytes());
        try {
            if (process.waitFor() != 0) {
                throw new IOException("keytool failed: " + output);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    Path file(String name) {
        return dir.resolve(name);
    }

    /** A TLS context that trusts the authority and, when {@code client} is true, presents the client certificate. */
    SSLContext context(boolean client) {
        try {
            KeyStore trust = KeyStore.getInstance("PKCS12");
            try (var in = Files.newInputStream(file("client-trust.p12"))) {
                trust.load(in, PASSWORD.toCharArray());
            }
            TrustManagerFactory trustManagers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(trust);
            KeyManagerFactory keyManagers = null;
            if (client) {
                KeyStore keys = KeyStore.getInstance("PKCS12");
                try (var in = Files.newInputStream(file("client.p12"))) {
                    keys.load(in, PASSWORD.toCharArray());
                }
                keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagers.init(keys, PASSWORD.toCharArray());
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(
                    keyManagers == null ? null : keyManagers.getKeyManagers(),
                    trustManagers.getTrustManagers(),
                    new SecureRandom());
            return context;
        } catch (Exception e) {
            throw new IllegalStateException("Could not build the TLS context", e);
        }
    }
}
