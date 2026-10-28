package io.github.sudoitir.openbroker.kafka.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.sudoitir.openbroker.BrokerException;
import io.github.sudoitir.openbroker.ErrorReasons;
import io.github.sudoitir.openbroker.Status;
import io.github.sudoitir.openbroker.access.Assessment;
import io.github.sudoitir.openbroker.kafka.messaging.ShareFeatures;
import java.util.Map;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class ClusterFeaturesTest {

    private static final ClusterFeatures.Level OFF = new ClusterFeatures.Level(0, 0);

    private static ClusterFeatures features(
            boolean kraft, String release, OptionalInt level, ClusterFeatures.Level group) {
        return new ClusterFeatures(kraft, release, level, null, group, OFF, OFF, OFF, new ShareFeatures(0, 0, false));
    }

    @Test
    void eachReleaseIsToldApartByTheNewestLevelItsSoftwareSupports() {
        assertThat(ClusterFeatures.releaseOf(7)).isEqualTo("3.3");
        assertThat(ClusterFeatures.releaseOf(11)).isEqualTo("3.5");
        assertThat(ClusterFeatures.releaseOf(19)).isEqualTo("3.7");
        assertThat(ClusterFeatures.releaseOf(21)).isEqualTo("3.9");
        assertThat(ClusterFeatures.releaseOf(25)).isEqualTo("4.0");
        assertThat(ClusterFeatures.releaseOf(27)).isEqualTo("4.1");
    }

    @Test
    void theNewestKnownReleaseIsOnlyAtLeast() {
        assertThat(ClusterFeatures.releaseOf(33)).isEqualTo("at least 4.4");
        assertThat(ClusterFeatures.releaseOf(34)).isEqualTo("at least 4.4");
    }

    @Test
    void aLevelBelowTheTableIsNoRelease() {
        assertThat(ClusterFeatures.releaseOf(6)).isNull();
    }

    @Test
    void aClusterBeforeThreeZeroIsRefusedNamingWhatWasFound() {
        ClusterFeatures old = new ClusterFeatures(
                false,
                "before 3.0",
                OptionalInt.empty(),
                "it reports no metadata.version and does not list transactions",
                OFF,
                OFF,
                OFF,
                OFF,
                new ShareFeatures(0, 0, false));

        assertThatThrownBy(() -> old.require("[3.0.0,)")).isInstanceOfSatisfying(BrokerException.class, e -> {
            assertThat(e.status()).isEqualTo(Status.FAILED_PRECONDITION);
            assertThat(e.reason()).isEqualTo(ErrorReasons.UNSUPPORTED_VERSION);
            assertThat(e.getMessage())
                    .contains("before 3.0")
                    .contains("does not list transactions")
                    .contains("[3.0.0,)");
        });
    }

    @Test
    void aSupportedClusterIsDescribedByReleaseAndMode() {
        assertThat(features(false, "at least 3.0", OptionalInt.empty(), OFF).describe())
                .isEqualTo("at least 3.0, ZooKeeper mode");
        assertThat(features(true, "4.0", OptionalInt.of(25), OFF).describe()).isEqualTo("4.0, KRaft mode");
        features(true, "4.0", OptionalInt.of(25), OFF).require("[3.0.0,)");
    }

    @Test
    void aZooKeeperClusterLacksTheQuorumTheConsumerProtocolAndFencedBrokers() {
        Map<io.github.sudoitir.openbroker.access.Capability, Assessment> out =
                features(false, "3.9", OptionalInt.of(21), OFF).assessments();

        assertThat(out.get(KafkaAccess.QUORUM).status()).isEqualTo(Assessment.Status.UNAVAILABLE);
        assertThat(out.get(KafkaAccess.QUORUM).reason())
                .contains("ZooKeeper mode")
                .contains("KRaft controller quorum");
        assertThat(out.get(KafkaAccess.CONSUMER_PROTOCOL_GROUPS).reason()).contains("finalizes it at 0");
        assertThat(out.get(KafkaAccess.CONSUMER_PROTOCOL_GROUPS).remedy()).isEmpty();
        assertThat(out.get(KafkaAccess.FENCED_BROKERS).reason()).contains("before 4.0");
        assertThat(out.get(KafkaAccess.SHARE_GROUPS).status()).isEqualTo(Assessment.Status.UNAVAILABLE);
        assertThat(out.get(KafkaAccess.STREAMS_GROUPS).status()).isEqualTo(Assessment.Status.UNAVAILABLE);
        assertThat(out.get(KafkaAccess.ELIGIBLE_LEADERS).status()).isEqualTo(Assessment.Status.UNAVAILABLE);
    }

    @Test
    void aKraftClusterOfKafkaFourOffersTheConsumerProtocolAndNamesTheRemedyWhereItIsOff() {
        ClusterFeatures on = features(true, "4.0", OptionalInt.of(25), new ClusterFeatures.Level(1, 1));
        ClusterFeatures off = features(true, "4.0", OptionalInt.of(25), new ClusterFeatures.Level(0, 1));

        assertThat(on.consumerProtocol()).isTrue();
        assertThat(on.fencedBrokers()).isTrue();
        assertThat(on.assessments().get(KafkaAccess.QUORUM).status()).isEqualTo(Assessment.Status.AVAILABLE);
        assertThat(on.assessments().get(KafkaAccess.CONSUMER_PROTOCOL_GROUPS).status())
                .isEqualTo(Assessment.Status.AVAILABLE);
        assertThat(off.consumerProtocol()).isFalse();
        assertThat(off.assessments().get(KafkaAccess.CONSUMER_PROTOCOL_GROUPS).remedy())
                .hasValueSatisfying(
                        r -> assertThat(r.snippet()).isEqualTo("kafka-features.sh upgrade --feature group.version=1"));
    }

    @Test
    void fencedBrokersNeedKraftAndKafkaFour() {
        assertThat(features(true, "3.9", OptionalInt.of(21), OFF).fencedBrokers())
                .isFalse();
        assertThat(features(false, "4.0", OptionalInt.of(25), OFF).fencedBrokers())
                .isFalse();
        assertThat(features(true, "4.0", OptionalInt.of(25), OFF).fencedBrokers())
                .isTrue();
    }

    @Test
    void aClusterWithEveryFeatureOnDeclaresEveryCapabilityAvailable() {
        ClusterFeatures.Level on = new ClusterFeatures.Level(1, 1);
        ClusterFeatures all = new ClusterFeatures(
                true, "4.4", OptionalInt.of(33), null, on, on, on, on, new ShareFeatures(2, 2, true));

        assertThat(all.streamsGroups()).isTrue();
        assertThat(all.eligibleLeaders()).isTrue();
        assertThat(all.assessments().values())
                .allSatisfy(a -> assertThat(a.status()).isEqualTo(Assessment.Status.AVAILABLE));
    }

    @Test
    void aGapNamesItsRemedyOnlyWhereTheSoftwareHasTheFeature() {
        ClusterFeatures.Level absent = new ClusterFeatures.Level(0, 0);
        ClusterFeatures.Level supported = new ClusterFeatures.Level(0, 1);
        ClusterFeatures cluster = new ClusterFeatures(
                true,
                "4.0",
                OptionalInt.of(25),
                null,
                supported,
                absent,
                supported,
                OFF,
                new ShareFeatures(0, 1, true));
        var out = cluster.assessments();

        assertThat(out.get(KafkaAccess.STREAMS_GROUPS).remedy()).isEmpty();
        assertThat(out.get(KafkaAccess.ELIGIBLE_LEADERS).remedy())
                .hasValueSatisfying(r -> assertThat(r.snippet()).contains("eligible.leader.replicas.version=1"));
        assertThat(out.get(KafkaAccess.SHARE_GROUPS).status()).isEqualTo(Assessment.Status.UNAVAILABLE);
        assertThat(out.get(KafkaAccess.SHARE_GROUPS).remedy()).isPresent();
    }
}
