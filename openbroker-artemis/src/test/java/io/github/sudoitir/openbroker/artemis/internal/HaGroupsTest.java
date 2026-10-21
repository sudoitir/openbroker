package io.github.sudoitir.openbroker.artemis.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sudoitir.openbroker.NodeId;
import io.github.sudoitir.openbroker.model.Attributes;
import io.github.sudoitir.openbroker.model.Designation;
import io.github.sudoitir.openbroker.model.Membership;
import io.github.sudoitir.openbroker.model.NodeDescription;
import io.github.sudoitir.openbroker.model.NodeFunction;
import io.github.sudoitir.openbroker.model.Serving;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Where a lost link goes: its own node, then the serving members of its node's HA group. */
class HaGroupsTest {

    private static final NodeHandle PRIMARY = node("primary");
    private static final NodeHandle BACKUP = node("backup");
    private static final NodeHandle OTHER = node("other");

    private static NodeHandle node(String id) {
        return new NodeHandle(
                NodeId.of(id),
                Optional.of(URI.create("http://" + id + "/console/jolokia")),
                Optional.of(id + ":61616"));
    }

    private static NodeDescription described(
            NodeHandle node, Optional<String> group, Designation designation, Serving serving) {
        return new NodeDescription(
                node.id(),
                group.orElse(""),
                new Membership(group, designation, serving, Optional.empty()),
                Set.of(NodeFunction.BROKER),
                List.of(),
                Optional.empty(),
                List.of(),
                Attributes.empty(),
                Optional.empty());
    }

    @Test
    void aNodeAloneIsItsOwnOnlyCandidate() {
        HaGroups groups = new HaGroups(() -> List.of(PRIMARY), List::of);

        assertThat(groups.candidates(PRIMARY, PRIMARY)).containsExactly(PRIMARY);
    }

    @Test
    void aBackupThatTookOverIsTriedFirstWhenThePrimaryIsDown() {
        List<NodeDescription> answers = new ArrayList<>(List.of(
                described(PRIMARY, Optional.of("node-1"), Designation.PRIMARY, Serving.ACTIVE),
                described(BACKUP, Optional.empty(), Designation.BACKUP, Serving.STANDBY)));
        HaGroups groups = new HaGroups(() -> List.of(PRIMARY, BACKUP, OTHER), () -> List.copyOf(answers));
        groups.learn(PRIMARY);

        // The primary dies; the backup now serves, reporting the node id they share.
        answers.clear();
        answers.add(described(BACKUP, Optional.of("node-1"), Designation.BACKUP, Serving.ACTIVE));
        answers.add(described(OTHER, Optional.of("node-2"), Designation.PRIMARY, Serving.ACTIVE));

        assertThat(groups.candidates(PRIMARY, PRIMARY)).containsExactly(BACKUP, PRIMARY);
    }

    @Test
    void aBackupThatIsStillStandingByIsTriedAfterTheNodeItself() {
        List<NodeDescription> answers = List.of(
                described(PRIMARY, Optional.of("node-1"), Designation.PRIMARY, Serving.ACTIVE),
                described(BACKUP, Optional.empty(), Designation.BACKUP, Serving.STANDBY));
        HaGroups groups = new HaGroups(() -> List.of(PRIMARY, BACKUP), () -> answers);

        assertThat(groups.candidates(PRIMARY, PRIMARY)).containsExactly(PRIMARY, BACKUP);
    }

    @Test
    void aBackupWithNoGroupIsNotACandidateWhileAnotherGroupIsKnown() {
        // It could be the other pair's backup: a link that moved there would read another journal.
        List<NodeDescription> answers = List.of(
                described(PRIMARY, Optional.of("node-1"), Designation.PRIMARY, Serving.ACTIVE),
                described(BACKUP, Optional.empty(), Designation.BACKUP, Serving.ACTIVE),
                described(OTHER, Optional.of("node-2"), Designation.PRIMARY, Serving.ACTIVE));
        HaGroups groups = new HaGroups(() -> List.of(PRIMARY, BACKUP, OTHER), () -> answers);

        assertThat(groups.candidates(PRIMARY, PRIMARY)).containsExactly(PRIMARY);
    }

    @Test
    void aNodeOfAnotherGroupIsNeverACandidate() {
        List<NodeDescription> answers = List.of(
                described(PRIMARY, Optional.of("node-1"), Designation.PRIMARY, Serving.ACTIVE),
                described(OTHER, Optional.of("node-2"), Designation.PRIMARY, Serving.ACTIVE));
        HaGroups groups = new HaGroups(() -> List.of(PRIMARY, OTHER), () -> answers);

        assertThat(groups.candidates(PRIMARY, PRIMARY)).containsExactly(PRIMARY);
    }

    @Test
    void aLinkThatFailedOverComesBackToTheServingMemberFirst() {
        List<NodeDescription> answers = List.of(
                described(PRIMARY, Optional.of("node-1"), Designation.PRIMARY, Serving.STANDBY),
                described(BACKUP, Optional.of("node-1"), Designation.BACKUP, Serving.ACTIVE));
        HaGroups groups = new HaGroups(() -> List.of(PRIMARY, BACKUP), () -> answers);

        assertThat(groups.candidates(PRIMARY, BACKUP)).containsExactly(BACKUP, PRIMARY);
    }

    @Test
    void aNewLinkOpensOnTheMemberThatServes() {
        List<NodeDescription> answers = new ArrayList<>(List.of(
                described(PRIMARY, Optional.of("node-1"), Designation.PRIMARY, Serving.ACTIVE),
                described(BACKUP, Optional.empty(), Designation.BACKUP, Serving.STANDBY)));
        HaGroups groups = new HaGroups(() -> List.of(PRIMARY, BACKUP), () -> List.copyOf(answers));
        groups.learn(PRIMARY);
        assertThat(groups.serving(PRIMARY)).isEqualTo(PRIMARY);

        answers.clear();
        answers.add(described(BACKUP, Optional.of("node-1"), Designation.BACKUP, Serving.ACTIVE));

        assertThat(groups.serving(PRIMARY)).isEqualTo(BACKUP);
    }

    @Test
    void theGroupsAreLearnedUntilTheNodesOwnIsKnown() {
        AtomicInteger calls = new AtomicInteger();
        HaGroups groups = new HaGroups(() -> List.of(PRIMARY, BACKUP), () -> {
            calls.incrementAndGet();
            return List.of(described(PRIMARY, Optional.of("node-1"), Designation.PRIMARY, Serving.ACTIVE));
        });

        groups.learn(PRIMARY);
        groups.learn(PRIMARY);

        assertThat(calls)
                .as("a node whose group is known is not asked about again")
                .hasValue(1);
    }
}
