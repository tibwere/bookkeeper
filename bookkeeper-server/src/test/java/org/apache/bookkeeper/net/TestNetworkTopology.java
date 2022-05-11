package org.apache.bookkeeper.net;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

@RunWith(value = Enclosed.class)
public class TestNetworkTopology {

    @RunWith(value = Parameterized.class)
    public static class AddNodeAndCheckTests {

        private String nodeName;
        private String nodeLocation;
        private String rack;
        private int expectedLeaves;
        private boolean expectedException;

        public AddNodeAndCheckTests(String nodeName, String nodeLocation, String rack, int expectedLeaves,
                            boolean expectedException) {
            configure(nodeName, nodeLocation, rack, expectedLeaves, expectedException);
        }

        public void configure(String nodeName, String nodeLocation, String rack, int expectedLeaves,
                              boolean expectedException) {
            this.nodeName = nodeName;
            this.nodeLocation = nodeLocation;
            this.rack = rack;
            this.expectedLeaves = expectedLeaves;
            this.expectedException = expectedException;
        }

        /**
         * BOUNDARY VALUE ANALYSIS
         *  - nodeName:             [valid, invalid] (n.b. null is valid!)
         *  - nodeLocation:         [valid_root, valid_other, invalid] (root is "" or null and in this case is invalid)
         *  - rack:                 [valid_same_inserted, valid_other]
         *  - expectedLeaves:       [0, 1]
         *  - expectedException:    [true, false]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            String validLocations[] = new String[]{
                    NodeBase.PATH_SEPARATOR_STR + "test-rack-1",
                    NodeBase.PATH_SEPARATOR_STR + "test-rack-2"
            };

            String invalidLocations[] = new String[] {
                "not-start-with-sep",
                NodeBase.ROOT
            };

            String validName = "test-node";
            String invalidName = NodeBase.PATH_SEPARATOR_STR + "invalid-name";


            return Arrays.asList(new Object[][]{
                    // NODE_NAME    NODE_LOCATION           RACK                EXPECTED_LEAVES     EXPECTED_EXCEPTION
                    {  validName,   validLocations[0],      validLocations[0],  1,                  false   },
                    {  invalidName, validLocations[1],      validLocations[1],  1,                  true    },
                    {  null,        validLocations[0],      validLocations[1],  0,                  false   },
                    {  validName,   invalidLocations[0],    validLocations[0],  0,                  true    },
                    {  validName,   invalidLocations[1],    validLocations[1],  0,                  true    }
            });
        }

        @Test
        public void testAddNodes() {
            try {
                NetworkTopology nt = new NetworkTopologyImpl();
                Node n = new NodeBase(this.nodeName, this.nodeLocation);
                nt.add(n);

                Set<Node> nodes = nt.getLeaves(this.rack);
                Assert.assertEquals("Wrong number of leaves detected", this.expectedLeaves, nodes.size());
                Assert.assertTrue("Node should be contained inside the topology", nt.contains(n));
            } catch (IllegalArgumentException e) {
                Assert.assertTrue("IllegalArgumentException should have been thrown", this.expectedException);
            }
        }
    }

    @RunWith(value = Parameterized.class)
    public static class RemoveNodeTests {

        private Node nodeToBeAdded;
        private RemovalTypes typeOfRemoval;

        private NetworkTopology nt;

        public RemoveNodeTests(Node nodeToBeAdded, RemovalTypes typeOfRemoval) {
            configure(nodeToBeAdded, typeOfRemoval);
        }

        public void configure(Node nodeToBeAdded, RemovalTypes typeOfRemoval) {
            this.nodeToBeAdded = nodeToBeAdded;
            this.typeOfRemoval = typeOfRemoval;
            this.nt = new NetworkTopologyImpl();
        }

        @Before
        public void addInitialNode() {
            try {
                nt.add(this.nodeToBeAdded);
            } catch (IllegalArgumentException e) {
                Assert.fail("This test assumes that node to be inserted initially is valid (or null)");
            }
        }

        /**
         * BOUNDARY VALUE ANALYSIS
         *  - nodeToBeAdded: Since the test is focused on the removal it is assumed to use only valid or null nodes
         *      (if you choose an invalid node as a parameter the test fails deterministically due to the Assert.fail)
         *
         *      [valid_node, null]
         *
         * - typeOfRemoval [ADDED, NOT_ADDED, INNER]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {

            try {
                Node node = new NodeBase("test-node", NodeBase.PATH_SEPARATOR_STR + "test-rack");

                return Arrays.asList(new Object[][]{
                        // NODE_TO_BE_ADDED     TYPE_OF_REMOVAL
                        {  node,                RemovalTypes.ADDED      },
                        {  node,                RemovalTypes.NOT_ADDED  },
                        {  null,                RemovalTypes.INNER      }
                });

            } catch (IllegalArgumentException e) {
                Assert.fail("This test assumes that node to be inserted initially is valid (or null)");
                return null;
            }
        }

        @Test
        public void testRemoveNode() {

            Node nodeToBeRemoved = null;
            switch (this.typeOfRemoval) {
                case ADDED:
                    nodeToBeRemoved = this.nodeToBeAdded;
                    break;
                case INNER:
                    nodeToBeRemoved = new NetworkTopologyImpl.InnerNode(NetworkTopologyImpl.InnerNode.ROOT);
                    break;
                case NOT_ADDED:
                    try {
                        nodeToBeRemoved = new NodeBase(this.nodeToBeAdded.getName() + "-new",
                                this.nodeToBeAdded.getNetworkLocation() + "-new");
                    } catch (IllegalArgumentException e) {
                        Assert.fail("This test assumes that node to be inserted initially is valid (or null)");
                    }
            }

            try {
                this.nt.remove(nodeToBeRemoved);
                Assert.assertFalse("It is not possible to remove inner node",
                        RemovalTypes.INNER.equals(this.typeOfRemoval));

            } catch (IllegalArgumentException e) {
                Assert.assertTrue("It is not possible to remove inner node",
                        RemovalTypes.INNER.equals(this.typeOfRemoval));
            }

        }
    }

    private enum RemovalTypes { INNER, ADDED, NOT_ADDED }
}
