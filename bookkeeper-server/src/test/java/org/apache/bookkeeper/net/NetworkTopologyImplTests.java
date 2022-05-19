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

import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(value = Enclosed.class)
public class NetworkTopologyImplTests {

    @RunWith(value = Parameterized.class)
    public static class AddNodeAndCheckTests {

        private NodeType type;
        private String nodeName;
        private String nodeLocation;
        private String rack;
        private int expectedLeaves;
        private boolean expectedException;

        public AddNodeAndCheckTests(NodeType type, String nodeName, String nodeLocation, String rack, int expectedLeaves,
                            boolean expectedException) {
            configure(type, nodeName, nodeLocation, rack, expectedLeaves, expectedException);
        }

        public void configure(NodeType type, String nodeName, String nodeLocation, String rack, int expectedLeaves,
                              boolean expectedException) {
            this.type = type;
            this.nodeName = nodeName;
            this.nodeLocation = nodeLocation;
            this.rack = rack;
            this.expectedLeaves = expectedLeaves;
            this.expectedException = expectedException;
        }

        /**
         * BOUNDARY VALUE ANALYSIS
         *  - type:                 [null, node_base, inner_node, bookie_node]
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
                    // NODE_TYPE                NODE_NAME       NODE_LOCATION           RACK                EXPECTED_LEAVES     EXPECTED_EXCEPTION
                    {  NodeType.BOOKIE_NODE,    validName,      validLocations[0],      validLocations[0],  1,                  false   },
                    {  NodeType.NULL,           null,           null,                   validLocations[0],  0,                  false   },
                    {  NodeType.INNER_NODE,     validName,      validLocations[0],      validLocations[0],  0,                  true    },
                    {  NodeType.NODE_BASE,      validName,      validLocations[0],      validLocations[0],  1,                  false   },
                    {  NodeType.NODE_BASE,      invalidName,    validLocations[1],      validLocations[1],  1,                  true    },
                    {  NodeType.NODE_BASE,      null,           validLocations[0],      validLocations[1],  0,                  false   },
                    {  NodeType.NODE_BASE,      validName,      invalidLocations[0],    validLocations[0],  0,                  true    },
                    {  NodeType.NODE_BASE,      validName,      invalidLocations[1],    validLocations[1],  0,                  true    }
            });
        }

        private Node getNodeToBeAdded() {
            switch (this.type) {
                case NODE_BASE:
                    return new NodeBase(this.nodeName, this.nodeLocation);
                case INNER_NODE:
                    return new NetworkTopologyImpl.InnerNode(this.nodeName, this.nodeLocation);
                case BOOKIE_NODE:
                    return new BookieNode(BookieId.parse(this.nodeName), this.nodeLocation);
                default:
                    return null;
            }
        }

        @Test
        public void testAddNodes() {
            try {
                NetworkTopology nt = new NetworkTopologyImpl();
                Node n = getNodeToBeAdded();
                nt.add(n);

                Set<Node> nodes = nt.getLeaves(this.rack);
                Assert.assertEquals("Wrong number of leaves detected", this.expectedLeaves, nodes.size());
                if (!NodeType.NULL.equals(this.type))
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

        public RemoveNodeTests(RemovalTypes typeOfRemoval) {
            configure(typeOfRemoval);
        }

        public void configure(RemovalTypes typeOfRemoval) {
            this.typeOfRemoval = typeOfRemoval;
            this.nt = new NetworkTopologyImpl();
        }

        @Before
        public void addInitialNode() {
            try {
                this.nodeToBeAdded = new NodeBase("test-node", NodeBase.PATH_SEPARATOR_STR + "test-rack");
                nt.add(this.nodeToBeAdded);
            } catch (IllegalArgumentException e) {
                Assert.fail("This test assumes that node to be inserted initially is valid");
            }
        }

        /**
         * BOUNDARY VALUE ANALYSIS
         * - typeOfRemoval [ADDED, NOT_ADDED, INNER]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
                return Arrays.asList(new Object[][]{
                        // TYPE_OF_REMOVAL
                        {  RemovalTypes.ADDED      },
                        {  RemovalTypes.NOT_ADDED  },
                        {  RemovalTypes.INNER      }
                });
        }

        private Node getNodeToBeRemoved() {
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

            return nodeToBeRemoved;
        }

        @Test
        public void testRemoveNode() {

            try {
                Node nodeToBeRemoved = getNodeToBeRemoved();

                int oldSize = this.nt.getLeaves(nodeToBeAdded.getNetworkLocation()).size();
                this.nt.remove(nodeToBeRemoved);
                int newSize = this.nt.getLeaves(nodeToBeAdded.getNetworkLocation()).size();

                switch (this.typeOfRemoval) {
                    case ADDED:
                        Assert.assertEquals("The number of leaves should be decreased", oldSize-1, newSize);
                        break;
                    case INNER:
                        Assert.fail("It is not possible to remove inner node");
                        break;
                    case NOT_ADDED:
                        Assert.assertEquals("The number of leaves should not be decreased", oldSize, newSize);
                        break;
                }
            } catch (IllegalArgumentException e) {
                Assert.assertTrue("It is not possible to remove inner node",
                        RemovalTypes.INNER.equals(this.typeOfRemoval));
            }
        }
    }

    public static class ForcedScenariosTests {

        /******************************/
        /** Forced scenarios for add **/
        /******************************/

        @Test
        public void testAddRackAndNonRackSameLevel() {
            NetworkTopology nt = new NetworkTopologyImpl();
            Node invalidNode = null;
            try {
                Node validNode = new NodeBase("test-node-1", NodeBase.PATH_SEPARATOR_STR + "rack0");
                invalidNode = new NodeBase("test-node-2", NodeBase.PATH_SEPARATOR_STR + "rack0/sec1");
                nt.add(validNode);
            } catch (Exception e) {
                Assert.fail("This setup phase should not raise an exception");
            }

            try {
                nt.add(invalidNode);
                Assert.fail("An \"InvalidTopologyException\" should be raised");
            } catch (NetworkTopologyImpl.InvalidTopologyException e) {
                Assert.assertTrue(true);
            }
        }

        @Test
        public void testIllegalNetworkLocation() {
            NetworkTopologyImpl nt = spy(NetworkTopologyImpl.class);
            Node n = new NodeBase("test-node", NodeBase.PATH_SEPARATOR_STR + "rack0");
            when(nt.getNodeForNetworkLocation(n)).thenReturn(new NodeBase("test.node-2", NodeBase.PATH_SEPARATOR_STR + "rack0"));

            try {
                nt.add(n);
                Assert.fail("An \"IllegalArgumentException\" should be raised");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(true);
            }
        }

        @Test
        public void testAddTwoTimesSameNode() {
            NetworkTopology nt = new NetworkTopologyImpl();
            final String rack = NodeBase.PATH_SEPARATOR_STR + "rack0";

            Node n = new NodeBase("test-node", rack);
            nt.add(n);
            int beforeSize = nt.getLeaves(rack).size();

            nt.add(n);
            int afterSize = nt.getLeaves(rack).size();

            Assert.assertEquals("The node should not be added the second time", beforeSize, afterSize);
        }

        @Test
        public void testAddTwoValidNodes() {
            NetworkTopology nt = new NetworkTopologyImpl();

            Node valid1 = new NodeBase("test-node-1", NodeBase.PATH_SEPARATOR_STR + "rack0");
            Node valid2 = new NodeBase("test-node-2", NodeBase.PATH_SEPARATOR_STR + "rack1");

            nt.add(valid1);
            nt.add(valid2);

            Assert.assertTrue("The node is valid, so it should be inserted (1)", nt.contains(valid1));
            Assert.assertTrue("The node is valid, so it should be inserted (2)", nt.contains(valid2));
        }

        /***********************************/
        /** Forced scenarios for contains **/
        /***********************************/

        @Test
        public void testNodeWithExplicitParent() {
            Node parent = new NodeBase("parent", NodeBase.PATH_SEPARATOR_STR + "/rack0");
            Node child1 = new NodeBase("child", NodeBase.PATH_SEPARATOR_STR + "/rack0", parent, 1);
            Node child2 = new NodeBase("child", NodeBase.PATH_SEPARATOR_STR + "/rack0", parent, 0);

            NetworkTopology nt = new NetworkTopologyImpl();

            Assert.assertFalse("This node has not been added", nt.contains(child1));
            Assert.assertFalse("This node has not been added", nt.contains(child2));
        }

       /************************************/
       /** Forced scenarios for getLeaves **/
       /************************************/

        @Test
        public void getLeavesWithTilde() {
            NetworkTopology nt = new NetworkTopologyImpl();
            String location = NodeBase.PATH_SEPARATOR_STR + "rack0";
            Node node = new NodeBase("test-node", location);
            nt.add(node);

            Assert.assertEquals(0, nt.getLeaves("~" + location).size());
        }

       /*********************************/
       /** Forced scenarios for remove **/
       /*********************************/

        @Test
        public void testRemoveNullShouldReturnImmediately() {
            try {
                NetworkTopology nt = new NetworkTopologyImpl();
                nt.remove(null);
                Assert.assertTrue(true);
            } catch (Exception e) {
                Assert.fail("No exception should be raised");
            }
        }

        @Test
        public void testIfRackNullNumOfRacksShouldNotChange() {
            NetworkTopology nt = spy(NetworkTopologyImpl.class);
            when(nt.getNode(notNull())).thenReturn(new NetworkTopologyImpl.InnerNode(
                    "core", NodeBase.PATH_SEPARATOR_STR + "rack0"));

            Node n = new NodeBase("test-node", NodeBase.PATH_SEPARATOR_STR + "rack0");
            nt.add(n);

            int before = nt.getNumOfRacks();
            nt.remove(n);
            int after = nt.getNumOfRacks();

            Assert.assertEquals("Number of racks should not change if rack is not null", before, after);
        }
    }

    @RunWith(value = Parameterized.class)
    public static class ContainsTests {
        private SearchNode typeOfSearch;
        private final Node validNode = new NodeBase("test-node-1", NodeBase.PATH_SEPARATOR_STR + "rak0");
        private NetworkTopology nt;

        public ContainsTests(SearchNode typeOfSearch) {
            this.typeOfSearch = typeOfSearch;
        }

        @Before
        public void configureTopology() {
            this.nt = new NetworkTopologyImpl();
            if (this.typeOfSearch == SearchNode.PRESENT)
                nt.add(this.validNode);
        }

        /**
         * BOUNDARY VALUE ANALYSIS
         * - typeOfRemoval [PRESENT, NOT_PRESENT, NULL]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            return Arrays.asList(new Object[][]{
                    // TYPE_OF_SEARCH
                    {  SearchNode.NOT_PRESENT   },
                    {  SearchNode.PRESENT       },
                    {  SearchNode.NULL          }
            });
        }

        @Test
        public void testContains() {
            switch (this.typeOfSearch) {
                case NULL:
                    Assert.assertFalse("If contains is executed over null node, false should be immediately returned",
                            nt.contains(null));
                    break;
                default:
                    Assert.assertEquals("If a node has been added, it must be contained inside topology",
                            nt.contains(this.validNode), SearchNode.PRESENT.equals(this.typeOfSearch));
            }

        }
    }

    private enum RemovalTypes { INNER, ADDED, NOT_ADDED }

    private enum NodeType {NULL, NODE_BASE, INNER_NODE, BOOKIE_NODE}

    private enum SearchNode {NULL, PRESENT, NOT_PRESENT}
}
