package it.uniroma2.dicii.isw2.bookkeeper.net;

import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieNode;
import org.apache.bookkeeper.net.NetworkTopologyImpl;
import org.apache.bookkeeper.net.Node;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.junit.Assert;

import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Enclosed.class)
public class NetworkTopologyImplTest {

    @RunWith(value = Parameterized.class)
    public static class AddAndCheckTests {
        private NetworkTopologyImpl nt;
        private Node newNode;

        public AddAndCheckTests(String id, String networkLocation) {
            configure(id, networkLocation);
        }

        public void configure(String id, String networkLocation) {
            this.nt = new NetworkTopologyImpl();
            this.newNode = new BookieNode(BookieId.parse(id), networkLocation);
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            return Arrays.asList(new Object[][] {
                    {"test-node", "/test-loc"}
            });
        }

        @Test
        public void testAddAndCheckMembership() {
            this.nt.add(this.newNode);
            Assert.assertTrue(nt.contains(newNode));
        }
    }
}
