package it.uniroma2.dicii.isw2.bookkeeper.net;

import org.apache.bookkeeper.net.*;
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
        private String name;
        private String location;
        private String exceptionClassName;
        private boolean expected;

        public AddAndCheckTests(String name, String location, boolean expected, String exceptionClassName) {
            configure(name, location, expected, exceptionClassName);
        }

        public void configure(String name, String location, boolean expected, String exceptionClassName) {
            this.nt = new NetworkTopologyImpl();
            this.name = name;
            this.location = location;
            this.exceptionClassName = exceptionClassName;
            this.expected = expected;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            String validName = "test-node";
            String validLocation = NodeBase.PATH_SEPARATOR_STR + "test-loc";

            String invalidName = "";
            String invalidLocation = "";

            String illArgExc = "java.lang.IllegalArgumentException";
            String nilPtrExc = "java.lang.NullPointerException";

            return Arrays.asList(new Object[][] {
                    // NAME         LOCATION            EXPECTED    EXCEPTION
                    {  validName,   validLocation,      true,       null  },        // valid,   valid
                    {  invalidName, "",                 false,      illArgExc  },   // invalid, empty
                    {  "",          null,               false,      illArgExc  },   // empty,   null
                    {  null,        invalidLocation,    false,      nilPtrExc  }    // invalid, invalid
            });
        }

        /**
         * testAddAndCheckMembership - insert a node into the network
         * topology and tests if the node is contained.
         *
         * A node is defined starting from:
         *  - name
         *  - location
         *
         * For name the equivalence classes are:
         *  { EMPTY, VALID, INVALID }
         * since null must be considered as invalid as BookieId
         *
         * For the location the equivalence classes are:
         *  { EMPTY, NULL, VALID, INVALID }
         *
         * @author: Simone Tiberi
         */
        @Test
        public void testAddAndCheckMembership() {
            try {
                Node node = new BookieNode(BookieId.parse(this.name), this.location);
                this.nt.add(node);

                boolean actual = this.nt.contains(node);

                if (exceptionClassName == null)
                    Assert.assertEquals(this.expected, actual);
                else
                    Assert.fail("The exception \"" + this.exceptionClassName + "\" had to be thrown");

            } catch (Exception e) {
                if (exceptionClassName == null)
                    Assert.fail("The exception \"" + e.getClass().getName() + "\" had not to be thrown");
                else {
                    try {
                        Assert.assertEquals(e.getClass(), Class.forName(this.exceptionClassName));
                    } catch (ClassNotFoundException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }
}
