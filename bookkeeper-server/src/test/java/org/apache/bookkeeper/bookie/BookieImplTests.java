package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;


/**
 * Test cases for BookieImpl class
 *
 * @author Simone Tiberi
 */
@RunWith(value = Enclosed.class)
public class BookieImplTests {

    /**
     * This method returns a mocked instance of WriteCallback that does nothing
     * when writeComplete is invoked.
     *
     * @return a mocked WriteCallback
     */
    public static BookkeeperInternalCallbacks.WriteCallback generateMockedNopCb() {
        BookkeeperInternalCallbacks.WriteCallback nopCb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
        /* A NOP callback should do nothing when invoked */
        doNothing().when(nopCb).writeComplete(isA(Integer.class), isA(Long.class), isA(Long.class),
                isA(BookieId.class), isA(Object.class));

        return nopCb;
    }

    /**
     * Initial test class derived from black-box analysis
     */
    @RunWith(value = Parameterized.class)
    public static class AddEntryTests {

        private long readEntryID;
        private EntryBuilder builder;
        private boolean ackBeforeSync;
        private Object ctx;
        private byte[] masterKey;
        private boolean expectedException;

        private Bookie bookie;

        public AddEntryTests(long readEntryID, EntryBuilder builder, boolean ackBeforeSync, Object ctx, byte[] masterKey,
                             boolean expectedException) {
            configure(readEntryID, builder, ackBeforeSync, ctx, masterKey, expectedException);
        }

        public void configure(long readEntryID, EntryBuilder builder, boolean ackBeforeSync, Object ctx, byte[] masterKey,
                              boolean expectedException) {
            this.readEntryID = readEntryID;
            this.builder = builder;
            this.ackBeforeSync = ackBeforeSync;
            this.ctx = ctx;
            this.masterKey = masterKey;
            this.expectedException = expectedException;

            try {
                this.bookie = new TestBookieImpl(TestBKConfiguration.newServerConfiguration());
            } catch (Exception e) {
                Assert.fail("The exception \"" + e.getClass().getName() + "\" should not be thrown");
            }
        }

        /**
         * BOUNDARY VALUE ANALYSIS
         *  - readEntryID:          [SAME, DIFFERENT]
         *  - builder:              [VALID, INVALID, NULL]
         *  - ackBeforeSync:        [TRUE, FALSE]
         *  - ctx:                  [NULL, VALID_INSTANCE]
         *  - masterKey:            [NULL, NOT_NULL]
         *  - expectedException:    [FALSE, TRUE]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            final byte []validMasterKey = "master".getBytes(StandardCharsets.UTF_8);

            EntryBuilder validEntry = EntryBuilder.validEntry(0, 0);
            EntryBuilder invalidEntry = EntryBuilder.invalidEntry();
            EntryBuilder nullEntry = EntryBuilder.nullEntry();

            return Arrays.asList(new Object[][]{
                    // READ_ENTRY_ID    ENTRY_BUILDER   ACK     CONTEXT         MASTER_KEY      EXCEPTION
                    {  0,               validEntry,     true,   null,           validMasterKey, false   },
                    {  1,               validEntry,     false,  new Object(),   validMasterKey, true    },
                    {  0,               nullEntry,      false,  null,           null,           true    },
                    {  1,               invalidEntry,   false,  new Object(),   validMasterKey, true    }
            });
        }

        @Before
        public void startupBookie() {
            this.bookie.start();
        }

        @After
        public void shutdownBookie() {
            this.bookie.shutdown();
        }

        @Test
        public void testAddEntry() {
            try {
                this.bookie.addEntry(this.builder.build(), this.ackBeforeSync, generateMockedNopCb(),
                        this.ctx, this.masterKey);

                ByteBuf bb = this.bookie.readEntry(this.builder.getLedgerID(), this.readEntryID);
                Assert.assertEquals("Ledger IDs should be the same", this.builder.getLedgerID(), bb.readLong());
                Assert.assertEquals("Entry IDs should be the same", this.builder.getEntryID(), bb.readLong());

                int i = 0;
                byte[] expectedPayload = this.builder.getPayload();
                while (i < expectedPayload.length && bb.isReadable())
                    Assert.assertEquals("Byte " + i + " of payload should be the same",
                            expectedPayload[i++], bb.readByte());

                Assert.assertTrue("Payload size should be the same", i == expectedPayload.length);
                Assert.assertFalse("An exception should be thrown", this.expectedException);
            } catch (Exception e) {
                Assert.assertTrue("Exception \"" + e.getClass().getName() + "\" should not be thrown",
                        this.expectedException);
            }
        }
    }

    /**
     * Test class added in the white box analysis phase of the SUT that stimulates a particular scenario in which the
     * ledger from which you want to read is fenced and therefore not writable
     */
    public static class AddToFencedLedgerTests {

        private BookieImplParameters params;
        private BookieImpl bookie;

        @Before
        public void setupEnvironment() {
            try {
                LedgerStorage mockedLedgerStorage = mock(LedgerStorage.class);
                when(mockedLedgerStorage.isFenced(any(long.class))).thenReturn(true);

                this.params = BookieImplParameters.getDefaultParameters()
                        .withLedgerStorage(mockedLedgerStorage);

                this.bookie = this.params.buildBookieInstance();
                this.bookie.start();
            } catch (Exception e) {
                Assert.fail("The setup phase should not raise an exception, instead \""
                        + e.getClass().getName() + "\" has thrown");
            }
        }

        @After
        public void tearDown() {
            try {
                this.bookie.shutdown();
                this.params.closeAll();
            } catch (Exception e) {
                Assert.fail("The tear down phase should not raise an exception, instead \""
                        + e.getClass().getName() + "\" has thrown");
            }
        }

        @Test
        public void testAddToAFencedLedger() {
            try {
                this.bookie.addEntry(EntryBuilder.validEntry(0, 0).build(),
                        true, generateMockedNopCb(), null, "master".getBytes(StandardCharsets.UTF_8));
                Assert.fail("It should be impossible to add an entry to a fenced ledger");
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
        }
    }

    /**
     * Parametric tests for the getBookieAddress method. This method from the blackbox point of view was poor as there
     * is no documentation, but by approaching it from the white box point of view it is possible to understand which
     * properties influence the execution and stimulate the various statements/branches
     */
    @RunWith(value = Parameterized.class)
    public static class GetBookieAddressTests {

        private ServerConfiguration conf;
        private boolean expectedException;

        public GetBookieAddressTests(ServerConfiguration conf, boolean expectedException) {
            configure(conf, expectedException);
        }

        public void configure(ServerConfiguration conf, boolean expectedException) {
            this.conf = conf;
            this.expectedException = expectedException;
        }

        /**
         * BOUNDARY VALUE ANALYSIS:
         *  - conf:                 an instance for each relevant property
         *  - expectedAddr:         different from case to case
         *  - expectedException:    [true, false]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            ServerConfiguration defaultConf = TestBKConfiguration.newServerConfiguration()
                    .setListeningInterface("lo"); // to avoid unpredictable execution specially in CI pipeline set up a precise interface
            ServerConfiguration advertisedAddrConf = defaultConf.setAdvertisedAddress("isw2.software.testing");
            ServerConfiguration nullListeningInterface = defaultConf.setListeningInterface(null);
            ServerConfiguration hostNameAsBookieIDConf = defaultConf.setUseHostNameAsBookieID(true);
            ServerConfiguration shortHostNameAsBookieIDConf = defaultConf.setUseHostNameAsBookieID(true)
                    .setUseShortHostName(true);
            ServerConfiguration denyLoopbackConf = defaultConf.setAllowLoopback(false);
            ServerConfiguration invalidPortConf = defaultConf.setBookiePort(Integer.MAX_VALUE);
            ServerConfiguration zeroLengthAdvAddressConf = defaultConf.setAdvertisedAddress("");

            return Arrays.asList(new Object[][]{
                    // SERVER_CONFIG                EXPECTED_EXCEPTION
                    { defaultConf,                  false   },
                    { advertisedAddrConf,           false   },
                    { nullListeningInterface,       false   },
                    { hostNameAsBookieIDConf,       false   },
                    { shortHostNameAsBookieIDConf,  false   },
                    { denyLoopbackConf,             true    },
                    { invalidPortConf,              true    },
                    { zeroLengthAdvAddressConf,     false   }
            });
        }

        @Test
        public void testGetBookieAddress() {
            try {
                BookieSocketAddress sa = BookieImpl.getBookieAddress(this.conf);
                List<String> expectedAddresses = this.getExpectedAddress();
                Assert.assertTrue("The address should be inside the list of available address for the host (list: [" +
                                String.join(", ", expectedAddresses) + "], actual: " + sa.getHostName() + ")",
                        expectedAddresses.contains(sa.getHostName()));
                Assert.assertFalse("This configuration should throw an exception", this.expectedException);
            } catch (UnknownHostException | IllegalArgumentException | SocketException e) {
                Assert.assertTrue("This configuration should not throw an exception", this.expectedException);
            }
        }

        /**
         * Since these tests must run in any machine and not only in the local one,
         * the address cannot be hardcoded, so it is evaluated using InetAddress.getByName
         * instead of InetSocketAddress.
         */
        private List<String> getExpectedAddress() throws UnknownHostException, SocketException {

            /* If an address is advertised returns it*/
            if (this.conf.getAdvertisedAddress() != null && this.conf.getAdvertisedAddress().trim().length() > 0)
                return Arrays.asList(this.conf.getAdvertisedAddress());

            List<String> hostNames = new ArrayList<>();
            /* Use the local host canonical name or use a specified interface to determine it */
            if (this.conf.getListeningInterface() == null) {
                hostNames.add(InetAddress.getLocalHost().getCanonicalHostName());
            } else {
                Enumeration<InetAddress> enumIf = NetworkInterface.getByName(
                        this.conf.getListeningInterface()).getInetAddresses();

                while(enumIf.hasMoreElements()) {
                    hostNames.add(enumIf.nextElement().getCanonicalHostName());
                }
            }

            /*
             * If in the configuration it's specified to use hostname as bookieID,
             * then use canonical host name.
             * (see. https://superuser.com/questions/394816/what-is-the-difference-and-relation-between-host-name-and-canonical-name#:~:text=The%20host%20name%20is%20the,host%20is%20not%20actually%20called.)
             */
            List<String> addresses = new ArrayList<>();
            for (String h : hostNames) {
                if (this.conf.getUseHostNameAsBookieID()) {
                    String currAddr = InetAddress.getByName(h).getCanonicalHostName();

                    if (this.conf.getUseHostNameAsBookieID())
                        addresses.add(currAddr.split("\\.", 2)[0]);
                    else
                        addresses.add(currAddr);
                } else {
                    addresses.add(InetAddress.getByName(h).getHostAddress());
                }
            }

            return addresses;
        }
    }

    /**
     * This class uses powermock to mock DNS's static getDefaultHost method to stimulate the scenario where the address
     * is not resolvable
     */
    @RunWith(PowerMockRunner.class)
    @PrepareForTest(DNS.class)
    @PowerMockIgnore("javax.management.*")
    public static class ForceUnknownHostExceptionTest {

        @Test
        public void testShouldThrowAnException() {
            PowerMockito.mockStatic(DNS.class);
            try {
                when(DNS.getDefaultHost(anyString())).thenReturn("https://");
            } catch (UnknownHostException e) {
                Assert.fail("The setup of the mock should not fail");
            }

            try {
                BookieSocketAddress sa = BookieImpl.getBookieAddress(TestBKConfiguration.newServerConfiguration());
                Assert.fail("This retrieve operation should fail instead \""
                        + sa.getHostName() + ":"  + sa.getPort() + "\" is retrieved");
            } catch (UnknownHostException e) {
                Assert.assertTrue(true);
            }
        }
    }

    /**
     * This class uses PowerMock to mock DNS's static getDefaultHost method to stimulate the scenario where the address
     * is not the loopback one
     */
    @RunWith(value = PowerMockRunner.class)
    @PrepareForTest(DNS.class)
    @PowerMockIgnore("javax.management.*")
    public static class ForceNotLoopbackAddressTest {

        @Test
        public void testSimulateGoogle() {
            PowerMockito.mockStatic(DNS.class);
            try {
                when(DNS.getDefaultHost(anyString())).thenReturn("www.google.com");
            } catch (UnknownHostException e) {
                Assert.fail("The setup of the mock should not fail");
            }

            /* Expected value must be evaluated since www.google.com has more than one IP associated */
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            InetSocketAddress expectedSa = new InetSocketAddress("www.google.com", conf.getBookiePort());

            try {
                BookieSocketAddress actualSa = BookieImpl.getBookieAddress(conf);
                Assert.assertEquals("Addresses should be the same", expectedSa.getAddress().getHostAddress(), actualSa.getHostName());
            } catch (UnknownHostException e) {
                Assert.fail("This setup should not throw");
            }
        }
    }
}
