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
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

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
                Assert.assertEquals(this.builder.getLedgerID(), bb.readLong());
                Assert.assertEquals(this.builder.getEntryID(), bb.readLong());

                int i = 0;
                byte[] expectedPayload = this.builder.getPayload();
                while (i < expectedPayload.length && bb.isReadable())
                    Assert.assertEquals(expectedPayload[i++], bb.readByte());

                Assert.assertTrue(i == expectedPayload.length);
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
        private String expectedAddress;
        private boolean expectedException;

        public GetBookieAddressTests(ServerConfiguration conf, String expectedAddress, boolean expectedException) {
            configure(conf, expectedAddress, expectedException);
        }

        public void configure(ServerConfiguration conf, String expectedAddress, boolean expectedException) {
            this.conf = conf;
            this.expectedAddress = expectedAddress;
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
            String advAddr = "isw2.software.testing";
            ServerConfiguration defaultConf = TestBKConfiguration.newServerConfiguration();
            ServerConfiguration advertisedAddrConf = TestBKConfiguration.newServerConfiguration()
                    .setAdvertisedAddress(advAddr);
            ServerConfiguration nullListeningInterface = TestBKConfiguration.newServerConfiguration()
                    .setListeningInterface(null);
            ServerConfiguration hostNameAsBookieIDConf = TestBKConfiguration.newServerConfiguration()
                    .setUseHostNameAsBookieID(true);
            ServerConfiguration shortHostNameAsBookieIDConf = TestBKConfiguration.newServerConfiguration()
                    .setUseHostNameAsBookieID(true)
                    .setUseShortHostName(true);
            ServerConfiguration denyLoopbackConf = TestBKConfiguration.newServerConfiguration()
                    .setAllowLoopback(false);
            ServerConfiguration invalidPortConf = TestBKConfiguration.newServerConfiguration()
                    .setBookiePort(Integer.MAX_VALUE);

            return Arrays.asList(new Object[][]{
                    // SERVER_CONFIG                EXPECTED_ADDR   EXPECTED_EXCEPTION  NOTE
                    { defaultConf,                  "127.0.0.1",    false   },
                    { advertisedAddrConf,           advAddr,        false   },
                    { nullListeningInterface,       "127.0.1.1",    false   },          // see /etc/hosts
                    { hostNameAsBookieIDConf,       "localhost",    false   },
                    { shortHostNameAsBookieIDConf,  "localhost",    false   },          // does not contain dots (so it is the same as before)
                    { denyLoopbackConf,             null,           true    },
                    { invalidPortConf,              null,           true    }
            });
        }

        @Test
        public void testGetBookieAddress() {
            try {
                BookieSocketAddress sa = BookieImpl.getBookieAddress(this.conf);
                Assert.assertEquals(this.expectedAddress, sa.getHostName());
                Assert.assertFalse("This configuration should throw an exception", this.expectedException);
            } catch (UnknownHostException | IllegalArgumentException e) {
                Assert.assertTrue("This configuration should not throw an exception", this.expectedException);
            }
        }


    }

    /**
     * This class uses powermock to mock DNS's static getDefaultHost method to stimulate the scenario where the address
     * is not resolvable
     */
    @RunWith(PowerMockRunner.class)
    @PrepareForTest(DNS.class)
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
    public static class ForceNotLoopbackAddress {

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
                Assert.assertEquals(expectedSa.getAddress().getHostAddress(), actualSa.getHostName());
            } catch (UnknownHostException e) {
                Assert.fail("This setup should not throw");
            }
        }
    }
}
