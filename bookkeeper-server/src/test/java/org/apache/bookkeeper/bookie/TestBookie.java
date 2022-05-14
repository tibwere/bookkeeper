package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.LedgerEntry;
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

@RunWith(value = Enclosed.class)
public class TestBookie {

    private static final byte[] DUMMY_PAYLOAD = "THIS IS AN ENTRY".getBytes(StandardCharsets.UTF_8);

    /**
     * This method is based on the description of a {@link LedgerEntry} structure.
     * @param entryNumber entry id
     * @return ByteBuf representation of the entry (VALID)
     */
    public static ByteBuf generateValidEntry(long entryNumber) {
        final long ledgerID = 0;
        final int byteBufSize = Long.BYTES + Long.BYTES + DUMMY_PAYLOAD.length;

        ByteBuf bb = Unpooled.buffer(byteBufSize);
        bb.writeLong(ledgerID);
        bb.writeLong(entryNumber);
        bb.writeBytes(DUMMY_PAYLOAD);

        return bb;
    }

    /**
     * This method is based on the description of a {@link LedgerEntry} structure.
     * @return ByteBuf representation of the entry (INVALID)
     */
    public static ByteBuf generateInvalidEntryWithoutHeader() {
        ByteBuf bb = Unpooled.buffer(DUMMY_PAYLOAD.length);
        bb.writeBytes(DUMMY_PAYLOAD);

        return bb;
    }

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

    @RunWith(value = Parameterized.class)
    public static class AddEntryTests {
        private ByteBuf entry;
        private boolean ackBeforeSync;
        private Object ctx;
        private byte[] masterKey;
        private boolean expectedException;

        private Bookie bookie;

        public AddEntryTests(ByteBuf entry, boolean ackBeforeSync, Object ctx, byte[] masterKey,
                             boolean expectedException) {
            configure(entry, ackBeforeSync, ctx, masterKey, expectedException);
        }

        public void configure(ByteBuf entry, boolean ackBeforeSync, Object ctx, byte[] masterKey,
                              boolean expectedException) {
            this.entry = entry;
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
         *  - entry:            [valid, invalid, null]
         *  - ackBeforeSync:    [true, false]
         *  - ctx:              [null, new instance of object]
         *  - masterKey:        [null, valid]
         *  - needException:    [true, false]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            final byte []validMasterKey = "master".getBytes(StandardCharsets.UTF_8);
            return Arrays.asList(new Object[][]{
                    // ENTRY                                ACK_BEFORE_SYNC     CTX             MASTER_KEY          EXPECTED_EXCEPTION
                    {  generateValidEntry(0),               true,               null,           validMasterKey,     false},
                    {  null,                                false,              null,           validMasterKey,     true},
                    {  generateInvalidEntryWithoutHeader(), true,               new Object(),   null,               true},
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
                this.bookie.addEntry(this.entry, this.ackBeforeSync, generateMockedNopCb(), this.ctx, this.masterKey);

                Assert.assertFalse("An exception should be thrown", this.expectedException);
            } catch (Exception e) {
                Assert.assertTrue("Exception \"" + e.getClass().getName() + "\" should not be thrown",
                        this.expectedException);
            }
        }
    }

    @RunWith(value = Parameterized.class)
    public static class GetBookieAddressTests {

        private ServerConfiguration serverConfiguration;
        private boolean expectedException;

        public GetBookieAddressTests(ServerConfiguration serverConfiguration, boolean expectedException) {
            configure(serverConfiguration, expectedException);
        }

        public void configure(ServerConfiguration serverConfiguration, boolean expectedException) {
            this.serverConfiguration = serverConfiguration;
            this.expectedException = expectedException;
        }

        /**
         * BOUNDARY VALUE ANALYSIS
         *  - serverConfiguration {allowLoopback, denyLoopback}
         *  - expectedException {true, false}
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            ServerConfiguration allowLoopbackCfg = TestBKConfiguration.newServerConfiguration()
                    .setAllowLoopback(true);
            ServerConfiguration denyLoopbackCfg = TestBKConfiguration.newServerConfiguration()
                    .setAllowLoopback(false);

            return Arrays.asList(new Object[][]{
                    // SERVER_CONFIGURATION     EXPECTED_EXCEPTION
                    {  allowLoopbackCfg,        false   },
                    {  denyLoopbackCfg,         true    }
            });
        }

        /**
         * This method calculates the expected value based on a third-party function.
         *
         * @param cfg Server configuration
         * @return dotted decimal representation of the address
         */
        private String getExpectedAddress(ServerConfiguration cfg) throws UnknownHostException {
            /* If an address is specified then that is used */
            if (cfg.getAdvertisedAddress() != null)
                return InetAddress.getByName(cfg.getAdvertisedAddress()).getHostAddress();

            /* Otherwise, use listening interface to derive hostname*/
            String hostname = DNS.getDefaultHost( (cfg.getListeningInterface() == null) ? "default" : cfg.getListeningInterface());
            return InetAddress.getByName(hostname).getHostAddress();
        }

        @Test
        public void testGetBookieAddress() {
            try {
                BookieSocketAddress sa = BookieImpl.getBookieAddress(this.serverConfiguration);
                Assert.assertEquals("Addresses mismatch", getExpectedAddress(this.serverConfiguration),
                        sa.getHostName());
                Assert.assertEquals("Ports mismatch", this.serverConfiguration.getBookiePort(), sa.getPort());
            } catch (UnknownHostException e) {
                Assert.assertTrue("This parameter configuration should cause an exception", this.expectedException);
            }
        }
    }
}
