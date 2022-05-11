package it.uniroma2.dicii.isw2.bookkeeper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.eclipse.jetty.server.Server;
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

    /**
     * This method has been copied from: <a href="https://github.com/apache/bookkeeper/blob/ff3607587a000507ad21e024ed58e51206290684/bookkeeper-server/src/test/java/org/apache/bookkeeper/bookie/LedgerCacheTest.java#L808">LedgerCacheTest (original)</a>
     * @param ledger ledger id
     * @param entry entry id
     * @return ByteBuf representation of the entry
     */
    public static ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = ("ledger-" + ledger + "-" + entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }

    public static BookkeeperInternalCallbacks.WriteCallback generateMockedNopCb() {
        BookkeeperInternalCallbacks.WriteCallback nopCb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
        /* A NOP callback should do nothing when invoked */
        doNothing().when(nopCb).writeComplete(isA(Integer.class), isA(Long.class), isA(Long.class),
                isA(BookieId.class), isA(Object.class));

        return nopCb;
    }

    @RunWith(value = Parameterized.class)
    public static class AddEntryTests {

        private ServerConfiguration serverConfiguration;
        private long ledgerId;
        private long entryId;
        private boolean ackBeforeSync;
        private Object ctx;
        private byte[] masterKey;
        private boolean expectedException;

        private Bookie bookie;

        public AddEntryTests(long ledgerId, long entryId, boolean ackBeforeSync, Object ctx, byte[] masterKey,
                             boolean expectedException) {
            configure(ledgerId, entryId, ackBeforeSync, ctx, masterKey, expectedException);
        }

        public void configure(long ledgerId, long entryId, boolean ackBeforeSync, Object ctx, byte[] masterKey,
                              boolean expectedException) {
            this.serverConfiguration = TestBKConfiguration.newServerConfiguration();
            this.ledgerId = ledgerId;
            this.entryId = entryId;
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
         *  - ledgerId:         [-1, 0, 1]
         *  - entryId:          [-1, 0, 1]
         *  - ackBeforeSync:    [true, false]
         *  - ctx:              [null, new instance of object]
         *  - masterKey:        [null, valid]
         *  - needException:    [true, false]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            final byte []validMasterKey = "master".getBytes(StandardCharsets.UTF_8);
            return Arrays.asList(new Object[][]{
                    // LEDGER_ID    ENTRY_ID    ACK_BEFORE_SYNC     CTX             MASTER_KEY      EXPECTED_EXCEPTION
                    {  0,           0,          true,               null,           validMasterKey, false },
                    {  1,           1,          false,              null,           validMasterKey, false },
                    {  -1,          -1,         true,               new Object(),   null,           true  },
            });
        }

        @Before
        public void startupBookie() {
            this.bookie.start();
        }

        public void shutdownBookie() {
            this.bookie.shutdown();
        }

        @Test
        public void testAddEntry() {
            try {
                this.bookie.addEntry(generateEntry(this.ledgerId, this.entryId), this.ackBeforeSync,
                        generateMockedNopCb(), this.ctx, this.masterKey);

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
                    {allowLoopbackCfg, false},
                    {denyLoopbackCfg, true}
            });
        }

        /**
         * This method calculates the expected value based on a third-party function.
         *
         * Therefore, the correctness of the test case is a function of the correctness of the implementation of the
         * java.net library.
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
