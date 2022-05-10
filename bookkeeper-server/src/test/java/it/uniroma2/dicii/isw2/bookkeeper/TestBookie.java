package it.uniroma2.dicii.isw2.bookkeeper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

    @RunWith(value = Parameterized.class)
    public static class AddEntryTests {

        private ServerConfiguration serverConfiguration;
        private long ledgerId;
        private long entryId;
        private boolean ackBeforeSync;
        private Object ctx;
        private byte[] masterKey;
        private boolean hasToThrowException;

        public AddEntryTests(long ledgerId, long entryId, boolean ackBeforeSync, Object ctx, byte[] masterKey,
                             boolean hasToThrowException) {
            configure(ledgerId, entryId, ackBeforeSync, ctx, masterKey, hasToThrowException);
        }

        public void configure(long ledgerId, long entryId, boolean ackBeforeSync, Object ctx, byte[] masterKey,
                              boolean hasToThrowException) {
            this.serverConfiguration = TestBKConfiguration.newServerConfiguration();
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.ackBeforeSync = ackBeforeSync;
            this.ctx = ctx;
            this.masterKey = masterKey;
            this.hasToThrowException = hasToThrowException;
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {
            return Arrays.asList(new Object[][]{
                    {0, 0, true, null, "master".getBytes(StandardCharsets.UTF_8), false}, // "test" test-case :)
            });
        }

        private BookkeeperInternalCallbacks.WriteCallback generateMockedNopCb() {
            BookkeeperInternalCallbacks.WriteCallback nopCb = mock(BookkeeperInternalCallbacks.WriteCallback.class);
            /* A NOP callback should do nothing when invoked */
            doNothing().when(nopCb).writeComplete(isA(Integer.class), isA(Long.class), isA(Long.class),
                    isA(BookieId.class), isA(Object.class));

            return nopCb;
        }

        @Test
        public void testAddEntry() {
            try {
                Bookie bookie = new TestBookieImpl(this.serverConfiguration);
                bookie.start();
                bookie.addEntry(generateEntry(this.ledgerId, this.entryId), this.ackBeforeSync,
                        this.generateMockedNopCb(), this.ctx, this.masterKey);

                bookie.shutdown();

                Assert.assertFalse("An exception should be thrown", this.hasToThrowException);

            } catch (Exception e) {
                Assert.assertTrue("Exception \"" + e.getClass().getName() + "\" should not be thrown",
                        this.hasToThrowException);
            }
        }
    }
}
