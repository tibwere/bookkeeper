package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

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
                    {0,                 validEntry,     true,   null,           validMasterKey, false   },
                    {1,                 validEntry,     false,  new Object(),   validMasterKey, true    },
                    {0,                 nullEntry,      false,  null,           null,           true    },
                    {1,                 invalidEntry,   false,  new Object(),   validMasterKey, true    }
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
}
