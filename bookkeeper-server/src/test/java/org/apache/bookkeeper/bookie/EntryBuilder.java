package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

public class EntryBuilder {

    private static final long INVALID_ID = -1;
    private static final byte[]INVALID_PAYLOAD = "invalid".getBytes(StandardCharsets.UTF_8);

    private final long ledgerID;
    private final long entryID;
    private final byte[] payload;
    private final EntryType type;

    private EntryBuilder(long ledgerID, long entryID, byte[] payload, EntryType type) {
        this.ledgerID = ledgerID;
        this.entryID = entryID;
        this.payload = payload;
        this.type = type;
    }

    public static EntryBuilder validEntry(long ledgerID, long entryID) {
        byte[]payload = ("this is the entry with LID: " + ledgerID + "and EID: " + entryID)
                .getBytes(StandardCharsets.UTF_8);
        return new EntryBuilder(ledgerID, entryID, payload, EntryType.VALID);
    }

    public static EntryBuilder invalidEntry() {
        return new EntryBuilder(INVALID_ID, INVALID_ID, INVALID_PAYLOAD, EntryType.INVALID);
    }

    public static EntryBuilder nullEntry() {
        return new EntryBuilder(INVALID_ID, INVALID_ID, null, EntryType.NULL);
    }

    public long getLedgerID() {
        return ledgerID;
    }

    public long getEntryID() {
        return entryID;
    }

    public byte[] getPayload() {
        return payload;
    }

    public EntryType getType() {
        return type;
    }

    public ByteBuf build() {
        ByteBuf bb;
        switch (this.type) {
            case VALID:
                bb = Unpooled.buffer( 2*Long.BYTES + this.payload.length);
                bb.writeLong(this.ledgerID);
                bb.writeLong(this.entryID);
                bb.writeBytes(this.payload);
                return bb;

            case INVALID:
                bb = Unpooled.buffer(this.payload.length);
                bb.writeBytes(this.payload);
                return bb;

            default:
                return null;
        }
    }

    public enum EntryType {
        VALID, INVALID, NULL
    }
}
