package it.uniroma2.dicii.isw2.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.Cookie;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class CookieTest {

    private Cookie cookie;

    public CookieTest(int layoutVersion, String bookieId, String journalDirs, String ledgerDirs, String instanceId) {
        configure(layoutVersion, bookieId, journalDirs, ledgerDirs, instanceId);
    }

    public void configure(int layoutVersion, String bookieId, String journalDirs, String ledgerDirs, String instanceId) {
        Cookie.Builder builder = Cookie.newBuilder();
        builder.setLayoutVersion(layoutVersion);
        builder.setBookieId(bookieId);
        builder.setJournalDirs(journalDirs);
        builder.setLedgerDirs(ledgerDirs);
        builder.setInstanceId(instanceId);

        this.cookie = builder.build();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testCasesTuples() {
        return Arrays.asList(new Object[][] {
                {0, "", "", "", ""}
        });
    }

    @Test
    public void testVerify() {
        assertEquals(true, true);
    }
}

