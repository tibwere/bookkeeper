package org.apache.bookkeeper.bookie;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.proto.SimpleBookieServiceInfoProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * This class allows you to instantiate Bookie using a default configuration and also allowing you to modify the
 * LedgerStorage in order to insert a mock
 *
 * @author Simone Tiberi
 */
public class BookieImplParameters {
    private ServerConfiguration conf;
    private MetadataBookieDriver metadataBookieDriver;
    private RegistrationManager registrationManager;
    private LedgerManagerFactory ledgerManagerFactory;
    private LedgerManager ledgerManager;
    private DiskChecker diskChecker;
    private LedgerDirsManager ledgerDirsManager;

    private LedgerDirsManager indexDirsManager;
    private LedgerStorage ledgerStorage;

    public BookieImplParameters(ServerConfiguration conf, MetadataBookieDriver metadataBookieDriver,
                                RegistrationManager registrationManager,
                                LedgerManagerFactory ledgerManagerFactory,
                                LedgerManager ledgerManager,
                                DiskChecker diskChecker,
                                LedgerDirsManager ledgerDirsManager,
                                LedgerDirsManager indexDirsManager,
                                LedgerStorage ledgerStorage) {
        this.conf = conf;
        this.metadataBookieDriver = metadataBookieDriver;
        this.registrationManager = registrationManager;
        this.ledgerManagerFactory = ledgerManagerFactory;
        this.ledgerManager = ledgerManager;
        this.diskChecker = diskChecker;
        this.ledgerDirsManager = ledgerDirsManager;
        this.indexDirsManager = indexDirsManager;
        this.ledgerStorage = ledgerStorage;
    }

    /**
     * Default build of BookieImpl (see {@link TestBookieImpl.ResourceBuilder#build()})
     * specifying a configuration
     */
    public static BookieImplParameters getDefaultParametersWithConfiguration(ServerConfiguration conf) throws MetadataException, IOException {

        MetadataBookieDriver metadataBookieDriver = new NullMetadataBookieDriver();
        RegistrationManager registrationManager = metadataBookieDriver.createRegistrationManager();
        LedgerManagerFactory ledgerManagerFactory = metadataBookieDriver.getLedgerManagerFactory();
        LedgerManager ledgerManager = ledgerManagerFactory.newLedgerManager();
        DiskChecker diskChecker = BookieResources.createDiskChecker(conf);
        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(conf, diskChecker,
                NullStatsLogger.INSTANCE);
        LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(conf, diskChecker,
                NullStatsLogger.INSTANCE, ledgerDirsManager);
        LedgerStorage storage = BookieResources.createLedgerStorage(
                conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

        return new BookieImplParameters(conf, metadataBookieDriver, registrationManager, ledgerManagerFactory,
                ledgerManager, diskChecker, ledgerDirsManager, indexDirsManager, storage);
    }

    /**
     * Default build of BookieImpl (see {@link TestBookieImpl.ResourceBuilder#build()})
     * using default configuration
     */
    public static BookieImplParameters getDefaultParameters() throws MetadataException, IOException {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        return getDefaultParametersWithConfiguration(conf);
    }

    /**
     * This method lets you to inject a mock :)
     */
    public BookieImplParameters withLedgerStorage(LedgerStorage ledgerStorage) {
        this.ledgerStorage = ledgerStorage;
        return this;
    }

    /**
     * Build BookieImpl starting from chosen configuration
     */
    public BookieImpl buildBookieInstance() throws IOException, InterruptedException, BookieException {
        return new BookieImpl(this.conf, this.registrationManager, this.ledgerStorage, this.diskChecker,
                this.ledgerDirsManager, this.indexDirsManager,
                NullStatsLogger.INSTANCE,UnpooledByteBufAllocator.DEFAULT,
                new SimpleBookieServiceInfoProvider(conf));
    }

    /**
     * Close all resources associated with a BookieImpl instance
     */
    public void closeAll() throws IOException {
        this.ledgerManager.close();
        this.ledgerManagerFactory.close();
        this.registrationManager.close();
        this.metadataBookieDriver.close();
    }
}

