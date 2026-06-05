package com.datahondo.flink.streaming.source;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class StoragePathResolverTest {

    @Test
    void detect_returnsLocal_forNullPath() {
        assertEquals(StoragePathResolver.StorageScheme.LOCAL, StoragePathResolver.detect(null));
    }

    @Test
    void detect_returnsLocal_forEmptyPath() {
        assertEquals(StoragePathResolver.StorageScheme.LOCAL, StoragePathResolver.detect(""));
    }

    @Test
    void detect_returnsLocal_forBarePath() {
        assertEquals(StoragePathResolver.StorageScheme.LOCAL,
                StoragePathResolver.detect("/app/data/orders.csv"));
    }

    @Test
    void detect_returnsLocal_forFileScheme() {
        assertEquals(StoragePathResolver.StorageScheme.LOCAL,
                StoragePathResolver.detect("file:///app/data/orders.csv"));
    }

    @Test
    void detect_returnsAdls_forAbfsScheme() {
        assertEquals(StoragePathResolver.StorageScheme.ADLS,
                StoragePathResolver.detect("abfs://raw@myaccount.dfs.core.windows.net/orders/"));
    }

    @Test
    void detect_returnsAdls_forAbfssScheme() {
        assertEquals(StoragePathResolver.StorageScheme.ADLS,
                StoragePathResolver.detect("abfss://raw@myaccount.dfs.core.windows.net/orders/"));
    }

    @Test
    void detect_returnsS3_forS3Scheme() {
        assertEquals(StoragePathResolver.StorageScheme.S3,
                StoragePathResolver.detect("s3://my-bucket/output/orders"));
    }

    @Test
    void detect_returnsS3_forS3aScheme() {
        assertEquals(StoragePathResolver.StorageScheme.S3,
                StoragePathResolver.detect("s3a://my-bucket/output/orders"));
    }

    @Test
    void normalise_convertsS3_toS3a() {
        assertEquals("s3a://my-bucket/prefix",
                StoragePathResolver.normalise("s3://my-bucket/prefix"));
    }

    @Test
    void normalise_leavesS3a_unchanged() {
        assertEquals("s3a://my-bucket/prefix",
                StoragePathResolver.normalise("s3a://my-bucket/prefix"));
    }

    @Test
    void normalise_leavesLocalPath_unchanged() {
        assertEquals("/app/data/file.csv",
                StoragePathResolver.normalise("/app/data/file.csv"));
    }
}
