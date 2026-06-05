package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.ApiAuthConfig;
import com.datahondo.flink.streaming.config.SourceConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ApiSourceLayerTest {

    private final ApiSourceLayer layer = new ApiSourceLayer();

    @Test
    void getSourceType_returnsApi() {
        assertEquals("API", layer.getSourceType());
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenUrlIsNull() {
        SourceConfig config = new SourceConfig();
        config.setTableName("prices");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenUrlIsBlank() {
        SourceConfig config = new SourceConfig();
        config.setTableName("prices");
        config.setUrl("   ");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenTableNameIsNull() {
        SourceConfig config = new SourceConfig();
        config.setUrl("https://api.example.com/prices");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenOauth2MissingTokenUrl() {
        SourceConfig config = new SourceConfig();
        config.setTableName("prices");
        config.setUrl("https://api.example.com/prices");
        ApiAuthConfig auth = new ApiAuthConfig();
        auth.setType(ApiAuthConfig.AuthType.OAUTH2);
        auth.setClientId("my-client");
        // tokenUrl intentionally not set
        config.setApiAuth(auth);
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }

    @Test
    void createSourceTable_throwsIllegalArgument_whenUrlIsInvalidUri() {
        SourceConfig config = new SourceConfig();
        config.setTableName("prices");
        config.setUrl("not a valid url with spaces");
        assertThrows(IllegalArgumentException.class,
                () -> layer.createSourceTable(null, null, config));
    }
}
