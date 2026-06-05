package com.datahondo.flink.streaming.sink;

import com.datahondo.flink.streaming.config.ApiAuthConfig;
import com.datahondo.flink.streaming.config.TargetConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ApiTargetLayerTest {

    private final ApiTargetLayer layer = new ApiTargetLayer();

    @Test
    void getSinkType_returnsApi() {
        assertEquals("API", layer.getSinkType());
    }

    @Test
    void sink_throwsIllegalArgument_whenUrlIsNull() {
        TargetConfig config = new TargetConfig();
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }

    @Test
    void sink_throwsIllegalArgument_whenUrlIsBlank() {
        TargetConfig config = new TargetConfig();
        config.setUrl("  ");
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }

    @Test
    void sink_throwsIllegalArgument_whenOauth2MissingTokenUrl() {
        TargetConfig config = new TargetConfig();
        config.setUrl("https://api.example.com/ingest");
        ApiAuthConfig auth = new ApiAuthConfig();
        auth.setType(ApiAuthConfig.AuthType.OAUTH2);
        // tokenUrl not set
        config.setApiAuth(auth);
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }

    @Test
    void sink_throwsIllegalArgument_whenUrlIsInvalidUri() {
        TargetConfig config = new TargetConfig();
        config.setUrl("not a url");
        assertThrows(IllegalArgumentException.class,
                () -> layer.sink(null, null, config));
    }
}
