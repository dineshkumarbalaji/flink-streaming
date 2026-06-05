package com.datahondo.flink.streaming.source;

import com.datahondo.flink.streaming.config.ApiAuthConfig;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OAuthTokenManagerTest {

    @Test
    void constructor_acceptsValidConfig() {
        ApiAuthConfig auth = new ApiAuthConfig();
        auth.setType(ApiAuthConfig.AuthType.OAUTH2);
        auth.setTokenUrl("https://auth.example.com/token");
        auth.setClientId("client-id");
        auth.setClientSecret("secret");
        assertDoesNotThrow(() -> new OAuthTokenManager(auth));
    }

    @Test
    void getToken_throwsIoException_whenTokenUrlUnreachable() {
        ApiAuthConfig auth = new ApiAuthConfig();
        auth.setType(ApiAuthConfig.AuthType.OAUTH2);
        auth.setTokenUrl("http://localhost:1/nonexistent");
        auth.setClientId("client-id");
        auth.setClientSecret("secret");
        OAuthTokenManager mgr = new OAuthTokenManager(auth);
        assertThrows(Exception.class, mgr::getToken);
    }
}
