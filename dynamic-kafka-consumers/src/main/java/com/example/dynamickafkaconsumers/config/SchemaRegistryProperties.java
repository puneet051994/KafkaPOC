package com.example.dynamickafkaconsumers.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kafka.schema-registry")
public class SchemaRegistryProperties {

    private String url; // https://sr.yourdomain:8081
    private String basicAuthUserInfo; // username:password
    private String basicAuthCredentialsSource; // USER_INFO

    public Map<String, Object> asKafkaProperties() {
        Map<String, Object> props = new HashMap<>();
        putIfNotNull(props, "schema.registry.url", url);
        putIfNotNull(props, "basic.auth.user.info", basicAuthUserInfo);
        putIfNotNull(props, "basic.auth.credentials.source", basicAuthCredentialsSource);
        return props;
    }

    private static void putIfNotNull(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getBasicAuthUserInfo() {
        return basicAuthUserInfo;
    }

    public void setBasicAuthUserInfo(String basicAuthUserInfo) {
        this.basicAuthUserInfo = basicAuthUserInfo;
    }

    public String getBasicAuthCredentialsSource() {
        return basicAuthCredentialsSource;
    }

    public void setBasicAuthCredentialsSource(String basicAuthCredentialsSource) {
        this.basicAuthCredentialsSource = basicAuthCredentialsSource;
    }
}

