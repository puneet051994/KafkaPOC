package com.example.dynamickafkaconsumers.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kafka.security")
public class SecurityProperties {

    private String protocol; // SSL or SASL_SSL

    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslTruststoreType;

    private String sslKeystoreLocation;
    private String sslKeystorePassword;
    private String sslKeystoreType;

    private String sslKeyPassword;
    private String sslEndpointIdentificationAlgorithm; // typically HTTPS or ""

    private String saslMechanism; // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
    private String saslJaasConfig; // full JAAS config string

    public Map<String, Object> asKafkaProperties() {
        Map<String, Object> props = new HashMap<>();
        putIfNotNull(props, "security.protocol", protocol);
        putIfNotNull(props, "ssl.truststore.location", sslTruststoreLocation);
        putIfNotNull(props, "ssl.truststore.password", sslTruststorePassword);
        putIfNotNull(props, "ssl.truststore.type", sslTruststoreType);
        putIfNotNull(props, "ssl.keystore.location", sslKeystoreLocation);
        putIfNotNull(props, "ssl.keystore.password", sslKeystorePassword);
        putIfNotNull(props, "ssl.keystore.type", sslKeystoreType);
        putIfNotNull(props, "ssl.key.password", sslKeyPassword);
        putIfNotNull(props, "ssl.endpoint.identification.algorithm", sslEndpointIdentificationAlgorithm);
        putIfNotNull(props, "sasl.mechanism", saslMechanism);
        putIfNotNull(props, "sasl.jaas.config", saslJaasConfig);
        return props;
    }

    private static void putIfNotNull(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getSslTruststoreLocation() {
        return sslTruststoreLocation;
    }

    public void setSslTruststoreLocation(String sslTruststoreLocation) {
        this.sslTruststoreLocation = sslTruststoreLocation;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    public void setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public String getSslTruststoreType() {
        return sslTruststoreType;
    }

    public void setSslTruststoreType(String sslTruststoreType) {
        this.sslTruststoreType = sslTruststoreType;
    }

    public String getSslKeystoreLocation() {
        return sslKeystoreLocation;
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation) {
        this.sslKeystoreLocation = sslKeystoreLocation;
    }

    public String getSslKeystorePassword() {
        return sslKeystorePassword;
    }

    public void setSslKeystorePassword(String sslKeystorePassword) {
        this.sslKeystorePassword = sslKeystorePassword;
    }

    public String getSslKeystoreType() {
        return sslKeystoreType;
    }

    public void setSslKeystoreType(String sslKeystoreType) {
        this.sslKeystoreType = sslKeystoreType;
    }

    public String getSslKeyPassword() {
        return sslKeyPassword;
    }

    public void setSslKeyPassword(String sslKeyPassword) {
        this.sslKeyPassword = sslKeyPassword;
    }

    public String getSslEndpointIdentificationAlgorithm() {
        return sslEndpointIdentificationAlgorithm;
    }

    public void setSslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm) {
        this.sslEndpointIdentificationAlgorithm = sslEndpointIdentificationAlgorithm;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
        this.saslJaasConfig = saslJaasConfig;
    }
}

