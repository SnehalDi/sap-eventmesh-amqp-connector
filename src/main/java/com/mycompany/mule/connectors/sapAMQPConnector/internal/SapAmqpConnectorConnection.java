package com.mycompany.mule.connectors.sapAMQPConnector.internal;

// Removed ClientCredentialsState import
import javax.jms.Connection;
import java.time.Instant;

public final class SapAmqpConnectorConnection {

    // Store token and potentially expiry time manually
    private String currentToken;
    private Instant tokenExpiryTime; // Optional: for proactive refresh
    private Connection jmsConnection;

    // Default constructor is fine now
    public SapAmqpConnectorConnection() {}

    // --- Manual Token Management ---
    public String getAccessToken() {
        // Optional: Check expiry time here if implemented
        // if (tokenExpiryTime != null && Instant.now().isAfter(tokenExpiryTime)) {
        //     return null; // Signal that token needs refresh
        // }
        return this.currentToken;
    }

    public void setAccessToken(String token, long expiresInSeconds) {
        this.currentToken = token;
        if (expiresInSeconds > 0) {
            this.tokenExpiryTime = Instant.now().plusSeconds(expiresInSeconds - 60); // Refresh a bit early
        } else {
            this.tokenExpiryTime = null; // No expiry info
        }
    }
    // --- End Manual Token Management ---


    // --- JMS Connection Management ---
    public void setJmsConnection(Connection jmsConnection) {
        this.jmsConnection = jmsConnection;
    }

    public Connection getJmsConnection() {
        return this.jmsConnection;
    }

    public boolean isConnected() {
        // A basic check, could be improved
        return this.jmsConnection != null;
    }

    public void disconnect() throws Exception {
        if (this.jmsConnection != null) {
            try {
                this.jmsConnection.close();
            } finally {
                 this.jmsConnection = null;
                 // Optionally clear token on disconnect if needed: this.currentToken = null;
            }
        }
    }
    // --- End JMS Connection Management ---
}