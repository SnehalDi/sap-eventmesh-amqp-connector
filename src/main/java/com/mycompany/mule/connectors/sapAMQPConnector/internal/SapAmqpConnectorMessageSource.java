package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.connection.ConnectionProvider;
import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.runtime.extension.api.annotation.param.display.Example;
import org.mule.runtime.extension.api.annotation.param.display.Password;
import org.mule.runtime.extension.api.annotation.Expression;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.api.meta.ExpressionSupport;
import org.mule.runtime.extension.api.runtime.source.Source;
import org.mule.runtime.extension.api.runtime.source.SourceCallback;
import org.mule.runtime.extension.api.runtime.operation.Result;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.*;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Enumeration for JMS Acknowledgment Modes
 */
enum AcknowledgmentMode {
    AUTO,
    CLIENT
}

/**
 * Message Source for consuming messages from SAP Event Mesh queues
 */
@Alias("listener")
@DisplayName("Message Listener")
@Summary("Listens for messages from SAP Event Mesh queue")
@MediaType(value = ANY, strict = false)
public class SapAmqpConnectorMessageSource extends Source<Object, MessageAttributes> {

    private final Logger LOGGER = LoggerFactory.getLogger(SapAmqpConnectorMessageSource.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    // JMS property key for AMQP content-type
    private static final String JMS_AMQP_CONTENT_TYPE = "JMS_AMQP_CONTENT_TYPE";
    
    // Qpid JMS INDIVIDUAL_ACKNOWLEDGE mode constant
    private static final int INDIVIDUAL_ACKNOWLEDGE = 101;
    
    @Config
    private SapAmqpConnectorConfiguration config;

    @Connection
    private ConnectionProvider<SapAmqpConnectorConnection> connectionProvider;
     
    @Parameter
    @DisplayName("Token URL")
    @Summary("OAuth2 token endpoint URL")
    @Example("https://your-tenant.authentication.sap.hana.ondemand.com/oauth/token")
    @Expression(ExpressionSupport.SUPPORTED)
    private String tokenUrl;

    @Parameter
    @DisplayName("Client ID")
    @Summary("OAuth2 Client ID")
    @Example("#[p('sap.clientId')]")
    @Expression(ExpressionSupport.SUPPORTED)
    private String clientId;

    @Parameter
    @Password
    @DisplayName("Client Secret")
    @Summary("OAuth2 Client Secret")
    @Example("#[p('sap.clientSecret')]")
    @Expression(ExpressionSupport.SUPPORTED)
    private String clientSecret;
    
    @Parameter
    @DisplayName("Queue Name")
    @Summary("Name of the queue to consume from")
    @Example("myQueue")
    @Expression(ExpressionSupport.SUPPORTED)
    String queueName;

    @Parameter
    @Optional(defaultValue = "1")
    @DisplayName("Number of Consumers")
    @Summary("Number of concurrent message consumers (default: 1)")
    private int numberOfConsumers;

    @Parameter
    @Optional(defaultValue = "AUTO")
    @DisplayName("Acknowledgment Mode")
    @Summary("AUTO: messages auto-acknowledged (default), CLIENT: manual acknowledgment required")
    private AcknowledgmentMode ackMode;

    @Parameter
    @Optional
    @DisplayName("Message Selector")
    @Summary("JMS message selector for filtering messages (optional)")
    @Example("JMSPriority > 5")
    @Expression(ExpressionSupport.SUPPORTED)
    private String messageSelector;

    private jakarta.jms.Connection jmsConnection;
    private List<ConsumerWorker> consumers = new ArrayList<>();

    @Override
    public void onStart(SourceCallback<Object, MessageAttributes> sourceCallback) {
        LOGGER.info("=== Starting SAP Event Mesh Message Listener ===");
        LOGGER.debug("Token URL: {}", tokenUrl);
        LOGGER.debug("Client ID: {}", clientId);
        LOGGER.debug("Queue Name: {}", queueName);
        LOGGER.info("Acknowledgment Mode: {}", ackMode);
        
        if (ackMode == AcknowledgmentMode.CLIENT) {
            LOGGER.warn("===================================================================");
            LOGGER.warn("CLIENT ACKNOWLEDGMENT MODE ENABLED");
            LOGGER.warn("Messages MUST be explicitly acknowledged using 'Acknowledge Message' operation");
            LOGGER.warn("Unacknowledged messages will be redelivered by the broker");
            LOGGER.warn("===================================================================");
        }
        
        running.set(true);

        try {
            // Validate required parameters
            validateParameters();
            
            // Get connection from provider
            SapAmqpConnectorConnection muleConnection = connectionProvider.connect();
            
            // Fetch OAuth2 token
            String accessToken = fetchNewAccessToken();
            if (accessToken == null || accessToken.trim().isEmpty()) {
                LOGGER.error("Failed to obtain access token for message consumption");
                return;
            }
            muleConnection.setAccessToken(accessToken, 3600);
            LOGGER.info("OAuth2 token obtained for message consumption");

            // Build AMQP WebSocket Connection URL
            String connectionUrl = buildConnectionUrl(config.getUri(), accessToken);
            LOGGER.info("Connection URL built for consumer");

            // Create JMS Connection Factory
            JmsConnectionFactory factory = new JmsConnectionFactory();
            factory.setRemoteURI(connectionUrl);
            factory.setUsername(clientId);
            factory.setPassword(accessToken);

            // Create and start JMS connection
            jmsConnection = factory.createConnection();
            jmsConnection.start();
            muleConnection.setJmsConnection(jmsConnection);
            LOGGER.info("JMS Connection established for message consumption");

            // Determine acknowledgment mode
            int sessionAckMode = getAcknowledgmentMode();

            // Start consumer threads
            for (int i = 0; i < numberOfConsumers; i++) {
                ConsumerWorker worker = new ConsumerWorker(
                    jmsConnection, 
                    queueName, 
                    sourceCallback, 
                    sessionAckMode,
                    messageSelector,
                    ackMode == AcknowledgmentMode.CLIENT,
                    i + 1
                );
                consumers.add(worker);
                new Thread(worker, "SAP-AMQP-Consumer-" + (i + 1)).start();
                LOGGER.info("Started consumer thread #{}", i + 1);
            }

            LOGGER.info("Message Listener started successfully with {} consumer(s)", numberOfConsumers);

        } catch (Exception e) {
            LOGGER.error("Failed to start message listener", e);
            onStop();
        }
    }

    /**
     * Validate required parameters
     */
    private void validateParameters() throws ConnectionException {
        if (tokenUrl == null || tokenUrl.trim().isEmpty()) {
            throw new ConnectionException("Token URL is required");
        }
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new ConnectionException("Client ID is required");
        }
        if (clientSecret == null || clientSecret.trim().isEmpty()) {
            throw new ConnectionException("Client Secret is required");
        }
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new ConnectionException("Queue Name is required");
        }
    }

    @Override
    public void onStop() {
        LOGGER.info("=== Stopping SAP Event Mesh Message Listener ===");
        running.set(false);

        // Stop all consumers
        for (ConsumerWorker worker : consumers) {
            worker.stop();
        }
        consumers.clear();

        // Close JMS connection
        if (jmsConnection != null) {
            try {
                jmsConnection.close();
                LOGGER.info("JMS Connection closed");
            } catch (JMSException e) {
                LOGGER.error("Error closing JMS connection", e);
            }
            jmsConnection = null;
        }
        
        // Clear pending acknowledgments if in CLIENT mode
        if (ackMode == AcknowledgmentMode.CLIENT) {
            SapAmqpConnectorOperations.AcknowledgmentRegistry.getInstance().clearAll();
            LOGGER.debug("Cleared all pending acknowledgments");
        }

        LOGGER.info("Message Listener stopped successfully");
    }

    /**
     * Worker class that consumes messages in a separate thread
     * CRITICAL FIX: Each worker creates its own session and connection per message in CLIENT mode
     */
    private class ConsumerWorker implements Runnable {
        private final jakarta.jms.Connection sharedJmsConn;
        private final String queueName;
        private final SourceCallback<Object, MessageAttributes> callback;
        private final int sessionAckMode;
        private final String selector;
        private final boolean isClientAckMode;
        private final int workerId;
        private volatile boolean active = true;

        public ConsumerWorker(jakarta.jms.Connection sharedJmsConn, String queueName, 
                            SourceCallback<Object, MessageAttributes> callback,
                            int sessionAckMode, String selector, 
                            boolean isClientAckMode, int workerId) {
            this.sharedJmsConn = sharedJmsConn;
            this.queueName = queueName;
            this.callback = callback;
            this.sessionAckMode = sessionAckMode;
            this.selector = selector;
            this.isClientAckMode = isClientAckMode;
            this.workerId = workerId;
        }

        @Override
        public void run() {
            LOGGER.debug("Consumer #{} initializing...", workerId);
            
            if (isClientAckMode) {
                // CLIENT MODE: Create new session/connection for each message
                runClientMode();
            } else {
                // AUTO MODE: Reuse session for all messages
                runAutoMode();
            }
        }

        /**
         * AUTO mode: Reuse single session for all messages
         */
        private void runAutoMode() {
            Session session = null;
            MessageConsumer consumer = null;

            try {
                LOGGER.debug("Consumer #{} - AUTO mode initializing...", workerId);
                
                session = sharedJmsConn.createSession(false, sessionAckMode);
                Destination queue = session.createQueue(queueName);
                
                if (selector != null && !selector.trim().isEmpty()) {
                    consumer = session.createConsumer(queue, selector);
                    LOGGER.info("Consumer #{} - Created with message selector: {}", workerId, selector);
                } else {
                    consumer = session.createConsumer(queue);
                    LOGGER.info("Consumer #{} - Created without message selector", workerId);
                }

                LOGGER.info("Consumer #{} ready and listening for messages (AUTO mode)...", workerId);

                while (running.get() && active) {
                    try {
                        Message message = consumer.receive(1000);
                        
                        if (message != null) {
                            processMessageAutoMode(message);
                        }
                    } catch (JMSException e) {
                        if (running.get() && active) {
                            LOGGER.error("Consumer #{} - Error receiving message", workerId, e);
                            Thread.sleep(1000);
                        }
                    }
                }

            } catch (Exception e) {
                LOGGER.error("Consumer #{} - Fatal error", workerId, e);
            } finally {
                try {
                    if (consumer != null) consumer.close();
                    if (session != null) session.close();
                    LOGGER.debug("Consumer #{} - Resources cleaned up", workerId);
                } catch (JMSException e) {
                    LOGGER.error("Consumer #{} - Error closing resources", workerId, e);
                }
            }
        }

        /**
         * CLIENT mode: Create dedicated session/connection for each message
         * This ensures the session remains valid for later acknowledgment
         */
        private void runClientMode() {
            LOGGER.info("Consumer #{} ready and listening for messages (CLIENT mode)...", workerId);
            
            while (running.get() && active) {
                Session dedicatedSession = null;
                MessageConsumer dedicatedConsumer = null;
                
                try {
                    // Create dedicated session for this message
                    dedicatedSession = sharedJmsConn.createSession(false, sessionAckMode);
                    Destination queue = dedicatedSession.createQueue(queueName);
                    
                    if (selector != null && !selector.trim().isEmpty()) {
                        dedicatedConsumer = dedicatedSession.createConsumer(queue, selector);
                    } else {
                        dedicatedConsumer = dedicatedSession.createConsumer(queue);
                    }
                    
                    // Receive message with timeout
                    Message message = dedicatedConsumer.receive(1000);
                    
                    if (message != null) {
                        // Process and register for acknowledgment
                        // Session ownership transfers to AcknowledgmentRegistry
                        processMessageClientMode(message, dedicatedSession);
                        
                        // DO NOT close session - it's now owned by AcknowledgmentRegistry
                        dedicatedSession = null;
                    } else {
                        // No message, close this session and try again
                        if (dedicatedConsumer != null) dedicatedConsumer.close();
                        if (dedicatedSession != null) dedicatedSession.close();
                    }
                    
                } catch (JMSException e) {
                    if (running.get() && active) {
                        LOGGER.error("Consumer #{} - Error in CLIENT mode", workerId, e);
                        
                        // Clean up on error
                        try {
                            if (dedicatedConsumer != null) dedicatedConsumer.close();
                            if (dedicatedSession != null) dedicatedSession.close();
                        } catch (Exception cleanupEx) {
                            LOGGER.debug("Error during cleanup: {}", cleanupEx.getMessage());
                        }
                        
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.info("Consumer #{} interrupted", workerId);
                    break;
                }
            }
            
            LOGGER.debug("Consumer #{} - CLIENT mode loop exited", workerId);
        }

        /**
         * Process message in AUTO mode (message is auto-acknowledged)
         */
        private void processMessageAutoMode(Message message) {
            try {
                LOGGER.debug("Consumer #{} - Message received (AUTO mode)", workerId);

                byte[] payloadBytes = extractPayloadAsBytes(message);
                MessageAttributes attributes = extractMessageAttributes(message);
                attributes.setRequiresAcknowledgment(false);
                
                String contentType = attributes.getContentType();
                org.mule.runtime.api.metadata.MediaType outputMediaType;
                
                if (contentType != null && !contentType.trim().isEmpty()) {
                    try {
                        outputMediaType = org.mule.runtime.api.metadata.MediaType.parse(contentType);
                    } catch (Exception e) {
                        outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
                    }
                } else {
                    outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
                }
                
                LOGGER.info("Consumer #{} - Processing message [ID: {}, Size: {} bytes, Mode: AUTO]", 
                    workerId, attributes.getMessageId(), payloadBytes.length);
                
                Object outputPayload = parsePayload(payloadBytes, outputMediaType);
                
                Result<Object, MessageAttributes> result = Result.<Object, MessageAttributes>builder()
                    .output(outputPayload)
                    .attributes(attributes)
                    .mediaType(outputMediaType)
                    .build();

                callback.handle(result);
                
                LOGGER.info("Consumer #{} - Message processed successfully (AUTO acknowledged)", workerId);

            } catch (Exception e) {
                LOGGER.error("Consumer #{} - Error processing message", workerId, e);
            }
        }

        /**
         * Process message in CLIENT mode
         * CRITICAL: Session is NOT closed here - ownership transfers to AcknowledgmentRegistry
         */
        private void processMessageClientMode(Message message, Session dedicatedSession) throws InterruptedException {
            String acknowledgmentId = null;
            
            try {
                LOGGER.debug("Consumer #{} - Message received (CLIENT mode)", workerId);

                byte[] payloadBytes = extractPayloadAsBytes(message);
                MessageAttributes attributes = extractMessageAttributes(message);
                
                // Register message for acknowledgment with its dedicated session
                // Connection is shared and managed by the Message Source, not per-message
                acknowledgmentId = SapAmqpConnectorOperations.AcknowledgmentRegistry
                    .getInstance().registerMessage(message, dedicatedSession);
                attributes.setackId(acknowledgmentId);
                attributes.setRequiresAcknowledgment(true);
                
                LOGGER.debug("Consumer #{} - CLIENT mode: Message registered for acknowledgment", workerId);
                LOGGER.debug("Consumer #{} - Acknowledgment ID: {}", workerId, acknowledgmentId);
                
                String contentType = attributes.getContentType();
                org.mule.runtime.api.metadata.MediaType outputMediaType;
                
                if (contentType != null && !contentType.trim().isEmpty()) {
                    try {
                        outputMediaType = org.mule.runtime.api.metadata.MediaType.parse(contentType);
                    } catch (Exception e) {
                        outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
                    }
                } else {
                    outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
                }
                
                LOGGER.info("Consumer #{} - Processing message [ID: {}, Size: {} bytes, Mode: CLIENT]", 
                    workerId, attributes.getMessageId(), payloadBytes.length);
                
                Object outputPayload = parsePayload(payloadBytes, outputMediaType);
                
                Result<Object, MessageAttributes> result = Result.<Object, MessageAttributes>builder()
                    .output(outputPayload)
                    .attributes(attributes)
                    .mediaType(outputMediaType)
                    .build();

                callback.handle(result);
                
                LOGGER.info("Consumer #{} - Message processed successfully", workerId);
                LOGGER.warn("Consumer #{} - Message awaiting manual acknowledgment", workerId);

            } catch (Exception e) {
                LOGGER.error("Consumer #{} - Error processing message", workerId, e);
                
                // Clean up acknowledgment registration if processing failed
                if (acknowledgmentId != null) {
                    LOGGER.warn("Consumer #{} - Removing acknowledgment registration due to error", workerId);
                    SapAmqpConnectorOperations.AcknowledgmentRegistry
                        .getInstance().removePendingAcknowledgment(acknowledgmentId);
                }
                
                // Close the dedicated session on error
                try {
                    if (dedicatedSession != null) {
                        dedicatedSession.close();
                    }
                } catch (Exception closeEx) {
                    LOGGER.debug("Error closing session: {}", closeEx.getMessage());
                }
                
                LOGGER.warn("Consumer #{} - Message NOT acknowledged - will be redelivered by broker", workerId);
            }
        }

        /**
         * Extract payload as byte array from JMS message
         */
        private byte[] extractPayloadAsBytes(Message message) throws Exception {
            if (message instanceof BytesMessage) {
                BytesMessage bytesMessage = (BytesMessage) message;
                long bodyLength = bytesMessage.getBodyLength();
                
                if (bodyLength > Integer.MAX_VALUE) {
                    throw new RuntimeException("Message too large: " + bodyLength + " bytes");
                }
                
                if (bodyLength == 0) {
                    return new byte[0];
                }
                
                byte[] bytes = new byte[(int) bodyLength];
                bytesMessage.readBytes(bytes);
                return bytes;
                
            } else if (message instanceof TextMessage) {
                String text = ((TextMessage) message).getText();
                return text != null ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
                
            } else if (message instanceof ObjectMessage) {
                Object obj = ((ObjectMessage) message).getObject();
                if (obj == null) return new byte[0];
                if (obj instanceof byte[]) return (byte[]) obj;
                if (obj instanceof String) return ((String) obj).getBytes(StandardCharsets.UTF_8);
                return objectMapper.writeValueAsString(obj).getBytes(StandardCharsets.UTF_8);
                
            } else if (message instanceof MapMessage) {
                MapMessage mapMessage = (MapMessage) message;
                Map<String, Object> map = new HashMap<>();
                Enumeration<?> mapNames = mapMessage.getMapNames();
                while (mapNames.hasMoreElements()) {
                    String name = (String) mapNames.nextElement();
                    map.put(name, mapMessage.getObject(name));
                }
                return objectMapper.writeValueAsString(map).getBytes(StandardCharsets.UTF_8);
            }
            
            return message.toString().getBytes(StandardCharsets.UTF_8);
        }

        /**
         * Parse payload based on content type
         */
        private Object parsePayload(byte[] payloadBytes, org.mule.runtime.api.metadata.MediaType mediaType) {
            try {
                String mimeType = mediaType.toRfcString().toLowerCase();
                
                if (mimeType.contains("json") || mimeType.contains("xml") || mimeType.startsWith("text/")) {
                    return new String(payloadBytes, StandardCharsets.UTF_8);
                }
                
                return new java.io.ByteArrayInputStream(payloadBytes);
                
            } catch (Exception e) {
                return new java.io.ByteArrayInputStream(payloadBytes);
            }
        }

        /**
         * Extract message attributes including AMQP properties
         */
        private MessageAttributes extractMessageAttributes(Message message) throws Exception {
            MessageAttributes attributes = new MessageAttributes();

            // Standard JMS headers
            attributes.setMessageId(message.getJMSMessageID());
            attributes.setTimestamp(message.getJMSTimestamp());
            attributes.setCorrelationId(message.getJMSCorrelationID());
            attributes.setReplyTo(message.getJMSReplyTo() != null ? message.getJMSReplyTo().toString() : null);
            attributes.setDestination(message.getJMSDestination() != null ? message.getJMSDestination().toString() : null);
            attributes.setDeliveryMode(message.getJMSDeliveryMode());
            attributes.setRedelivered(message.getJMSRedelivered());
            attributes.setType(message.getJMSType());
            attributes.setExpiration(message.getJMSExpiration());
            attributes.setPriority(message.getJMSPriority());
            
            // Extract AMQP properties
            try {
                JmsMessage jmsMessage = (JmsMessage) message;
                AmqpJmsMessageFacade facade = (AmqpJmsMessageFacade) jmsMessage.getFacade();
                
                if (facade.getContentType() != null) {
                    attributes.setContentType(facade.getContentType().toString());
                }
                
                if (facade.getUserId() != null) {
                    attributes.setUserId(facade.getUserId().toString());
                }
                
                if (facade.getGroupId() != null) {
                    attributes.setGroupId(facade.getGroupId());
                }
                
                attributes.setGroupSequence(facade.getGroupSequence());
                
                if (facade.getReplyToGroupId() != null) {
                    attributes.setReplyToGroupId(facade.getReplyToGroupId());
                }
                
                // Extract custom properties
                Map<String, Object> customProperties = new HashMap<>();
                Set<String> propertyNames = new HashSet<>();
                facade.getApplicationPropertyNames(propertyNames);
                
                for (String name : propertyNames) {
                    Object value = facade.getApplicationProperty(name);
                    customProperties.put(name, value);
                }
                
                // Add JMS properties
                java.util.Enumeration<?> jmsPropertyNames = message.getPropertyNames();
                while (jmsPropertyNames.hasMoreElements()) {
                    String propertyName = (String) jmsPropertyNames.nextElement();
                    
                    if (JMS_AMQP_CONTENT_TYPE.equals(propertyName) ||
                        "JMSXContentType".equals(propertyName)) {
                        continue;
                    }
                    
                    if (!customProperties.containsKey(propertyName)) {
                        Object propertyValue = message.getObjectProperty(propertyName);
                        customProperties.put(propertyName, propertyValue);
                    }
                }
                
                attributes.setCustomProperties(customProperties);
                
            } catch (Exception e) {
                // Fallback to standard JMS properties
                Map<String, Object> customProperties = new HashMap<>();
                java.util.Enumeration<?> propertyNames = message.getPropertyNames();
                while (propertyNames.hasMoreElements()) {
                    String propertyName = (String) propertyNames.nextElement();
                    
                    if (JMS_AMQP_CONTENT_TYPE.equals(propertyName) ||
                        "JMSXContentType".equals(propertyName)) {
                        continue;
                    }
                    
                    Object propertyValue = message.getObjectProperty(propertyName);
                    customProperties.put(propertyName, propertyValue);
                }
                attributes.setCustomProperties(customProperties);
            }

            return attributes;
        }

        public void stop() {
            active = false;
            LOGGER.info("Consumer #{} - Stop signal received", workerId);
        }
    }

    /**
     * Get JMS acknowledgment mode
     */
    private int getAcknowledgmentMode() {
        if (ackMode == null || ackMode == AcknowledgmentMode.AUTO) {
            LOGGER.debug("Using AUTO_ACKNOWLEDGE mode");
            return Session.AUTO_ACKNOWLEDGE;
        }
        
        if (ackMode == AcknowledgmentMode.CLIENT) {
            LOGGER.debug("Using CLIENT_ACKNOWLEDGE mode");
            return INDIVIDUAL_ACKNOWLEDGE;
        }
        
        return Session.AUTO_ACKNOWLEDGE;
    }

    /**
     * Build AMQP WebSocket connection URL with OAuth token
     */
    private String buildConnectionUrl(String wsUri, String accessToken) throws Exception {
        try {
            URI uri = new URI(wsUri);
            String protocol = uri.getScheme();
            String host = uri.getHost();
            String path = uri.getPath();
            int port = uri.getPort();
            
            String amqpProtocol;
            int targetPort;
            
            if ("wss".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpwss";
                targetPort = (port > 0) ? port : 443;
            } else if ("ws".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpws";
                targetPort = (port > 0) ? port : 80;
            } else if ("amqpwss".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpwss";
                targetPort = (port > 0) ? port : 443;
            } else if ("amqpws".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpws";
                targetPort = (port > 0) ? port : 80;
            } else {
                throw new ConnectionException("Unsupported protocol: " + protocol);
            }
            
            String encodedToken = URLEncoder.encode("Bearer " + accessToken, "UTF-8");
            
            return String.format(
                "%s://%s:%d%s?transport.tcpNoDelay=true&transport.ws.httpHeader.Authorization=%s",
                amqpProtocol, host, targetPort, path, encodedToken
            );
            
        } catch (Exception e) {
            LOGGER.error("Error parsing URI: {}", wsUri, e);
            throw new ConnectionException("Invalid URI format: " + wsUri, e);
        }
    }

    /**
     * Fetch OAuth2 access token
     */
    private String fetchNewAccessToken() throws ConnectionException {
        LOGGER.info("Fetching OAuth2 token...");
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(tokenUrl);

        try {
            String auth = clientId + ":" + clientSecret;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            httpPost.setHeader("Authorization", "Basic " + encodedAuth);
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");

            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("grant_type", "client_credentials"));
            httpPost.setEntity(new UrlEncodedFormEntity(params));

            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode >= 200 && statusCode < 300) {
                String responseBody = EntityUtils.toString(entity);
                JsonNode jsonNode = objectMapper.readTree(responseBody);
                
                if (jsonNode.has("access_token")) {
                    String token = jsonNode.get("access_token").asText();
                    LOGGER.info("Successfully fetched OAuth2 token");
                    return token;
                } else {
                    throw new ConnectionException("Access token not found in response");
                }
            } else {
                String errorBody = EntityUtils.toString(entity);
                LOGGER.error("Token fetch failed - Status: {}, Body: {}", statusCode, errorBody);
                throw new ConnectionException("Failed to fetch token. Status: " + statusCode);
            }
        } catch (Exception e) {
            LOGGER.error("Error during token fetch", e);
            throw new ConnectionException("Error fetching token: " + e.getMessage(), e);
        } finally {
            try {
                httpClient.close();
            } catch (Exception e) {
                LOGGER.error("Error closing HTTP client", e);
            }
        }
    }
}