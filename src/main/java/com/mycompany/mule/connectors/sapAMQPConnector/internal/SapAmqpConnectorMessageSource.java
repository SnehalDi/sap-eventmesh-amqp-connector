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
    CLIENT,
    DUPS_OK
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
    @Summary("Message acknowledgment mode")
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

        LOGGER.info("Message Listener stopped successfully");
    }

    /**
     * Worker class that consumes messages in a separate thread
     */
    private class ConsumerWorker implements Runnable {
        private final jakarta.jms.Connection jmsConn;
        private final String queueName;
        private final SourceCallback<Object, MessageAttributes> callback;
        private final int sessionAckMode;
        private final String selector;
        private final int workerId;
        private volatile boolean active = true;

        public ConsumerWorker(jakarta.jms.Connection jmsConn, String queueName, 
                            SourceCallback<Object, MessageAttributes> callback,
                            int sessionAckMode, String selector, int workerId) {
            this.jmsConn = jmsConn;
            this.queueName = queueName;
            this.callback = callback;
            this.sessionAckMode = sessionAckMode;
            this.selector = selector;
            this.workerId = workerId;
        }

        @Override
        public void run() {
            Session session = null;
            MessageConsumer consumer = null;

            try {
                LOGGER.debug("Consumer #{} initializing...", workerId);
                
                // Create session
                session = jmsConn.createSession(
                    sessionAckMode == Session.CLIENT_ACKNOWLEDGE,
                    sessionAckMode
                );
                LOGGER.debug("Consumer #{} - Session created", workerId);

                // Create queue destination
                Destination queue = session.createQueue(queueName);
                LOGGER.debug("Consumer #{} - Queue destination: {}", workerId, queueName);

                // Create consumer with or without selector
                if (selector != null && !selector.trim().isEmpty()) {
                    consumer = session.createConsumer(queue, selector);
                    LOGGER.info("Consumer #{} - Created with message selector: {}", workerId, selector);
                } else {
                    consumer = session.createConsumer(queue);
                    LOGGER.info("Consumer #{} - Created without message selector", workerId);
                }

                LOGGER.info("Consumer #{} ready and listening for messages...", workerId);

                // Message consumption loop
                while (running.get() && active) {
                    try {
                        // Receive message with 1 second timeout
                        Message message = consumer.receive(1000);
                        
                        if (message != null) {
                            processMessage(message, session, sessionAckMode);
                        }
                    } catch (JMSException e) {
                        if (running.get() && active) {
                            LOGGER.error("Consumer #{} - Error receiving message", workerId, e);
                            Thread.sleep(1000); // Wait before retry
                        }
                    }
                }

            } catch (Exception e) {
                LOGGER.error("Consumer #{} - Fatal error", workerId, e);
            } finally {
                // Cleanup resources
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
         * Process received JMS message and invoke callback
         */
        private void processMessage(Message message, Session session, int ackMode) {
            try {
                LOGGER.debug("Consumer #{} - Message received", workerId);

                // Extract message payload as bytes
                byte[] payloadBytes = extractPayloadAsBytes(message);
                
                // Extract message attributes (including AMQP properties)
                MessageAttributes attributes = extractMessageAttributes(message);
                
                // Get content-type from attributes (extracted from AMQP properties)
                String contentType = attributes.getContentType();
                org.mule.runtime.api.metadata.MediaType outputMediaType;
                
                if (contentType != null && !contentType.trim().isEmpty()) {
                    try {
                        outputMediaType = org.mule.runtime.api.metadata.MediaType.parse(contentType);
                        LOGGER.debug("Consumer #{} - Using AMQP content-type: {}", workerId, contentType);
                    } catch (Exception e) {
                        LOGGER.warn("Consumer #{} - Invalid content-type '{}', defaulting to ANY: {}", 
                            workerId, contentType, e.getMessage());
                        outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
                    }
                } else {
                    LOGGER.debug("Consumer #{} - No content-type found, defaulting to ANY", workerId);
                    outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
                }
                
                LOGGER.info("Consumer #{} - Processing message [ID: {}, Content-Type: {}, Size: {} bytes]", 
                    workerId, 
                    attributes.getMessageId(), 
                    contentType,
                    payloadBytes.length);
                
                // Parse payload based on content type from AMQP properties
                Object outputPayload = parsePayload(payloadBytes, outputMediaType);
                
                // Create result with proper media type
                Result<Object, MessageAttributes> result = Result.<Object, MessageAttributes>builder()
                    .output(outputPayload)
                    .attributes(attributes)
                    .mediaType(outputMediaType)
                    .build();

                // Invoke callback to pass message to Mule flow
                callback.handle(result);

                // Manual acknowledgment if CLIENT mode
                if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                    message.acknowledge();
                    LOGGER.debug("Consumer #{} - Message acknowledged", workerId);
                }

                LOGGER.info("Consumer #{} - Message processed successfully", workerId);

            } catch (Exception e) {
                LOGGER.error("Consumer #{} - Error processing message", workerId, e);
                
                // On error, try to recover session
                try {
                    if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                        session.recover();
                        LOGGER.info("Consumer #{} - Session recovered", workerId);
                    }
                } catch (JMSException ex) {
                    LOGGER.error("Consumer #{} - Failed to recover session", workerId, ex);
                }
            }
        }

        /**
         * Extract payload as byte array from JMS message
         */
        private byte[] extractPayloadAsBytes(Message message) throws Exception {
            LOGGER.debug("Consumer #{} - Message type: {}", workerId, message.getClass().getName());
            
            // Log AMQP properties
            logAmqpProperties(message);
            
            // Extract payload based on JMS message type
            if (message instanceof BytesMessage) {
                BytesMessage bytesMessage = (BytesMessage) message;
                long bodyLength = bytesMessage.getBodyLength();
                
                LOGGER.debug("Consumer #{} - BytesMessage detected (length: {} bytes)", workerId, bodyLength);
                
                if (bodyLength > Integer.MAX_VALUE) {
                    throw new RuntimeException("Message too large: " + bodyLength + " bytes");
                }
                
                if (bodyLength == 0) {
                    LOGGER.warn("Consumer #{} - BytesMessage has zero length body", workerId);
                    return new byte[0];
                }
                
                byte[] bytes = new byte[(int) bodyLength];
                int bytesRead = bytesMessage.readBytes(bytes);
                
                LOGGER.debug("Consumer #{} - Extracted {} bytes from BytesMessage", workerId, bytesRead);
                return bytes;
                
            } else if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                String text = textMessage.getText();
                
                LOGGER.debug("Consumer #{} - TextMessage detected", workerId);
                
                if (text == null) {
                    LOGGER.warn("Consumer #{} - TextMessage has null content", workerId);
                    return new byte[0];
                }
                
                // Log preview of text content
                String preview = text.length() > 100 ? text.substring(0, 100) + "..." : text;
                LOGGER.debug("Consumer #{} - Text preview: {}", workerId, preview);
                LOGGER.debug("Consumer #{} - Text length: {} characters", workerId, text.length());
                
                // Convert text to bytes using UTF-8
                byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
                LOGGER.debug("Consumer #{} - Extracted {} bytes from TextMessage", workerId, bytes.length);
                
                return bytes;
                
            } else if (message instanceof ObjectMessage) {
                ObjectMessage objMessage = (ObjectMessage) message;
                Object obj = objMessage.getObject();
                
                LOGGER.debug("Consumer #{} - ObjectMessage detected", workerId);
                
                if (obj == null) {
                    LOGGER.warn("Consumer #{} - ObjectMessage has null content", workerId);
                    return new byte[0];
                }
                
                // Handle different object types
                if (obj instanceof byte[]) {
                    byte[] bytes = (byte[]) obj;
                    LOGGER.debug("Consumer #{} - ObjectMessage contains byte array of {} bytes", workerId, bytes.length);
                    return bytes;
                } else if (obj instanceof String) {
                    String text = (String) obj;
                    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - ObjectMessage contains String of {} characters ({} bytes)", 
                        workerId, text.length(), bytes.length);
                    return bytes;
                } else {
                    // Serialize to JSON
                    LOGGER.debug("Consumer #{} - ObjectMessage contains {} object, serializing to JSON", 
                        workerId, obj.getClass().getSimpleName());
                    String json = objectMapper.writeValueAsString(obj);
                    return json.getBytes(StandardCharsets.UTF_8);
                }
                
            } else if (message instanceof MapMessage) {
                MapMessage mapMessage = (MapMessage) message;
                
                LOGGER.debug("Consumer #{} - MapMessage detected, converting to JSON", workerId);
                
                // Convert MapMessage to JSON
                Map<String, Object> map = new HashMap<>();
                Enumeration<?> mapNames = mapMessage.getMapNames();
                
                while (mapNames.hasMoreElements()) {
                    String name = (String) mapNames.nextElement();
                    map.put(name, mapMessage.getObject(name));
                }
                
                String json = objectMapper.writeValueAsString(map);
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                LOGGER.info("Consumer #{} - Extracted {} bytes from MapMessage (converted to JSON)", 
                    workerId, bytes.length);
                
                return bytes;
                
            } else if (message instanceof StreamMessage) {
                LOGGER.warn("Consumer #{} - StreamMessage detected - not fully supported, converting to string", 
                    workerId);
                String text = message.toString();
                return text.getBytes(StandardCharsets.UTF_8);
                
            } else {
                LOGGER.warn("Consumer #{} - Unknown message type: {}, using toString()", 
                    workerId, message.getClass().getName());
                String text = message.toString();
                return text.getBytes(StandardCharsets.UTF_8);
            }
        }

        /**
         * Logs AMQP properties from the message facade for debugging purposes.
         */
        private void logAmqpProperties(Message message) {
            try {
                JmsMessage jmsMessage = (JmsMessage) message;
                AmqpJmsMessageFacade facade = (AmqpJmsMessageFacade) jmsMessage.getFacade();
                
                LOGGER.debug("Consumer #{} - ==========================================", workerId);
                LOGGER.debug("Consumer #{} - AMQP MESSAGE PROPERTIES", workerId);
                LOGGER.debug("Consumer #{} - ==========================================", workerId);
                
                // Log AMQP standard properties
                LOGGER.debug("Consumer #{} - --- AMQP Standard Properties ---", workerId);
                logProperty("Content-Type", facade.getContentType());
                logProperty("Message-ID", facade.getMessageId());
                logProperty("Correlation-ID", facade.getCorrelationId());
                logProperty("User-ID", facade.getUserId());
                logProperty("Group-ID", facade.getGroupId());
                logProperty("Group-Sequence", facade.getGroupSequence());
                logProperty("Reply-To-Group-ID", facade.getReplyToGroupId());
                
                // Log all application properties
                LOGGER.debug("Consumer #{} - --- Application Properties ---", workerId);
                Set<String> propertyNames = new HashSet<>();
                facade.getApplicationPropertyNames(propertyNames);
                
                if (propertyNames.isEmpty()) {
                    LOGGER.debug("Consumer #{} - No application properties found", workerId);
                } else {
                    LOGGER.debug("Consumer #{} - Found {} application properties:", workerId, propertyNames.size());
                    for (String name : propertyNames) {
                        Object value = facade.getApplicationProperty(name);
                        String valueType = value != null ? value.getClass().getSimpleName() : "null";
                        LOGGER.info("Consumer #{} -   {} = {} ({})", workerId, name, value, valueType);
                    }
                }
                
                LOGGER.debug("Consumer #{} - ==========================================", workerId);
                
            } catch (ClassCastException e) {
                LOGGER.warn("Consumer #{} - Message facade is not AmqpJmsMessageFacade: {}", 
                    workerId, e.getMessage());
            } catch (Exception e) {
                LOGGER.warn("Consumer #{} - Error accessing AMQP message properties: {}", 
                    workerId, e.getMessage(), e);
            }
        }

        /**
         * Helper method to log a property with null-safe handling
         */
        private void logProperty(String name, Object value) {
            if (value != null) {
                LOGGER.debug("Consumer #{} - {}: {}", workerId, name, value);
            } else {
                LOGGER.debug("Consumer #{} - {}: <not set>", workerId, name);
            }
        }

        /**
         * Parse payload based on content type
         */
        private Object parsePayload(byte[] payloadBytes, org.mule.runtime.api.metadata.MediaType mediaType) {
            try {
                String mimeType = mediaType.toRfcString().toLowerCase();
                LOGGER.debug("Consumer #{} - Parsing payload with MIME type: {}", workerId, mimeType);
                
                // Handle JSON content types - return as String for Mule compatibility
                if (mimeType.contains("application/json") || mimeType.contains("+json")) {
                    String jsonString = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning JSON payload as String (size: {} bytes)", 
                        workerId, payloadBytes.length);
                    LOGGER.debug("Consumer #{} - Successfully prepared JSON payload", workerId);
                    return jsonString;
                }
                
                // Handle XML content types
                if (mimeType.contains("application/xml") || 
                    mimeType.contains("text/xml") || 
                    mimeType.contains("+xml")) {
                    String xmlContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning XML payload as String (size: {} bytes)", 
                        workerId, payloadBytes.length);
                    return xmlContent;
                }
                
                // Handle CSV content types
                if (mimeType.contains("text/csv") || 
                    mimeType.contains("application/csv")) {
                    String csvContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning CSV payload as String (size: {} bytes)", 
                        workerId, payloadBytes.length);
                    return csvContent;
                }
                
                // Handle plain text content types
                if (mimeType.contains("text/plain")) {
                    String textContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning plain text payload as String (size: {} bytes)", 
                        workerId, payloadBytes.length);
                    return textContent;
                }
                
                // Handle other text-based content types
                if (mimeType.startsWith("text/")) {
                    String textContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning text/* payload as String (size: {} bytes)", 
                        workerId, payloadBytes.length);
                    return textContent;
                }
                
                // Handle HTML content types
                if (mimeType.contains("text/html") || 
                    mimeType.contains("application/xhtml")) {
                    String htmlContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning HTML payload as String (size: {} bytes)", 
                        workerId, payloadBytes.length);
                    return htmlContent;
                }
                
                // Handle YAML content types
                if (mimeType.contains("application/yaml") || 
                    mimeType.contains("application/x-yaml") ||
                    mimeType.contains("text/yaml")) {
                    String yamlContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning YAML payload as String (size: {} bytes)", 
                        workerId, payloadBytes.length);
                    return yamlContent;
                }
                
                // For binary or unknown types, return as InputStream
                LOGGER.debug("Consumer #{} - Returning payload as InputStream for binary/unknown content-type: {} (size: {} bytes)", 
                    workerId, mimeType, payloadBytes.length);
                return new java.io.ByteArrayInputStream(payloadBytes);
                
            } catch (Exception e) {
                LOGGER.warn("Consumer #{} - Failed to parse payload ({}), returning as InputStream: {}", 
                    workerId, mediaType.toRfcString(), e.getMessage());
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
            
            // Extract AMQP properties and add to attributes
            try {
                JmsMessage jmsMessage = (JmsMessage) message;
                AmqpJmsMessageFacade facade = (AmqpJmsMessageFacade) jmsMessage.getFacade();
                
                LOGGER.debug("Consumer #{} - Extracting AMQP properties into MessageAttributes", workerId);
                
                // AMQP Standard Properties
                if (facade.getContentType() != null) {
                    String contentTypeStr = facade.getContentType().toString();
                    attributes.setContentType(contentTypeStr);
                    LOGGER.debug("Consumer #{} - Set contentType: {}", workerId, contentTypeStr);
                }
                
                if (facade.getUserId() != null) {
                    String userIdStr = facade.getUserId().toString();
                    attributes.setUserId(userIdStr);
                    LOGGER.debug("Consumer #{} - Set userId: {}", workerId, userIdStr);
                }
                
                if (facade.getGroupId() != null) {
                    attributes.setGroupId(facade.getGroupId());
                    LOGGER.debug("Consumer #{} - Set groupId: {}", workerId, facade.getGroupId());
                }
                
                attributes.setGroupSequence(facade.getGroupSequence());
                LOGGER.debug("Consumer #{} - Set groupSequence: {}", workerId, facade.getGroupSequence());
                
                if (facade.getReplyToGroupId() != null) {
                    attributes.setReplyToGroupId(facade.getReplyToGroupId());
                    LOGGER.debug("Consumer #{} - Set replyToGroupId: {}", workerId, facade.getReplyToGroupId());
                }
                
                // Extract AMQP Application Properties into custom properties
                Map<String, Object> customProperties = new HashMap<>();
                Set<String> propertyNames = new HashSet<>();
                facade.getApplicationPropertyNames(propertyNames);
                
                if (!propertyNames.isEmpty()) {
                    LOGGER.debug("Consumer #{} - Extracting {} AMQP application properties", 
                        workerId, propertyNames.size());
                    for (String name : propertyNames) {
                        Object value = facade.getApplicationProperty(name);
                        customProperties.put(name, value);
                        LOGGER.debug("Consumer #{} - Added AMQP application property: {} = {}", 
                            workerId, name, value);
                    }
                }
                
                // Also add standard JMS custom properties (non-internal)
                java.util.Enumeration<?> jmsPropertyNames = message.getPropertyNames();
                while (jmsPropertyNames.hasMoreElements()) {
                    String propertyName = (String) jmsPropertyNames.nextElement();
                    
                    // Skip internal properties that are already handled
                    if (JMS_AMQP_CONTENT_TYPE.equals(propertyName) ||
                        "JMSXContentType".equals(propertyName)) {
                        continue;
                    }
                    
                    // Only add if not already present from AMQP application properties
                    if (!customProperties.containsKey(propertyName)) {
                        Object propertyValue = message.getObjectProperty(propertyName);
                        customProperties.put(propertyName, propertyValue);
                        LOGGER.debug("Consumer #{} - Added JMS property: {} = {}", 
                            workerId, propertyName, propertyValue);
                    }
                }
                
                attributes.setCustomProperties(customProperties);
                LOGGER.debug("Consumer #{} - Extracted {} total custom properties", 
                    workerId, customProperties.size());
                
            } catch (ClassCastException e) {
                LOGGER.warn("Consumer #{} - Message facade is not AmqpJmsMessageFacade, falling back to standard JMS properties: {}", 
                    workerId, e.getMessage());
                
                // Fallback: Extract standard JMS properties only
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
                
            } catch (Exception e) {
                LOGGER.error("Consumer #{} - Error extracting AMQP properties: {}", 
                    workerId, e.getMessage(), e);
                
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
     * Get JMS acknowledgment mode from enum parameter
     */
    private int getAcknowledgmentMode() {
        if (ackMode == null) {
            return Session.AUTO_ACKNOWLEDGE;
        }

        switch (ackMode) {
            case CLIENT:
                LOGGER.info("Using CLIENT_ACKNOWLEDGE mode");
                return Session.CLIENT_ACKNOWLEDGE;
            case DUPS_OK:
                LOGGER.info("Using DUPS_OK_ACKNOWLEDGE mode");
                return Session.DUPS_OK_ACKNOWLEDGE;
            case AUTO:
            default:
                LOGGER.info("Using AUTO_ACKNOWLEDGE mode");
                return Session.AUTO_ACKNOWLEDGE;
        }
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
            
            // URL encode the Bearer token
            String encodedToken = URLEncoder.encode("Bearer " + accessToken, "UTF-8");
            
            // Build connection URL with Authorization header
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
     * Fetch OAuth2 access token from SAP Event Mesh
     */
    private String fetchNewAccessToken() throws ConnectionException {
        LOGGER.info("Fetching OAuth2 token for message consumption...");
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(tokenUrl);

        try {
            // Set Basic Authentication header
            String auth = clientId + ":" + clientSecret;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            httpPost.setHeader("Authorization", "Basic " + encodedAuth);
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");

            // Set grant_type parameter
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("grant_type", "client_credentials"));
            httpPost.setEntity(new UrlEncodedFormEntity(params));

            LOGGER.debug("Token request to: {}", tokenUrl);
            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode >= 200 && statusCode < 300) {
                String responseBody = EntityUtils.toString(entity);
                JsonNode jsonNode = objectMapper.readTree(responseBody);
                
                if (jsonNode.has("access_token")) {
                    String token = jsonNode.get("access_token").asText();
                    LOGGER.info("Successfully fetched OAuth2 token for consumption");
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