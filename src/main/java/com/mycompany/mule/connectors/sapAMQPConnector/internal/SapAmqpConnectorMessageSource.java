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
 * MUST BE PUBLIC for Mule SDK to access
 */
enum AcknowledgmentMode {
    AUTO,
    CLIENT
}

/**
 * Message Source for consuming messages from SAP Event Mesh queues
 * Supports both AUTO and CLIENT acknowledgment modes
 */
@Alias("listener")
@DisplayName("Message Listener")
@Summary("Listens for messages from SAP Event Mesh queue")
@MediaType(value = ANY, strict = false)
public class SapAmqpConnectorMessageSource extends Source<Object, MessageAttributes> {

    private final Logger LOGGER = LoggerFactory.getLogger(SapAmqpConnectorMessageSource.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(false);
    
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
    @Summary("AUTO: messages auto-acknowledged, CLIENT: manual acknowledgment required")
    private AcknowledgmentMode ackMode;

//    @Parameter
//    @Optional
//    @DisplayName("Message Selector")
//    @Summary("JMS message selector for filtering messages (optional)")
//    @Example("JMSPriority > 5")
//    @Expression(ExpressionSupport.SUPPORTED)
//    private String messageSelector;

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
            LOGGER.warn("Messages MUST be explicitly acknowledged using 'Acknowledge Message'");
            LOGGER.warn("Unacknowledged messages will be redelivered by broker after timeout");
            LOGGER.warn("Multiple unacknowledged messages are supported (session-per-message)");
            LOGGER.warn("===================================================================");
        }
        
        running.set(true);

        try {
            validateParameters();
            
            SapAmqpConnectorConnection muleConnection = connectionProvider.connect();
            
            String accessToken = fetchNewAccessToken();
            if (accessToken == null || accessToken.trim().isEmpty()) {
                LOGGER.error("Failed to obtain access token");
                return;
            }
            muleConnection.setAccessToken(accessToken, 3600);
            LOGGER.info("OAuth2 token obtained");

            String connectionUrl = buildConnectionUrl(config.getUri(), accessToken);
            LOGGER.info("Connection URL built");

            JmsConnectionFactory factory = new JmsConnectionFactory();
            factory.setRemoteURI(connectionUrl);
            factory.setUsername(clientId);
            factory.setPassword(accessToken);

            jmsConnection = factory.createConnection();
            jmsConnection.start();
            muleConnection.setJmsConnection(jmsConnection);
            LOGGER.info("JMS Connection established");

            int sessionAckMode = getAcknowledgmentMode();

            for (int i = 0; i < numberOfConsumers; i++) {
                ConsumerWorker worker = new ConsumerWorker(
                    jmsConnection, 
                    queueName, 
                    sourceCallback, 
                    sessionAckMode, //messageSelector,
                    i + 1
                );
                consumers.add(worker);
                new Thread(worker, "SAP-AMQP-Consumer-" + (i + 1)).start();
                LOGGER.info("Started consumer thread #{}", i + 1);
            }

            LOGGER.info("Message Listener started with {} consumer(s)", numberOfConsumers);

        } catch (Exception e) {
            LOGGER.error("Failed to start message listener", e);
            onStop();
        }
    }

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

        for (ConsumerWorker worker : consumers) {
            worker.stop();
        }
        consumers.clear();

        if (jmsConnection != null) {
            try {
                jmsConnection.close();
                LOGGER.info("JMS Connection closed");
            } catch (JMSException e) {
                LOGGER.error("Error closing JMS connection", e);
            }
            jmsConnection = null;
        }
        
        if (ackMode == AcknowledgmentMode.CLIENT) {
            SapAmqpConnectorOperations.AcknowledgmentRegistry.getInstance().clearAll();
            LOGGER.debug("Cleared all pending acknowledgments");
        }

        LOGGER.info("Message Listener stopped successfully");
    }

    /**
     * Worker class that consumes messages in a separate thread
     * Supports both AUTO and CLIENT acknowledgment modes
     */
    private class ConsumerWorker implements Runnable {
        private final jakarta.jms.Connection jmsConn;
        private final String queueName;
        private final SourceCallback<Object, MessageAttributes> callback;
        private final int sessionAckMode;
       // private final String selector;
        private final int workerId;
        private volatile boolean active = true;

        public ConsumerWorker(jakarta.jms.Connection jmsConn, String queueName, 
                            SourceCallback<Object, MessageAttributes> callback,
                            int sessionAckMode, int workerId) {
            this.jmsConn = jmsConn;
            this.queueName = queueName;
            this.callback = callback;
            this.sessionAckMode = sessionAckMode;
            //this.selector = selector;
            this.workerId = workerId;
        }

        @Override
        public void run() {
            LOGGER.debug("Consumer #{} initializing...", workerId);
            
            boolean isClientMode = (sessionAckMode == Session.CLIENT_ACKNOWLEDGE);
            
            if (isClientMode) {
                LOGGER.info("Consumer #{} - Running in CLIENT mode (session-per-message)", workerId);
                runClientMode();
            } else {
                LOGGER.info("Consumer #{} - Running in AUTO mode (shared session)", workerId);
                runAutoMode();
            }
        }

        /**
         * AUTO mode: Single shared session for all messages
         * Messages are automatically acknowledged
         */
        private void runAutoMode() {
            Session session = null;
            MessageConsumer consumer = null;

            try {
                LOGGER.debug("Consumer #{} - AUTO mode initializing...", workerId);
                
                session = jmsConn.createSession(false, sessionAckMode);
                Destination queue = session.createQueue(queueName);
                
//                if (selector != null && !selector.trim().isEmpty()) {
//                    consumer = session.createConsumer(queue, selector);
//                    LOGGER.info("Consumer #{} - Created with selector: {}", workerId, selector);
//                } else {
//                    consumer = session.createConsumer(queue);
//                    LOGGER.info("Consumer #{} - Created without selector", workerId);
//                }

                LOGGER.info("Consumer #{} ready and listening (AUTO mode)...", workerId);

                while (running.get() && active) {
                    try {
                        Message message = consumer.receive(1000);
                        
                        if (message != null) {
                            processMessageAutoMode(message);
                        }
                    } catch (JMSException e) {
                        if (running.get() && active) {
                            LOGGER.error("Consumer #{} - Error receiving message", workerId, e);
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException ie) {
                                LOGGER.info("Consumer #{} interrupted during sleep", workerId);
                                break;
                            }
                        }
                    }
                }

            } catch (Exception e) {
                LOGGER.error("Consumer #{} - Fatal error in AUTO mode", workerId, e);
            } finally {
                try {
                    if (consumer != null) consumer.close();
                    if (session != null) session.close();
                    LOGGER.debug("Consumer #{} - Resources cleaned up (AUTO mode)", workerId);
                } catch (JMSException e) {
                    LOGGER.error("Consumer #{} - Error closing resources", workerId, e);
                }
            }
        }

        /**
         * CLIENT mode: Session-per-message pattern
         * Each message gets its own dedicated session
         * This allows multiple unacknowledged messages to coexist
         */
        private void runClientMode() {
            LOGGER.info("Consumer #{} ready and listening (CLIENT mode)...", workerId);
            
            while (running.get() && active) {
                Session dedicatedSession = null;
                MessageConsumer dedicatedConsumer = null;
                
                try {
                    // CRITICAL: Create NEW session for THIS message
                    // This enables multiple unacknowledged messages
                    dedicatedSession = jmsConn.createSession(false, sessionAckMode);
                    Destination queue = dedicatedSession.createQueue(queueName);
                    
//                    if (selector != null && !selector.trim().isEmpty()) {
//                        dedicatedConsumer = dedicatedSession.createConsumer(queue, selector);
//                    } else {
//                        dedicatedConsumer = dedicatedSession.createConsumer(queue);
//                    }
//                    
                    // Receive message with timeout
                    Message message = dedicatedConsumer.receive(1000);
                    
                    if (message != null) {
                        // Close consumer immediately (but keep session open)
                        if (dedicatedConsumer != null) {
                            dedicatedConsumer.close();
                            dedicatedConsumer = null;
                        }
                        
                        // Process and register for acknowledgment
                        // Session ownership transfers to AcknowledgmentRegistry
                        processMessageClientMode(message, dedicatedSession);
                        
                        // CRITICAL: DO NOT close session here
                        // It's now owned by AcknowledgmentRegistry
                        dedicatedSession = null;
                    } else {
                        // No message received, close session and try again
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
                            LOGGER.debug("Consumer #{} - Error during cleanup: {}", 
                                workerId, cleanupEx.getMessage());
                        }
                        
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            LOGGER.info("Consumer #{} interrupted", workerId);
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
         * Process message in AUTO mode
         * Message is automatically acknowledged
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
                        LOGGER.warn("Consumer #{} - Invalid content-type, using ANY", workerId);
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
         * Registers message with AcknowledgmentRegistry for manual acknowledgment using JMS Message ID
         * Session is kept open until message is acknowledged
         */
        /**
         * Process message in CLIENT mode
         * Registers message with AcknowledgmentRegistry for manual acknowledgment using JMS Correlation ID
         * Session is kept open until message is acknowledged
         */
        private void processMessageClientMode(Message message, Session dedicatedSession) throws InterruptedException {
            String ackId = null;
            try {
                LOGGER.debug("[Consumer-{}] Message received (CLIENT mode)", workerId);
                
                byte[] payloadBytes = extractPayloadAsBytes(message);
                MessageAttributes attributes = extractMessageAttributes(message);
                
                // Register message for acknowledgment with its dedicated session using JMS Correlation ID
                try {
                    ackId = SapAmqpConnectorOperations.AcknowledgmentRegistry
                        .getInstance().registerMessage(message, dedicatedSession);
                    attributes.setackId(ackId);
                    attributes.setRequiresAcknowledgment(true);
                    
                    LOGGER.debug("[Consumer-{}] Registered for acknowledgment with Correlation ID: {}", workerId, ackId);
                } catch (JMSException e) {
                    LOGGER.error("[Consumer-{}] Failed to register message for acknowledgment: {}", workerId, e.getMessage());
                    LOGGER.error("[Consumer-{}] Ensure the publisher sets JMSCorrelationID on the message", workerId);
                    // Close session and rethrow
                    try {
                        if (dedicatedSession != null) {
                            dedicatedSession.close();
                        }
                    } catch (Exception closeEx) {
                        LOGGER.debug("[Consumer-{}] Error closing session: {}", workerId, closeEx.getMessage());
                    }
                    throw new RuntimeException("Failed to register message for acknowledgment: " + e.getMessage() + 
                                             ". Ensure the publisher sets a correlation ID.", e);
                }
                
                String contentType = attributes.getContentType();
                org.mule.runtime.api.metadata.MediaType outputMediaType;
                
                if (contentType != null && !contentType.trim().isEmpty()) {
                    try {
                        outputMediaType = org.mule.runtime.api.metadata.MediaType.parse(contentType);
                    } catch (Exception e) {
                        LOGGER.warn("[Consumer-{}] Invalid content-type, using ANY", workerId);
                        outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
                    }
                } else {
                    outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
                }
                
                LOGGER.info("[Consumer-{}] Processing message (Correlation ID: {}), Size: {} bytes, Mode: CLIENT", 
                    workerId, ackId, payloadBytes.length);
                
                Object outputPayload = parsePayload(payloadBytes, outputMediaType);
                
                Result<Object, MessageAttributes> result = Result.<Object, MessageAttributes>builder()
                        .output(outputPayload)
                        .attributes(attributes)
                        .mediaType(outputMediaType)
                        .build();
                
                callback.handle(result);
                
                LOGGER.info("[Consumer-{}] Message delivered to flow (awaiting acknowledgment)", workerId);
                LOGGER.warn("[Consumer-{}] Message MUST be acknowledged using Correlation ID: {}", workerId, ackId);
                
            } catch (Exception e) {
                LOGGER.error("[Consumer-{}] Error processing message", workerId, e);
                
                // Clean up acknowledgment registration if processing failed
                if (ackId != null) {
                    LOGGER.warn("[Consumer-{}] Removing acknowledgment due to error", workerId);
                    SapAmqpConnectorOperations.AcknowledgmentRegistry
                        .getInstance().removePendingAcknowledgment(ackId);
                }
                
                // Close the dedicated session on error
                try {
                    if (dedicatedSession != null) {
                        dedicatedSession.close();
                        LOGGER.debug("[Consumer-{}] Closed session after error", workerId);
                    }
                } catch (Exception closeEx) {
                    LOGGER.debug("[Consumer-{}] Error closing session: {}", workerId, closeEx.getMessage());
                }
                
                LOGGER.warn("[Consumer-{}] Message NOT acknowledged - will be redelivered by broker", workerId);
            }
        }

        
        /**
         * Extract payload as byte array from JMS message
         */
        private byte[] extractPayloadAsBytes(Message message) throws Exception {
            LOGGER.debug("Consumer #{} - Message type: {}", workerId, message.getClass().getName());
            
            if (message instanceof BytesMessage) {
                BytesMessage bytesMessage = (BytesMessage) message;
                long bodyLength = bytesMessage.getBodyLength();
                
                LOGGER.debug("Consumer #{} - BytesMessage (length: {} bytes)", workerId, bodyLength);
                
                if (bodyLength > Integer.MAX_VALUE) {
                    throw new RuntimeException("Message too large: " + bodyLength + " bytes");
                }
                
                if (bodyLength == 0) {
                    LOGGER.warn("Consumer #{} - BytesMessage has zero length", workerId);
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
                
                if (obj instanceof byte[]) {
                    byte[] bytes = (byte[]) obj;
                    LOGGER.debug("Consumer #{} - ObjectMessage contains byte array ({} bytes)", 
                        workerId, bytes.length);
                    return bytes;
                } else if (obj instanceof String) {
                    String text = (String) obj;
                    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - ObjectMessage contains String ({} bytes)", 
                        workerId, bytes.length);
                    return bytes;
                } else {
                    String json = objectMapper.writeValueAsString(obj);
                    LOGGER.debug("Consumer #{} - ObjectMessage serialized to JSON", workerId);
                    return json.getBytes(StandardCharsets.UTF_8);
                }
                
            } else if (message instanceof MapMessage) {
                MapMessage mapMessage = (MapMessage) message;
                
                LOGGER.debug("Consumer #{} - MapMessage detected", workerId);
                
                Map<String, Object> map = new HashMap<>();
                Enumeration<?> mapNames = mapMessage.getMapNames();
                
                while (mapNames.hasMoreElements()) {
                    String name = (String) mapNames.nextElement();
                    map.put(name, mapMessage.getObject(name));
                }
                
                String json = objectMapper.writeValueAsString(map);
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                LOGGER.debug("Consumer #{} - Extracted {} bytes from MapMessage", workerId, bytes.length);
                
                return bytes;
                
            } else if (message instanceof StreamMessage) {
                LOGGER.warn("Consumer #{} - StreamMessage not fully supported", workerId);
                String text = message.toString();
                return text.getBytes(StandardCharsets.UTF_8);
                
            } else {
                LOGGER.warn("Consumer #{} - Unknown message type: {}", 
                    workerId, message.getClass().getName());
                String text = message.toString();
                return text.getBytes(StandardCharsets.UTF_8);
            }
        }

        /**
         * Parse payload based on content type
         */
        private Object parsePayload(byte[] payloadBytes, org.mule.runtime.api.metadata.MediaType mediaType) {
            try {
                String mimeType = mediaType.toRfcString().toLowerCase();
                LOGGER.debug("Consumer #{} - Parsing with MIME type: {}", workerId, mimeType);
                
                if (mimeType.contains("application/json") || mimeType.contains("+json")) {
                    String jsonString = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning JSON as String ({} bytes)", 
                        workerId, payloadBytes.length);
                    return jsonString;
                }
                
                if (mimeType.contains("application/xml") || 
                    mimeType.contains("text/xml") || 
                    mimeType.contains("+xml")) {
                    String xmlContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning XML as String ({} bytes)", 
                        workerId, payloadBytes.length);
                    return xmlContent;
                }
                
                if (mimeType.contains("text/csv") || 
                    mimeType.contains("application/csv")) {
                    String csvContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning CSV as String ({} bytes)", 
                        workerId, payloadBytes.length);
                    return csvContent;
                }
                
                if (mimeType.contains("text/plain")) {
                    String textContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning text as String ({} bytes)", 
                        workerId, payloadBytes.length);
                    return textContent;
                }
                
                if (mimeType.startsWith("text/")) {
                    String textContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning text/* as String ({} bytes)", 
                        workerId, payloadBytes.length);
                    return textContent;
                }
                
                if (mimeType.contains("text/html") || 
                    mimeType.contains("application/xhtml")) {
                    String htmlContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning HTML as String ({} bytes)", 
                        workerId, payloadBytes.length);
                    return htmlContent;
                }
                
                if (mimeType.contains("application/yaml") || 
                    mimeType.contains("application/x-yaml") ||
                    mimeType.contains("text/yaml")) {
                    String yamlContent = new String(payloadBytes, StandardCharsets.UTF_8);
                    LOGGER.debug("Consumer #{} - Returning YAML as String ({} bytes)", 
                        workerId, payloadBytes.length);
                    return yamlContent;
                }
                
                LOGGER.debug("Consumer #{} - Returning as InputStream ({} bytes)", 
                    workerId, payloadBytes.length);
                return new java.io.ByteArrayInputStream(payloadBytes);
                
            } catch (Exception e) {
                LOGGER.warn("Consumer #{} - Failed to parse, returning as InputStream: {}", 
                    workerId, e.getMessage());
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
            attributes.setReplyTo(message.getJMSReplyTo() != null ? 
                message.getJMSReplyTo().toString() : null);
            attributes.setDestination(message.getJMSDestination() != null ? 
                message.getJMSDestination().toString() : null);
            attributes.setDeliveryMode(message.getJMSDeliveryMode());
            attributes.setRedelivered(message.getJMSRedelivered());
            attributes.setType(message.getJMSType());
            attributes.setExpiration(message.getJMSExpiration());
            attributes.setPriority(message.getJMSPriority());
            
            // Extract AMQP properties
            try {
                JmsMessage jmsMessage = (JmsMessage) message;
                AmqpJmsMessageFacade facade = (AmqpJmsMessageFacade) jmsMessage.getFacade();
                
                LOGGER.debug("Consumer #{} - Extracting AMQP properties", workerId);
                
                if (facade.getContentType() != null) {
                    String contentTypeStr = facade.getContentType().toString();
                    attributes.setContentType(contentTypeStr);
                    LOGGER.debug("Consumer #{} - Content-Type: {}", workerId, contentTypeStr);
                }
                
                if (facade.getUserId() != null) {
                    String userIdStr = facade.getUserId().toString();
                    attributes.setUserId(userIdStr);
                    LOGGER.debug("Consumer #{} - User-ID: {}", workerId, userIdStr);
                }
                
                if (facade.getGroupId() != null) {
                    attributes.setGroupId(facade.getGroupId());
                    LOGGER.debug("Consumer #{} - Group-ID: {}", workerId, facade.getGroupId());
                }
                
                attributes.setGroupSequence(facade.getGroupSequence());
                
                if (facade.getReplyToGroupId() != null) {
                    attributes.setReplyToGroupId(facade.getReplyToGroupId());
                }
                
                // Extract application properties
                Map<String, Object> customProperties = new HashMap<>();
                Set<String> propertyNames = new HashSet<>();
                facade.getApplicationPropertyNames(propertyNames);
                
                if (!propertyNames.isEmpty()) {
                    LOGGER.debug("Consumer #{} - Found {} application properties", 
                        workerId, propertyNames.size());
                    for (String name : propertyNames) {
                        Object value = facade.getApplicationProperty(name);
                        customProperties.put(name, value);
                    }
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
                LOGGER.debug("Consumer #{} - Total {} custom properties", 
                    workerId, customProperties.size());
                
            } catch (Exception e) {
                LOGGER.debug("Consumer #{} - Using standard JMS properties only", workerId);
                
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
        if (ackMode == null) {
            return Session.AUTO_ACKNOWLEDGE;
        }

        switch (ackMode) {
            case CLIENT:
                LOGGER.info("Using CLIENT_ACKNOWLEDGE mode (standard JMS)");
                return Session.CLIENT_ACKNOWLEDGE;
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