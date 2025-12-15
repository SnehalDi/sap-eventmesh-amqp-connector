package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;
import java.nio.charset.StandardCharsets;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.NullSafe;
import org.mule.runtime.extension.api.annotation.param.Config;
import org.mule.runtime.extension.api.annotation.param.Connection;
import org.mule.runtime.extension.api.annotation.param.Content;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.runtime.extension.api.runtime.operation.Result;
import org.mule.runtime.api.metadata.TypedValue;
import org.mule.runtime.api.metadata.DataType;
import org.mule.runtime.extension.api.annotation.metadata.OutputResolver;

import jakarta.jms.*;
import org.apache.qpid.jms.message.JmsMessage; 
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.mule.runtime.api.connection.ConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.net.URI;
import java.net.URLEncoder;
import java.io.InputStream;

public class SapAmqpConnectorOperations {

    private final Logger LOGGER = LoggerFactory.getLogger(SapAmqpConnectorOperations.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // JMS property key for AMQP content-type
    private static final String JMS_AMQP_CONTENT_TYPE = "JMS_AMQP_CONTENT_TYPE";

    @DisplayName("Publish")
    @MediaType(value = ANY, strict = false)
    public void publishMessage(
            @Config SapAmqpConnectorConfiguration config,
            @Connection SapAmqpConnectorConnection connection,
            @DisplayName("Queue Name") @Summary("Name of the queue to publish to") String queueName,
            @Content @DisplayName("Message Payload") TypedValue<Object> payload,
            @Optional @DisplayName("Headers") @Summary("Custom headers to add to the message") 
            @NullSafe List<MessageHeader> headers) throws ConnectionException {

        jakarta.jms.Connection jmsConnection = null;
        Session session = null;
        MessageProducer producer = null;

        try {
            LOGGER.info("=== Starting SAP Event Mesh Publish ===");

            // Extract and validate access token
            String accessToken = extractAccessToken(headers);
            if (accessToken == null || accessToken.trim().isEmpty()) {
                throw new ConnectionException("Authorization token must be provided in headers.");
            }

            // Extract MIME type information
            DataType dataType = payload.getDataType();
            org.mule.runtime.api.metadata.MediaType mediaType = dataType.getMediaType();
            String mimeType = mediaType.toRfcString();
            
            LOGGER.info("Payload MIME type: {}", mimeType);

            // Convert payload to byte array to preserve exact content
            byte[] messageBytes = convertPayloadToBytes(payload.getValue());
            LOGGER.info("Message payload converted to bytes (length: {})", messageBytes.length);

            // Create and start JMS connection
            jmsConnection = createJmsConnection(config, accessToken, connection);

            // Create session and producer
            session = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(queueName);
            producer = session.createProducer(queue);
            LOGGER.debug("Message Producer created for queue: {}", queueName);

            // Create BytesMessage to preserve content exactly as-is
            BytesMessage msg = session.createBytesMessage();
            msg.writeBytes(messageBytes);
            
            // Set AMQP content-type property (standard AMQP 1.0 property via Qpid JMS)
            msg.setStringProperty(JMS_AMQP_CONTENT_TYPE, mimeType);
            LOGGER.debug("Set AMQP content-type property: {}", mimeType);
            
            // Add custom headers
            addCustomHeaders(msg, headers);
            
            producer.send(msg);
            
            LOGGER.info("Successfully published message to queue: {} with content-type: {}", queueName, mimeType);

        } catch (JMSException e) {
            handleJmsException(e, connection);
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
            if (e instanceof ConnectionException) {
                throw (ConnectionException) e;
            }
            throw new RuntimeException("Unexpected error: " + e.getMessage(), e);
        } finally {
            closeResources(producer, session, connection);
        }
    }

    @DisplayName("Consume")
    @MediaType(value = ANY, strict = false)
    @Summary("Synchronously consume a single message from SAP Event Mesh queue")
    @OutputResolver(output = SapAmqpOutputResolver.class)
    public Result<Object, MessageAttributes> consumeMessage(
            @Config SapAmqpConnectorConfiguration config,
            @Connection SapAmqpConnectorConnection connection,
            @DisplayName("Queue Name") @Summary("Name of the queue to consume from") String queueName,
            @Optional(defaultValue = "5000") @DisplayName("Timeout (ms)") 
            @Summary("Time to wait for a message in milliseconds (default: 5000ms)") long timeout,
            @Optional @DisplayName("Headers") @Summary("Custom headers including Authorization token") 
            @NullSafe List<MessageHeader> headers) {

        jakarta.jms.Connection jmsConnection = null;
        Session session = null;
        MessageConsumer consumer = null;

        try {
            LOGGER.info("=== Starting SAP Event Mesh Message Consumption ===");

            // Extract and validate access token
            String accessToken = extractAccessToken(headers);
            if (accessToken == null || accessToken.trim().isEmpty()) {
                return buildErrorResult("AUTHENTICATION_ERROR", "Authorization token must be provided in headers");
            }

            // Create and start JMS connection
            jmsConnection = createJmsConnection(config, accessToken, connection);

            // Create session and consumer
            session = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(queueName);
            consumer = session.createConsumer(queue);
            LOGGER.debug("Message Consumer created for queue: {}", queueName);

            // Receive message with timeout
            LOGGER.info("Waiting for message (timeout: {}ms)...", timeout);
            Message message = consumer.receive(timeout);

            if (message == null) {
                LOGGER.warn("No message received within timeout period");
                return buildNoMessageResult(timeout);
            }

            // Extract message details 
            LOGGER.info("Message received from queue");
            byte[] payloadBytes = extractPayloadAsBytes(message);
            MessageAttributes attributes = extractMessageAttributes(message);
            
            // Get content-type from attributes (extracted from AMQP properties)
            String contentType = attributes.getContentType();
            org.mule.runtime.api.metadata.MediaType outputMediaType;
            
            if (contentType != null && !contentType.trim().isEmpty()) {
                try {
                    outputMediaType = org.mule.runtime.api.metadata.MediaType.parse(contentType);
                    LOGGER.info("Using AMQP content-type: {}", contentType);
                } catch (Exception e) {
                    LOGGER.warn("Invalid content-type '{}', defaulting to ANY: {}", contentType, e.getMessage());
                    outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
                }
            } else {
                LOGGER.info("No content-type found, defaulting to ANY");
                outputMediaType = org.mule.runtime.api.metadata.MediaType.ANY;
            }
            
            LOGGER.info("Message ID: {}, Content-Type: {}, Size: {} bytes", 
                attributes.getMessageId(), contentType, payloadBytes.length);
            
            // Parse payload based on content type from AMQP properties
            Object outputPayload = parsePayload(payloadBytes, outputMediaType);
            
            return Result.<Object, MessageAttributes>builder()
                    .output(outputPayload)
                    .attributes(attributes)
                    .mediaType(outputMediaType)
                    .build();

        } catch (JMSException e) {
            return handleJmsExceptionForConsume(e, connection);
        } catch (Exception e) {
            LOGGER.error("Unexpected error", e);
            return buildErrorResult("ERROR", "Unexpected error: " + e.getMessage());
        } finally {
            closeResources(consumer, session, connection);
        }
    }

    // ========================================================================
    // ACKNOWLEDGMENT OPERATIONS (CLIENT MODE)
    // ========================================================================
    
    /**
     * Acknowledge a message in CLIENT mode
     * 
     * @param acknowledgmentId The acknowledgment ID from the message attributes
     * @param connection The connection (not actively used but required for operation binding)
     */
    @DisplayName("Acknowledge Message")
    @Summary("Acknowledges a message that was received in CLIENT acknowledgment mode")
    @MediaType(value = ANY, strict = false)
    public void acknowledgeMessage(
            @DisplayName("Acknowledgment ID")
            @Summary("The acknowledgment ID from the message attributes")
            String acknowledgmentId,
            
            @Connection
            SapAmqpConnectorConnection connection) {
        
        LOGGER.info("=== Acknowledging Message ===");
        LOGGER.info("Acknowledgment ID: {}", acknowledgmentId);
        
        if (acknowledgmentId == null || acknowledgmentId.trim().isEmpty()) {
            String errorMsg = "Acknowledgment ID is required";
            LOGGER.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        
        // Retrieve pending acknowledgment from registry
        AcknowledgmentRegistry registry = AcknowledgmentRegistry.getInstance();
        AcknowledgmentRegistry.PendingAcknowledgment pending = registry.getPendingAcknowledgment(acknowledgmentId);
        
        if (pending == null) {
            String errorMsg = "No pending acknowledgment found for ID: " + acknowledgmentId + 
                            ". The message may have already been acknowledged or the ID is invalid.";
            LOGGER.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        
        try {
            Message message = pending.getMessage();
            
            LOGGER.debug("Pending acknowledgment age: {} seconds", pending.getAgeInSeconds());
            LOGGER.debug("Message ID: {}", message.getJMSMessageID());
            
            // Acknowledge the message
            message.acknowledge();
            
            LOGGER.info("Message acknowledged successfully");
            LOGGER.info("Acknowledgment ID: {}", acknowledgmentId);
            
            // Remove from registry
            registry.removePendingAcknowledgment(acknowledgmentId);
            
        } catch (JMSException e) {
            String errorMsg = "Failed to acknowledge message with ID: " + acknowledgmentId + " - " + e.getMessage();
            LOGGER.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }
    
    /**
     * Reject (nack) a message in CLIENT mode
     * Note: With INDIVIDUAL_ACKNOWLEDGE mode, this simply doesn't acknowledge the message.
     * The message will be redelivered by SAP Event Mesh after a timeout.
     * Other messages are NOT affected.
     * 
     * @param acknowledgmentId The acknowledgment ID from the message attributes
     * @param connection The connection (not actively used but required for operation binding)
     */
    @DisplayName("Reject Message (Nack)")
    @Summary("Rejects a message in CLIENT mode, causing it to be redelivered (other messages NOT affected)")
    @MediaType(value = ANY, strict = false)
    public void rejectMessage(
            @DisplayName("Acknowledgment ID")
            @Summary("The acknowledgment ID from the message attributes")
            String acknowledgmentId,
            
            @Connection
            SapAmqpConnectorConnection connection) {
        
        LOGGER.info("=== Rejecting Message (Nack) ===");
        LOGGER.info("Acknowledgment ID: {}", acknowledgmentId);
        
        if (acknowledgmentId == null || acknowledgmentId.trim().isEmpty()) {
            String errorMsg = "Acknowledgment ID is required";
            LOGGER.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        
        // Retrieve pending acknowledgment from registry
        AcknowledgmentRegistry registry = AcknowledgmentRegistry.getInstance();
        AcknowledgmentRegistry.PendingAcknowledgment pending = registry.getPendingAcknowledgment(acknowledgmentId);
        
        if (pending == null) {
            String errorMsg = "No pending acknowledgment found for ID: " + acknowledgmentId + 
                            ". The message may have already been processed or the ID is invalid.";
            LOGGER.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        
        try {
            Message message = pending.getMessage();
            
            LOGGER.debug("Pending acknowledgment age: {} seconds", pending.getAgeInSeconds());
            LOGGER.debug("Message ID: {}", message.getJMSMessageID());
            
            // With INDIVIDUAL_ACKNOWLEDGE mode:
            // - We simply do NOT acknowledge the message
            // - No session.recover() needed (would affect other messages)
            // - SAP Event Mesh will redeliver this specific message after timeout
            // - Other messages in the session are NOT affected
            
            LOGGER.info("Message NOT acknowledged (rejected)");
            LOGGER.info("Message will be redelivered by SAP Event Mesh after timeout");
            LOGGER.info("Other messages are NOT affected (INDIVIDUAL_ACKNOWLEDGE mode)");
            LOGGER.info("Acknowledgment ID: {}", acknowledgmentId);
            
            // Remove from registry (we're done with it)
            registry.removePendingAcknowledgment(acknowledgmentId);
            
        } catch (JMSException e) {
            String errorMsg = "Failed to reject message with ID: " + acknowledgmentId + " - " + e.getMessage();
            LOGGER.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }
    
    /**
     * Get the count of pending acknowledgments (useful for monitoring)
     * 
     * @param connection The connection (not actively used but required for operation binding)
     * @return The number of messages waiting for acknowledgment
     */
    @DisplayName("Get Pending Acknowledgment Count")
    @Summary("Returns the number of messages waiting for acknowledgment in CLIENT mode")
    @MediaType(value = ANY, strict = false)
    public int getPendingAcknowledgmentCount(@Connection SapAmqpConnectorConnection connection) {
        int count = AcknowledgmentRegistry.getInstance().getPendingCount();
        LOGGER.debug("Pending acknowledgments: {}", count);
        return count;
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    private String extractAccessToken(List<MessageHeader> headers) {
        if (headers == null) return null;
        
        for (MessageHeader header : headers) {
            if (header.getKey() != null && "Authorization".equalsIgnoreCase(header.getKey().trim())) {
                return header.getValue();
            }
        }
        return null;
    }

    private jakarta.jms.Connection createJmsConnection(
            SapAmqpConnectorConfiguration config, 
            String accessToken,
            SapAmqpConnectorConnection connection) throws Exception {
        
        LOGGER.debug("Creating JMS connection...");
        
        String connectionUrl = buildConnectionUrl(config.getUri(), accessToken);
        
        JmsConnectionFactory factory = new JmsConnectionFactory();
        factory.setRemoteURI(connectionUrl);
        factory.setUsername(config.getClientId());
        factory.setPassword(accessToken);

        jakarta.jms.Connection jmsConnection = factory.createConnection();
        connection.setJmsConnection(jmsConnection);
        jmsConnection.start();
        
        LOGGER.info("JMS Connection started successfully");
        return jmsConnection;
    }

    private String buildConnectionUrl(String wsUri, String accessToken) throws Exception {
        try {
            URI uri = new URI(wsUri);
            String protocol = uri.getScheme();
            String host = uri.getHost();
            String path = uri.getPath();
            int port = uri.getPort();
            
            // Determine protocol and port
            String amqpProtocol;
            int targetPort;
            
            if ("wss".equalsIgnoreCase(protocol) || "amqpwss".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpwss";
                targetPort = (port > 0) ? port : 443;
            } else if ("ws".equalsIgnoreCase(protocol) || "amqpws".equalsIgnoreCase(protocol)) {
                amqpProtocol = "amqpws";
                targetPort = (port > 0) ? port : 80;
            } else {
                throw new ConnectionException("Unsupported protocol: " + protocol);
            }
            
            String encodedToken = URLEncoder.encode(accessToken, StandardCharsets.UTF_8);
            
            return String.format(
                "%s://%s:%d%s?transport.tcpNoDelay=true&transport.ws.httpHeader.Authorization=%s",
                amqpProtocol, host, targetPort, path, encodedToken
            );
            
        } catch (Exception e) {
            LOGGER.error("Error parsing URI: {}", wsUri, e);
            throw new ConnectionException("Invalid URI format: " + wsUri, e);
        }
    }

    private void addCustomHeaders(Message msg, List<MessageHeader> headers) throws JMSException {
        if (headers == null || headers.isEmpty()) return;
        
        int headerCount = 0;
        for (MessageHeader header : headers) {
            // Skip authorization header
            if ("Authorization".equalsIgnoreCase(header.getKey())) {
                continue;
            }
            
            String headerName = header.getKey();
            String headerValue = header.getValue();
            
            if (headerName != null && !headerName.trim().isEmpty()) {
                try {
                    msg.setStringProperty(headerName.trim(), headerValue);
                    headerCount++;
                    LOGGER.debug("Added header: {} = {}", headerName, headerValue);
                } catch (JMSException e) {
                    LOGGER.warn("Failed to set header '{}': {}", headerName, e.getMessage());
                }
            }
        }
        
        if (headerCount > 0) {
            LOGGER.debug("Added {} custom headers to message", headerCount);
        }
    }

    private byte[] extractPayloadAsBytes(Message message) throws Exception {
        LOGGER.debug("Message type: {}", message.getClass().getName());
        
        // Log AMQP properties
        logAmqpProperties(message);
        
        // Extract payload based on JMS message type
        if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            long bodyLength = bytesMessage.getBodyLength();
            
            LOGGER.debug("=== BytesMessage Detected ===");
            LOGGER.debug("Body length: {} bytes", bodyLength);
            
            if (bodyLength > Integer.MAX_VALUE) {
                throw new RuntimeException("Message too large: " + bodyLength + " bytes");
            }
            
            if (bodyLength == 0) {
                LOGGER.warn("BytesMessage has zero length body");
                return new byte[0];
            }
            
            byte[] bytes = new byte[(int) bodyLength];
            int bytesRead = bytesMessage.readBytes(bytes);
            
            LOGGER.debug("Extracted {} bytes from BytesMessage", bytesRead);
            return bytes;
            
        } else if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            String text = textMessage.getText();
            
            LOGGER.debug("=== TextMessage Detected ===");
            
            if (text == null) {
                LOGGER.warn("TextMessage has null content");
                return new byte[0];
            }
            
            // Log preview of text content
            String preview = text.length() > 100 ? text.substring(0, 100) + "..." : text;
            LOGGER.debug("Text content preview: {}", preview);
            LOGGER.debug("Text content length: {} characters", text.length());
            
            // Convert text to bytes using UTF-8
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            LOGGER.debug("Extracted {} bytes from TextMessage", bytes.length);
            
            return bytes;
            
        } else if (message instanceof ObjectMessage) {
            ObjectMessage objMessage = (ObjectMessage) message;
            Object obj = objMessage.getObject();
            
            LOGGER.debug("ObjectMessage detected");
            
            if (obj == null) {
                LOGGER.warn("ObjectMessage has null content");
                return new byte[0];
            }
            
            // Handle different object types
            if (obj instanceof byte[]) {
                byte[] bytes = (byte[]) obj;
                LOGGER.debug("ObjectMessage contains byte array of {} bytes", bytes.length);
                return bytes;
            } else if (obj instanceof String) {
                String text = (String) obj;
                byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
                LOGGER.debug("ObjectMessage contains String of {} characters ({} bytes)", 
                    text.length(), bytes.length);
                return bytes;
            } else {
                // Serialize to JSON
                LOGGER.debug("ObjectMessage contains {} object, serializing to JSON", 
                    obj.getClass().getSimpleName());
                String json = objectMapper.writeValueAsString(obj);
                return json.getBytes(StandardCharsets.UTF_8);
            }
            
        } else if (message instanceof MapMessage) {
            MapMessage mapMessage = (MapMessage) message;
            
            LOGGER.debug("MapMessage detected, converting to JSON");
            
            // Convert MapMessage to JSON
            Map<String, Object> map = new HashMap<>();
            Enumeration<?> mapNames = mapMessage.getMapNames();
            
            while (mapNames.hasMoreElements()) {
                String name = (String) mapNames.nextElement();
                map.put(name, mapMessage.getObject(name));
            }
            
            String json = objectMapper.writeValueAsString(map);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            LOGGER.debug("Extracted {} bytes from MapMessage (converted to JSON)", bytes.length);
            
            return bytes;
            
        } else if (message instanceof StreamMessage) {
            LOGGER.warn("StreamMessage detected - not fully supported, converting to string");
            String text = message.toString();
            return text.getBytes(StandardCharsets.UTF_8);
            
        } else {
            LOGGER.warn("Unknown message type: {}, using toString()", message.getClass().getName());
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
            
            LOGGER.debug("==========================================");
            LOGGER.debug("AMQP MESSAGE PROPERTIES");
            LOGGER.debug("==========================================");
            
            // Log AMQP standard properties
            LOGGER.debug("--- AMQP Standard Properties ---");
            logProperty("Content-Type", facade.getContentType());
            logProperty("Message-ID", facade.getMessageId());
            logProperty("Correlation-ID", facade.getCorrelationId());
            logProperty("User-ID", facade.getUserId());
            logProperty("Group-ID", facade.getGroupId());
            logProperty("Group-Sequence", facade.getGroupSequence());
            logProperty("Reply-To-Group-ID", facade.getReplyToGroupId());
            
            // Log all application properties
            LOGGER.debug("--- Application Properties ---");
            Set<String> propertyNames = new HashSet<>();
            facade.getApplicationPropertyNames(propertyNames);
            
            if (propertyNames.isEmpty()) {
                LOGGER.debug("No application properties found");
            } else {
                LOGGER.debug("Found {} application properties:", propertyNames.size());
                for (String name : propertyNames) {
                    Object value = facade.getApplicationProperty(name);
                    String valueType = value != null ? value.getClass().getSimpleName() : "null";
                    LOGGER.debug("  {} = {} ({})", name, value, valueType);
                }
            }
            
            LOGGER.debug("==========================================");
            
        } catch (ClassCastException e) {
            LOGGER.warn("Message facade is not AmqpJmsMessageFacade: {}", e.getMessage());
        } catch (Exception e) {
            LOGGER.warn("Error accessing AMQP message properties: {}", e.getMessage(), e);
        }
    }

    /**
     * Helper method to log a property with null-safe handling
     */
    private void logProperty(String name, Object value) {
        if (value != null) {
            LOGGER.debug("{}: {}", name, value);
        } else {
            LOGGER.debug("{}: <not set>", name);
        }
    }

    private Object parsePayload(byte[] payloadBytes, org.mule.runtime.api.metadata.MediaType mediaType) {
        try {
            String mimeType = mediaType.toRfcString().toLowerCase();
            LOGGER.debug("Parsing payload with MIME type: {}", mimeType);
            
            // Handle JSON content types - return as String for Mule compatibility
            if (mimeType.contains("application/json") || mimeType.contains("+json")) {
                String jsonString = new String(payloadBytes, StandardCharsets.UTF_8);
                LOGGER.debug("Returning JSON payload as String (size: {} bytes)", payloadBytes.length);
                LOGGER.info("Successfully prepared JSON payload");
                return jsonString;  // Return as String, not parsed object
            }
            
            // Handle XML content types
            if (mimeType.contains("application/xml") || 
                mimeType.contains("text/xml") || 
                mimeType.contains("+xml")) {
                String xmlContent = new String(payloadBytes, StandardCharsets.UTF_8);
                LOGGER.debug("Returning XML payload as String (size: {} bytes)", payloadBytes.length);
                return xmlContent;
            }
            
            // Handle CSV content types
            if (mimeType.contains("text/csv") || 
                mimeType.contains("application/csv")) {
                String csvContent = new String(payloadBytes, StandardCharsets.UTF_8);
                LOGGER.debug("Returning CSV payload as String (size: {} bytes)", payloadBytes.length);
                return csvContent;
            }
            
            // Handle plain text content types
            if (mimeType.contains("text/plain")) {
                String textContent = new String(payloadBytes, StandardCharsets.UTF_8);
                LOGGER.debug("Returning plain text payload as String (size: {} bytes)", payloadBytes.length);
                return textContent;
            }
            
            // Handle other text-based content types
            if (mimeType.startsWith("text/")) {
                String textContent = new String(payloadBytes, StandardCharsets.UTF_8);
                LOGGER.debug("Returning text/* payload as String (size: {} bytes)", payloadBytes.length);
                return textContent;
            }
            
            // Handle HTML content types
            if (mimeType.contains("text/html") || 
                mimeType.contains("application/xhtml")) {
                String htmlContent = new String(payloadBytes, StandardCharsets.UTF_8);
                LOGGER.debug("Returning HTML payload as String (size: {} bytes)", payloadBytes.length);
                return htmlContent;
            }
            
            // Handle YAML content types
            if (mimeType.contains("application/yaml") || 
                mimeType.contains("application/x-yaml") ||
                mimeType.contains("text/yaml")) {
                String yamlContent = new String(payloadBytes, StandardCharsets.UTF_8);
                LOGGER.debug("Returning YAML payload as String (size: {} bytes)", payloadBytes.length);
                return yamlContent;
            }
            
            // For binary or unknown types, return as InputStream
            LOGGER.debug("Returning payload as InputStream for binary/unknown content-type: {} (size: {} bytes)", 
                mimeType, payloadBytes.length);
            return new java.io.ByteArrayInputStream(payloadBytes);
            
        } catch (Exception e) {
            LOGGER.warn("Failed to parse payload ({}), returning as InputStream: {}", 
                mediaType.toRfcString(), e.getMessage());
            return new java.io.ByteArrayInputStream(payloadBytes);
        }
    }
    
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
            
            LOGGER.debug("Extracting AMQP properties into MessageAttributes");
            
            // AMQP Standard Properties
            if (facade.getContentType() != null) {
                String contentTypeStr = facade.getContentType().toString();
                attributes.setContentType(contentTypeStr);
                LOGGER.debug("Set contentType: {}", contentTypeStr);
            }
            
            if (facade.getUserId() != null) {
                String userIdStr = facade.getUserId().toString();
                attributes.setUserId(userIdStr);
                LOGGER.debug("Set userId: {}", userIdStr);
            }
            
            if (facade.getGroupId() != null) {
                attributes.setGroupId(facade.getGroupId());
                LOGGER.debug("Set groupId: {}", facade.getGroupId());
            }
            
            attributes.setGroupSequence(facade.getGroupSequence());
            LOGGER.debug("Set groupSequence: {}", facade.getGroupSequence());
            
            if (facade.getReplyToGroupId() != null) {
                attributes.setReplyToGroupId(facade.getReplyToGroupId());
                LOGGER.debug("Set replyToGroupId: {}", facade.getReplyToGroupId());
            }
            
            // Extract AMQP Application Properties into custom properties
            Map<String, Object> customProperties = new HashMap<>();
            Set<String> propertyNames = new HashSet<>();
            facade.getApplicationPropertyNames(propertyNames);
            
            if (!propertyNames.isEmpty()) {
                LOGGER.debug("Extracting {} AMQP application properties", propertyNames.size());
                for (String name : propertyNames) {
                    Object value = facade.getApplicationProperty(name);
                    customProperties.put(name, value);
                    LOGGER.debug("Added AMQP application property: {} = {}", name, value);
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
                    LOGGER.debug("Added JMS property: {} = {}", propertyName, propertyValue);
                }
            }
            
            attributes.setCustomProperties(customProperties);
            LOGGER.debug("Extracted {} total custom properties", customProperties.size());
            
        } catch (ClassCastException e) {
            LOGGER.warn("Message facade is not AmqpJmsMessageFacade, falling back to standard JMS properties: {}", e.getMessage());
            
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
            LOGGER.error("Error extracting AMQP properties: {}", e.getMessage(), e);
            
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
    
    private byte[] convertPayloadToBytes(Object payload) throws Exception {
        if (payload == null) {
            return new byte[0];
        }
        
        if (payload instanceof String) {
            // String content - preserve as-is without any transformation
            return ((String) payload).getBytes(StandardCharsets.UTF_8);
        }
        
        if (payload instanceof byte[]) {
            return (byte[]) payload;
        }
        
        if (payload instanceof InputStream) {
            try (InputStream is = (InputStream) payload) {
                return readAllBytes(is);
            }
        }
        
        // For other objects, try to serialize to JSON
        try {
            String jsonString = objectMapper.writeValueAsString(payload);
            return jsonString.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOGGER.warn("Failed to serialize payload to JSON, using toString(): {}", e.getMessage());
            return payload.toString().getBytes(StandardCharsets.UTF_8);
        }
    }
    
    private byte[] readAllBytes(InputStream is) throws Exception {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[1024];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        buffer.flush();
        return buffer.toByteArray();
    }

    private Result<Object, MessageAttributes> buildNoMessageResult(long timeout) {
        try {
            MessageAttributes attributes = new MessageAttributes();
            attributes.setStatus("NO_MESSAGE");
            attributes.setStatusMessage("No message available in queue within timeout period");
            attributes.setTimeout(timeout);
            
            InputStream emptyStream = new java.io.ByteArrayInputStream(new byte[0]);
            
            return Result.<Object, MessageAttributes>builder()
                    .output(emptyStream)
                    .attributes(attributes)
                    .mediaType(org.mule.runtime.api.metadata.MediaType.ANY)
                    .build();
        } catch (Exception e) {
            LOGGER.error("Error building no message result", e);
            return buildErrorResult("ERROR", "Error building response: " + e.getMessage());
        }
    }

    private Result<Object, MessageAttributes> buildErrorResult(String status, String errorMessage) {
        try {
            MessageAttributes attributes = new MessageAttributes();
            attributes.setStatus(status);
            attributes.setErrorMessage(errorMessage);
            
            InputStream emptyStream = new java.io.ByteArrayInputStream(new byte[0]);
            
            return Result.<Object, MessageAttributes>builder()
                    .output(emptyStream)
                    .attributes(attributes)
                    .mediaType(org.mule.runtime.api.metadata.MediaType.ANY)
                    .build();
        } catch (Exception e) {
            LOGGER.error("Error building error result", e);
            // Fallback - return minimal result
            MessageAttributes fallbackAttrs = new MessageAttributes();
            fallbackAttrs.setStatus("ERROR");
            fallbackAttrs.setErrorMessage("Critical error: " + e.getMessage());
            InputStream emptyStream = new java.io.ByteArrayInputStream(new byte[0]);
            return Result.<Object, MessageAttributes>builder()
                    .output(emptyStream)
                    .attributes(fallbackAttrs)
                    .mediaType(org.mule.runtime.api.metadata.MediaType.ANY)
                    .build();
        }
    }

    private void handleJmsException(JMSException e, SapAmqpConnectorConnection connection) 
            throws ConnectionException {
        String errorMessage = (e.getMessage() != null) ? e.getMessage().toLowerCase() : "";
        LOGGER.error("JMS Error: {}", e.getMessage());
        
        if (isAuthenticationError(errorMessage)) {
            LOGGER.warn("Authentication failed - invalidating token");
            connection.clearToken();
            throw new ConnectionException("Authentication failed: " + e.getMessage(), e);
        } else {
            LOGGER.error("JMSException occurred", e);
            throw new RuntimeException("Error during JMS operation: " + e.getMessage(), e);
        }
    }

    private Result<Object, MessageAttributes> handleJmsExceptionForConsume(JMSException e, SapAmqpConnectorConnection connection) {
        String errorMessage = (e.getMessage() != null) ? e.getMessage().toLowerCase() : "";
        LOGGER.error("JMS Error: {}", e.getMessage());
        
        if (isAuthenticationError(errorMessage)) {
            LOGGER.warn("Authentication failed - invalidating token");
            connection.clearToken();
            return buildErrorResult("AUTHENTICATION_ERROR", "Authentication failed: " + e.getMessage());
        } else {
            LOGGER.error("JMSException occurred", e);
            return buildErrorResult("JMS_ERROR", "Error during JMS operation: " + e.getMessage());
        }
    }

    private boolean isAuthenticationError(String errorMessage) {
        return errorMessage.contains("unauthorized") || 
               errorMessage.contains("401") ||
               errorMessage.contains("authentication failed") || 
               errorMessage.contains("forbidden") || 
               errorMessage.contains("security exception");
    }

    private void closeResources(AutoCloseable resource1, AutoCloseable resource2, 
            SapAmqpConnectorConnection connection) {
        LOGGER.debug("Closing JMS resources...");
        
        if (resource1 != null) {
            try { 
                resource1.close(); 
            } catch (Exception e) { 
                LOGGER.error("Error closing resource", e); 
            }
        }
        
        if (resource2 != null) {
            try { 
                resource2.close(); 
            } catch (Exception e) { 
                LOGGER.error("Error closing resource", e); 
            }
        }
        
        try { 
            connection.disconnect(); 
            LOGGER.debug("JMS Connection closed"); 
        } catch (Exception e) { 
            LOGGER.error("Error closing connection", e); 
        }
    }
    
    // ========================================================================
    // INNER CLASS: Acknowledgment Registry
    // ========================================================================
    
    /**
     * Registry for managing pending message acknowledgments in CLIENT mode.
     * Stores JMS Message and Session references for later acknowledge/nack operations.
     * Singleton pattern for thread-safe access across the connector.
     */
    public static class AcknowledgmentRegistry {
        
        private static final Logger LOGGER = LoggerFactory.getLogger(AcknowledgmentRegistry.class);
        
        // Singleton instance
        private static final AcknowledgmentRegistry INSTANCE = new AcknowledgmentRegistry();
        
        // Storage for pending acknowledgments
        private final Map<String, PendingAcknowledgment> pendingAcks = new java.util.concurrent.ConcurrentHashMap<>();
        
        private AcknowledgmentRegistry() {
            // Private constructor for singleton
        }
        
        public static AcknowledgmentRegistry getInstance() {
            return INSTANCE;
        }
        
        /**
         * Register a message for acknowledgment and return a unique acknowledgment ID
         */
        public String registerMessage(Message message, Session session) {
            String ackId = java.util.UUID.randomUUID().toString();
            PendingAcknowledgment pending = new PendingAcknowledgment(message, session);
            pendingAcks.put(ackId, pending);
            
            LOGGER.debug("Registered message for acknowledgment with ID: {}", ackId);
            LOGGER.debug("Total pending acknowledgments: {}", pendingAcks.size());
            
            return ackId;
        }
        
        /**
         * Get a pending acknowledgment by ID
         */
        public PendingAcknowledgment getPendingAcknowledgment(String ackId) {
            return pendingAcks.get(ackId);
        }
        
        /**
         * Remove a pending acknowledgment after it has been processed
         */
        public void removePendingAcknowledgment(String ackId) {
            PendingAcknowledgment removed = pendingAcks.remove(ackId);
            if (removed != null) {
                LOGGER.debug("Removed pending acknowledgment with ID: {}", ackId);
                LOGGER.debug("Remaining pending acknowledgments: {}", pendingAcks.size());
            } else {
                LOGGER.warn("Attempted to remove non-existent acknowledgment ID: {}", ackId);
            }
        }
        
        /**
         * Clear all pending acknowledgments (typically on shutdown)
         */
        public void clearAll() {
            int count = pendingAcks.size();
            pendingAcks.clear();
            LOGGER.info("Cleared {} pending acknowledgments", count);
        }
        
        /**
         * Get the count of pending acknowledgments
         */
        public int getPendingCount() {
            return pendingAcks.size();
        }
        
        /**
         * Inner class to hold message and session references
         */
        public static class PendingAcknowledgment {
            private final Message message;
            private final Session session;
            private final long registeredTime;
            
            public PendingAcknowledgment(Message message, Session session) {
                this.message = message;
                this.session = session;
                this.registeredTime = System.currentTimeMillis();
            }
            
            public Message getMessage() {
                return message;
            }
            
            public Session getSession() {
                return session;
            }
            
            public long getRegisteredTime() {
                return registeredTime;
            }
            
            public long getAgeInSeconds() {
                return (System.currentTimeMillis() - registeredTime) / 1000;
            }
        }
    }
}