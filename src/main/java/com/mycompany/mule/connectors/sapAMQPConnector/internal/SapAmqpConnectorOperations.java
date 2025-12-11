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
import org.apache.qpid.jms.JmsConnectionFactory;
import org.mule.runtime.api.connection.ConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
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
            
            // Extract and parse MIME type from JMS properties
            org.mule.runtime.api.metadata.MediaType outputMediaType = extractMediaType(message);
            
            LOGGER.info("custom contentType: {}", message.getStringProperty("contentType"));
            LOGGER.info("contentType: {}", message.getStringProperty("JMS_AMQP_ContentType"));
;           LOGGER.info("JMSX contentType: {}", message.getStringProperty("JMSXContentType"));

            LOGGER.info("Message ID: {}, Type: {}, MIME Type: {}, Size: {} bytes", 
                attributes.getMessageId(), attributes.getMessageType(), 
                outputMediaType.toRfcString(), payloadBytes.length);
            
            // Parse payload based on content type
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
        if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            long bodyLength = bytesMessage.getBodyLength();
            if (bodyLength > Integer.MAX_VALUE) {
                throw new RuntimeException("Message too large: " + bodyLength + " bytes");
            }
            byte[] bytes = new byte[(int) bodyLength];
            bytesMessage.readBytes(bytes);
            LOGGER.debug("Extracted {} bytes from BytesMessage", bytes.length);
            return bytes;
        } else if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            byte[] bytes = text != null ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
            LOGGER.info("Extracted {} bytes from TextMessage", bytes.length); //changed to info
            return bytes;
        } else if (message instanceof ObjectMessage) {
            Object obj = ((ObjectMessage) message).getObject();
            String text = obj != null ? obj.toString() : "";
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            LOGGER.debug("Extracted {} bytes from ObjectMessage", bytes.length);
            return bytes;
        } else {
            String text = message.toString();
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            LOGGER.debug("Extracted {} bytes from generic Message", bytes.length);
            return bytes;
        }
    }

    private org.mule.runtime.api.metadata.MediaType extractMediaType(Message message) {
        try {
            // Check for AMQP 1.0 content-type property (standard for SAP Event Mesh and all AMQP clients)
            // When consumed, Qpid JMS maps AMQP content-type to JMSXContentType
            String contentType = message.getStringProperty("JMSXContentType");
            if (contentType != null && !contentType.trim().isEmpty()) {
                LOGGER.debug("Found AMQP content-type property: {}", contentType);
                try {
                    return org.mule.runtime.api.metadata.MediaType.parse(contentType);
                } catch (Exception e) {
                    LOGGER.debug("Content-type is not a valid MIME type: {}", contentType);
                }
            }
            
            // Fallback: Try to detect from JMSType (AMQP subject field)
            String jmsType = message.getJMSType();
            if (jmsType != null && !jmsType.trim().isEmpty()) {
                LOGGER.debug("Attempting to parse JMSType as MIME type: {}", jmsType);
                try {
                    return org.mule.runtime.api.metadata.MediaType.parse(jmsType);
                } catch (Exception e) {
                    LOGGER.debug("JMSType is not a valid MIME type: {}", jmsType);
                }
            }
            
            // Default to ANY
            LOGGER.debug("No MIME type found in any property, defaulting to ANY");
            return org.mule.runtime.api.metadata.MediaType.ANY;
            
        } catch (Exception e) {
            LOGGER.warn("Error extracting MIME type: {}", e.getMessage());
            return org.mule.runtime.api.metadata.MediaType.ANY;
        }
    }

    private Object parsePayload(byte[] payloadBytes, org.mule.runtime.api.metadata.MediaType mediaType) {
        try {
            String mimeType = mediaType.toRfcString().toLowerCase();
            LOGGER.debug("Parsing payload with MIME type: {}", mimeType);
            
            // Handle JSON content types
            if (mimeType.contains("application/json") || mimeType.contains("+json")) {
                String jsonString = new String(payloadBytes, StandardCharsets.UTF_8);
                LOGGER.debug("Parsing JSON payload (size: {} bytes)", payloadBytes.length);
                
                try {
                    // Parse to Map/List structure for Mule to work with
                    Object parsed = objectMapper.readValue(jsonString, Object.class);
                    LOGGER.info("Successfully parsed JSON payload");
                    return parsed;
                } catch (Exception e) {
                    LOGGER.warn("Failed to parse as JSON, returning as String: {}", e.getMessage());
                    return jsonString;
                }
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
        
        // Extract Content-Type from AMQP message
        // Priority order: JMSXContentType (AMQP 1.0 standard) > custom SAP properties > fallback attempts
        String contentType = message.getStringProperty("JMS_AMQP_ContentType");
//        
//        try {
//            // Primary: Standard AMQP content-type (mapped to JMSXContentType by Qpid JMS)
//            contentType = message.getStringProperty("JMSXContentType");
//            if (contentType != null && !contentType.trim().isEmpty()) {
//                LOGGER.debug("Content-Type extracted from JMSXContentType: {}", contentType);
//            }
//        } catch (Exception e) {
//            LOGGER.debug("Could not extract JMSXContentType: {}", e.getMessage());
//        }
//        
//        // Fallback: Try SAP-specific property names
//        if (contentType == null || contentType.trim().isEmpty()) {
//            try {
//                contentType = message.getStringProperty("JMS_AMQP_ContentType");
//                if (contentType != null && !contentType.trim().isEmpty()) {
//                    LOGGER.debug("Content-Type extracted from JMS_SAP_ContentType: {}", contentType);
//                }
//            } catch (Exception e) {
//                LOGGER.debug("Could not extract JMS_SAP_ContentType: {}", e.getMessage());
//            }
//        }
//        
//        // Additional fallback attempts
//        if (contentType == null || contentType.trim().isEmpty()) {
//            try {
//                contentType = message.getStringProperty("contentType");
//                if (contentType != null && !contentType.trim().isEmpty()) {
//                    LOGGER.debug("Content-Type extracted from contentType: {}", contentType);
//                }
//            } catch (Exception e) {
//                LOGGER.debug("Could not extract contentType: {}", e.getMessage());
//            }
//        }
//        
//        if (contentType == null || contentType.trim().isEmpty()) {
//            try {
//                contentType = message.getStringProperty("content_type");
//                if (contentType != null && !contentType.trim().isEmpty()) {
//                    LOGGER.debug("Content-Type extracted from content_type: {}", contentType);
//                }
//            } catch (Exception e) {
//                LOGGER.debug("Could not extract content_type: {}", e.getMessage());
//            }
//        }
        
        // Set the content type in attributes (even if null)
        attributes.setContentType(contentType);
        if (contentType != null) {
            LOGGER.info("Content-Type set in attributes: {}", contentType);
        } else {
            LOGGER.debug("No Content-Type found in message properties");
        }
        
        // Determine message type
        String messageType = "Unknown";
        if (message instanceof TextMessage) {
            messageType = "TextMessage";
        } else if (message instanceof BytesMessage) {
            messageType = "BytesMessage";
        } else if (message instanceof ObjectMessage) {
            messageType = "ObjectMessage";
        }
        attributes.setMessageType(messageType);
        
        // Extract MIME type and store in attributes for reference
        try {
            String mimeType = message.getStringProperty("JMSXContentType");
            if (mimeType != null) {
                attributes.setMimeType(mimeType);
            }
        } catch (Exception e) {
            LOGGER.debug("Could not extract MIME type property: {}", e.getMessage());
        }

        // Custom properties
        Map<String, Object> customProperties = new HashMap<>();
        java.util.Enumeration<?> propertyNames = message.getPropertyNames();
        while (propertyNames.hasMoreElements()) {
            String propertyName = (String) propertyNames.nextElement();
            
            // Skip internal properties
            if (JMS_AMQP_CONTENT_TYPE.equals(propertyName) ||
                "JMSXContentType".equals(propertyName)) {
                continue;
            }
            
            Object propertyValue = message.getObjectProperty(propertyName);
            customProperties.put(propertyName, propertyValue);
        }
        attributes.setCustomProperties(customProperties);

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
}