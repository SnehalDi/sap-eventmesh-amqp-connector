package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import java.util.Map;
import java.util.HashMap;

/**
 * Immutable attributes class for SAP Event Mesh messages.
 * Contains metadata about consumed messages.
 */
public class MessageAttributes {
    
    // Standard JMS attributes
    private String messageId;
    private Long timestamp;
    private String correlationId;
    private String replyTo;
    private String destination;
    private Integer deliveryMode;
    private Boolean redelivered;
    private String type;
    private Long expiration;
    private Integer priority;
    private String messageType;
    private String mimeType;
    private String contentType;
    
    // Status attributes (for error/no message cases)
    private String status;
    private String statusMessage;
    private String errorMessage;
    private Long timeout;
    
    // Custom properties
    private Map<String, Object> customProperties;
    
    public MessageAttributes() {
        this.customProperties = new HashMap<>();
    }
    
    // Getters and Setters for Standard JMS attributes
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getCorrelationId() {
        return correlationId;
    }
    
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    
    public String getReplyTo() {
        return replyTo;
    }
    
    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }
    
    public String getDestination() {
        return destination;
    }
    
    public void setDestination(String destination) {
        this.destination = destination;
    }
    
    public Integer getDeliveryMode() {
        return deliveryMode;
    }
    
    public void setDeliveryMode(Integer deliveryMode) {
        this.deliveryMode = deliveryMode;
    }
    
    public Boolean getRedelivered() {
        return redelivered;
    }
    
    public void setRedelivered(Boolean redelivered) {
        this.redelivered = redelivered;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public Long getExpiration() {
        return expiration;
    }
    
    public void setExpiration(Long expiration) {
        this.expiration = expiration;
    }
    
    public Integer getPriority() {
        return priority;
    }
    
    public void setPriority(Integer priority) {
        this.priority = priority;
    }
    
    public String getMessageType() {
        return messageType;
    }
    
    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }
    
    public String getMimeType() {
        return mimeType;
    }
    
    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }
    
    // Getters and Setters for Status attributes
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public String getStatusMessage() {
        return statusMessage;
    }
    
    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    public Long getTimeout() {
        return timeout;
    }
    
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }
    
    // Getters and Setters for Custom properties
    
    public Map<String, Object> getCustomProperties() {
        return customProperties;
    }
    
    public void setCustomProperties(Map<String, Object> customProperties) {
        this.customProperties = customProperties != null ? customProperties : new HashMap<>();
    }
}