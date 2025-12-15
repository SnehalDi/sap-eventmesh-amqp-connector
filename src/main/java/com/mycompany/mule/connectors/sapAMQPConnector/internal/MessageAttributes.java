package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import java.util.Map;

/**
 * Message attributes including JMS headers, AMQP properties, and retry metadata
 */
public class MessageAttributes {
    
    // Standard JMS headers
    private String messageId;
    private long timestamp;
    private String correlationId;
    private String replyTo;
    private String destination;
    private int deliveryMode;
    private boolean redelivered;
    private String type;
    private long expiration;
    private int priority;
    
    // AMQP properties
    private String contentType;
    private String userId;
    private String groupId;
    private long groupSequence;
    private String replyToGroupId;
    
	  // Additional metadata
    private String messageType;
    private String mimeType;
    private String status;
    private String statusMessage;
    private String errorMessage;
    private Long timeout;
	
    // Custom properties (AMQP application properties + JMS custom properties)
    private Map<String, Object> customProperties;
    
    // Acknowledgment properties (for CLIENT mode)
    private String ackId;
    private boolean requiresAcknowledgment;
    

    
    // Getters and Setters - Standard JMS Headers
    
    public String getMessageId() {
        return messageId;
    }
    
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
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
    
    public int getDeliveryMode() {
        return deliveryMode;
    }
    
    public void setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }
    
    public boolean isRedelivered() {
        return redelivered;
    }
    
    public void setRedelivered(boolean redelivered) {
        this.redelivered = redelivered;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public long getExpiration() {
        return expiration;
    }
    
    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }
    
    public int getPriority() {
        return priority;
    }
    
    public void setPriority(int priority) {
        this.priority = priority;
    }
    
    // Getters and Setters - AMQP Properties
    
    public String getContentType() {
        return contentType;
    }
    
    public void setContentType(String contentType) {
        this.contentType = contentType;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getGroupId() {
        return groupId;
    }
    
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
    
    public long getGroupSequence() {
        return groupSequence;
    }
    
    public void setGroupSequence(long groupSequence) {
        this.groupSequence = groupSequence;
    }
    
    public String getReplyToGroupId() {
        return replyToGroupId;
    }
    
    public void setReplyToGroupId(String replyToGroupId) {
        this.replyToGroupId = replyToGroupId;
    }
	   
 // Additional metadata
    
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
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    public Long getTimeout() {
        return timeout;
    }
    
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }
    
    // Getters and Setters - Custom Properties
    
    public Map<String, Object> getCustomProperties() {
        return customProperties;
    }
    
    public void setCustomProperties(Map<String, Object> customProperties) {
        this.customProperties = customProperties;
    }
    
    // Getters and Setters - Acknowledgment Properties
    
    public String getackId() {
        return ackId;
    }
    
    public void setackId(String ackId) {
        this.ackId = ackId;
    }
    
    public boolean isRequiresAcknowledgment() {
        return requiresAcknowledgment;
    }
    
    public void setRequiresAcknowledgment(boolean requiresAcknowledgment) {
        this.requiresAcknowledgment = requiresAcknowledgment;
    }
  
 
    
    @Override
    public String toString() {
        return "MessageAttributes{" +
                "messageId='" + messageId + '\'' +
                ", timestamp=" + timestamp +
                ", correlationId='" + correlationId + '\'' +
                ", destination='" + destination + '\'' +
                ", contentType='" + contentType + '\'' +
                ", requiresAcknowledgment=" + requiresAcknowledgment +
                '}';
    }
}