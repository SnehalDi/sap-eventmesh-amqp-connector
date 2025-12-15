package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import org.mule.metadata.api.model.MetadataType;
import org.mule.runtime.api.metadata.resolving.OutputTypeResolver;
import org.mule.runtime.api.connection.ConnectionException;
import org.mule.runtime.api.metadata.MetadataResolvingException;
import org.mule.metadata.api.builder.BaseTypeBuilder;

/**
 * Output resolver for SAP AMQP Connector consume operation.
 * Returns the appropriate metadata type based on the message content-type.
 */
public class SapAmqpOutputResolver implements OutputTypeResolver<String> {

    @Override
    public String getCategoryName() {
        return "SapAmqpConnector";
    }

    @Override
    public MetadataType getOutputType(org.mule.runtime.api.metadata.MetadataContext context, String key)
            throws MetadataResolvingException, ConnectionException {
        
        // Return ANY type to allow dynamic content
        // The actual type will be determined at runtime based on the message content-type
        return BaseTypeBuilder.create(org.mule.metadata.api.model.MetadataFormat.JAVA).anyType().build();
    }
}