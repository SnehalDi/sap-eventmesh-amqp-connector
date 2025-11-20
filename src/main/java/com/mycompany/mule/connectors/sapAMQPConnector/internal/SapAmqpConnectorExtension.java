package com.mycompany.mule.connectors.sapAMQPConnector.internal;

import org.mule.runtime.extension.api.annotation.Extension;
import org.mule.runtime.extension.api.annotation.Configurations;
import org.mule.runtime.extension.api.annotation.dsl.xml.Xml;
// Removed JavaVersion imports

@Xml(prefix = "sap-amqp")
@Extension(name = "sap-amqp-connector")
@Configurations(SapAmqpConnectorConfiguration.class)
// Removed @JavaVersionSupport annotation
public class SapAmqpConnectorExtension {

}