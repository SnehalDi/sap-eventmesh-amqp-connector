# SAP Event Mesh AMQP 1.0 Custom Connector for MuleSoft

A custom MuleSoft connector that resolves protocol mismatch issues when connecting to SAP Event Mesh using AMQP 1.0 protocol over WebSocket with OAuth2 authentication.

## 🎯 Overview

This connector addresses the challenge of connecting MuleSoft applications to SAP Event Mesh when standard AMQPS ports (5671) are blocked by corporate firewalls. It uses WebSocket transport (port 443) with AMQP 1.0 protocol, enabling secure communication through standard HTTPS ports.

### Key Features

- ✅ **WebSocket Transport**: Uses `wss://` (port 443) instead of `amqps://` (port 5671)
- ✅ **OAuth2 Authentication**: Automatic token fetching and management using client credentials flow
- ✅ **Protocol Conversion**: Converts WebSocket URIs to AMQP WebSocket protocol (`amqpwss://`)
- ✅ **Message Publishing**: Send messages to SAP Event Mesh queues and topics
- ✅ **Message Listening**: Consume messages from SAP Event Mesh queues with configurable consumers
- ✅ **Flexible Message Types**: Support for TextMessage, BytesMessage, and ObjectMessage
- ✅ **Token Management**: Automatic token caching and refresh on authentication failures
- ✅ **Firewall-Friendly**: Works through corporate firewalls by using standard HTTPS port (443)

## 🔧 Technical Solution

### The Problem
SAP Event Mesh typically uses AMQP 1.0 protocol on port 5671 (`amqps://`), which is often blocked by corporate firewalls. This prevents MuleSoft applications from connecting to SAP Event Mesh in enterprise environments.

### The Solution
This connector solves the problem by:

1. **WebSocket Transport**: Uses WebSocket Secure (`wss://`) on port 443
2. **Protocol Bridging**: Converts WebSocket URLs to AMQP WebSocket format (`amqpwss://`)
3. **OAuth2 Integration**: Fetches OAuth2 tokens and passes them via HTTP headers
4. **Qpid JMS Client**: Leverages Apache Qpid JMS client for AMQP 1.0 support

### Connection Flow

```
MuleSoft Flow → Custom Connector → OAuth2 Token Fetch → 
WebSocket Connection (wss://host:443) → AMQP 1.0 Protocol → 
SAP Event Mesh Queue/Topic
```

## 📋 Prerequisites

- **Anypoint Studio**: 7.15 or higher
- **Mule Runtime**: 4.4.0 or higher
- **Java**: JDK 8 or higher 
- **Maven**: 3.6.0 or higher
- **SAP BTP Account**: With Event Mesh service instance

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/nimrazafar/sap-eventmesh-amqp-connector.git
cd sap-eventmesh-amqp-connector
```

### 2. Build the Connector

```bash
mvn clean install
```

This will generate the connector JAR file in the `target` directory.

### 3. Install in Anypoint Studio

**Option A: Local Repository Installation**
1. Copy the generated JAR from `target/` to your Mule project's `lib` folder
2. Add dependency in your project's `pom.xml`:

```xml
<dependency>
    <groupId>com.myCompany</groupId>
    <artifactId>sap-amqp-connector</artifactId>
    <version>1.0.14</version>
    <classifier>mule-plugin</classifier>
</dependency>
```

**Option B: Maven Repository (if published)**
```bash
mvn install:install-file \
  -Dfile=target/sap-amqp-connector-1.0.14-mule-plugin.jar \
  -DgroupId=com.myCompany \
  -DartifactId=sap-amqp-connector \
  -Dversion=1.0.14 \
  -Dpackaging=mule-plugin
```

### 4. Add to Mule Palette

After installation, the connector will appear in the Anypoint Studio Mule Palette under "Sap Amqp Connector".

## ⚙️ Configuration

### SAP Event Mesh Credentials

You'll need the following from your SAP BTP Event Mesh service instance:

1. **WebSocket URI**: `wss://your-instance.eu20.a.eventmesh.integration.cloud.sap/protocols/amqp10ws`
2. **Token Endpoint URL**: `https://your-subdomain.authentication.eu20.hana.ondemand.com/oauth/token`
3. **Client ID**: Your OAuth2 client ID
4. **Client Secret**: Your OAuth2 client secret

### Connector Configuration in Mule

```xml
<sap-amqp:config name="SAP_Event_Mesh_Config">
    <sap-amqp:connection 
        uri="wss://your-instance.eu20.a.eventmesh.integration.cloud.sap/protocols/amqp10ws"
        tokenUrl="https://your-subdomain.authentication.eu20.hana.ondemand.com/oauth/token"
        clientId="${sap.clientId}"
        clientSecret="${sap.clientSecret}" />
</sap-amqp:config>
```

**Best Practice**: Store credentials in a properties file:

```properties
# sap-config.properties
sap.clientId=your-client-id
sap.clientSecret=your-client-secret
sap.uri=wss://your-instance.eu20.a.eventmesh.integration.cloud.sap/protocols/amqp10ws
sap.tokenUrl=https://your-subdomain.authentication.eu20.hana.ondemand.com/oauth/token
```

## 📖 Usage Examples

### Example 1: Publish Message to Queue

```xml
<flow name="publish-message-flow">
    <http:listener config-ref="HTTP_Listener_config" path="/publish"/>
    
    <sap-amqp:publish-message 
        config-ref="SAP_Event_Mesh_Config" 
        queueName="your-queuename">
        <sap-amqp:message-payload>
            <![CDATA[#[%dw 2.0
                output application/json
                ---
                {
                    "orderId": "12345",
                    "status": "PROCESSED",
                    "timestamp": now()
                }
            ]]]>
        </sap-amqp:message-payload>
    </sap-amqp:publish-message>
    
    <set-payload value="Message published successfully"/>
</flow>
```

### Example 2: Listen for Messages from Queue

```xml
<flow name="message-listener-flow">
    <sap-amqp:listener 
        config-ref="SAP_Event_Mesh_Config"
        queueName="your-queuename"
        numberOfConsumers="2"
        ackMode="AUTO">
    </sap-amqp:listener>
    
    <logger level="INFO" message="Received message: #[payload]"/>
    
    <!-- Process the message -->
    <flow-ref name="process-order-flow"/>
</flow>
```

### Example 3: Consume Single Message

```xml
<flow name="consume-single-message-flow">
    <scheduler>
        <scheduling-strategy>
            <fixed-frequency frequency="10000"/>
        </scheduling-strategy>
    </scheduler>
    
    <sap-amqp:consume-message 
        config-ref="SAP_Event_Mesh_Config"
        queueName="your-queuename"
        timeout="5000"/>
    
    <choice>
        <when expression="#[payload != null]">
            <logger level="INFO" message="Message consumed: #[payload]"/>
        </when>
        <otherwise>
            <logger level="DEBUG" message="No message available"/>
        </otherwise>
    </choice>
</flow>
```

## 📊 Connector Operations

### 1. Publish Message
Sends a message to a specified queue or topic.

**Parameters:**
- `queueName` (Required): Name of the destination queue
- `payload` (Required): Message content (String, JSON, XML, etc.)

### 2. Message Listener (Source)
Listens continuously for messages from a queue.

**Parameters:**
- `queueName` (Required): Name of the queue to listen to
- `numberOfConsumers` (Optional, default: 1): Number of concurrent consumers
- `ackMode` (Optional, default: AUTO): Acknowledgment mode (AUTO, CLIENT, DUPS_OK)
- `messageSelector` (Optional): JMS message selector for filtering

### 3. Consume Message
Consumes a single message from a queue (polling).

**Parameters:**
- `queueName` (Required): Name of the queue
- `timeout` (Optional, default: 5000ms): Wait time for message availability

## 🏗️ Project Structure

```
sap-amqp-connector/
├── src/main/java/com/mycompany/mule/connectors/sapAMQPConnector/internal/
│   ├── SapAmqpConnectorConfiguration.java      # Connector configuration
│   ├── SapAmqpConnectorConnection.java         # Connection management & token handling
│   ├── SapAmqpConnectorConnectionProvider.java # Connection provider
│   ├── SapAmqpConnectorExtension.java          # Main extension class
│   ├── SapAmqpConnectorOperations.java         # Publish & consume operations
│   └── SapAmqpConnectorMessageSource.java      # Message listener source
├── src/main/resources/
│   └── mule-artifact.json                      # Mule artifact descriptor
├── pom.xml                                      # Maven configuration
└── README.md                                    # This file
```

## 🔍 Key Components

### 1. Configuration (`SapAmqpConnectorConfiguration.java`)
Defines connector configuration parameters: URI, token URL, client credentials.

### 2. Connection (`SapAmqpConnectorConnection.java`)
Manages JMS connections and OAuth2 token lifecycle with automatic refresh.

### 3. Operations (`SapAmqpConnectorOperations.java`)
Implements message publishing and consumption operations with error handling.

### 4. Message Source (`SapAmqpConnectorMessageSource.java`)
Provides message listener capability with multi-threaded consumers.

## 🛠️ Dependencies

```xml
<!-- Core Dependencies -->
<dependency>
    <groupId>org.apache.qpid</groupId>
    <artifactId>qpid-jms-client</artifactId>
    <version>0.57.0</version>
</dependency>

<dependency>
    <groupId>org.apache.httpcomponents</groupId>
    <artifactId>httpclient</artifactId>
    <version>4.5.13</version>
</dependency>

<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.12.7</version>
</dependency>
```

## 🐛 Troubleshooting

### Issue: Authentication Failures
**Symptom**: `401 Unauthorized` or `Authentication failed` errors

**Solution**:
- Verify client ID and client secret are correct
- Check token endpoint URL is accessible
- Ensure OAuth2 credentials have proper permissions in SAP BTP

### Issue: Connection Timeout
**Symptom**: Connection hangs or times out

**Solution**:
- Verify WebSocket URI is correct (should use `wss://` and port 443)
- Check if port 443 is open in your firewall
- Test connectivity: `curl -v https://your-instance.eu20.a.eventmesh.integration.cloud.sap`

### Issue: Protocol Mismatch
**Symptom**: `Protocol mismatch` or `Unsupported protocol` errors

**Solution**:
- Ensure URI uses WebSocket protocol (`wss://`)
- Connector automatically converts to `amqpwss://`
- Verify the path includes `/protocols/amqp10ws`

### Issue: Messages Not Received
**Symptom**: Listener starts but no messages arrive

**Solution**:
- Verify queue name is correct (case-sensitive)
- Check queue has messages in SAP Event Mesh console
- Ensure proper queue permissions in SAP BTP
- Review message selector if configured

## 📝 Configuration Properties Reference

| Property | Description | Required | Example |
|----------|-------------|----------|---------|
| `uri` | WebSocket URI for SAP Event Mesh | Yes | `wss://host.eu20.a.eventmesh.integration.cloud.sap/protocols/amqp10ws` |
| `tokenUrl` | OAuth2 token endpoint | Yes | `https://subdomain.authentication.eu20.hana.ondemand.com/oauth/token` |
| `clientId` | OAuth2 client ID | Yes | `sb-clone-xbem-service-...` |
| `clientSecret` | OAuth2 client secret | Yes | `********` |
| `queueName` | Destination queue name | Yes | `your-queuename` |
| `numberOfConsumers` | Concurrent consumers | No (default: 1) | `2` |
| `ackMode` | Acknowledgment mode | No (default: AUTO) | `AUTO`, `CLIENT`, `DUPS_OK` |
| `timeout` | Consume timeout (ms) | No (default: 5000) | `10000` |

## 🔐 Security Considerations

1. **Never commit credentials**: Use properties files or vault services
2. **Token Security**: Tokens are cached in memory and cleared on disconnect
3. **TLS/SSL**: Always use `wss://` protocol for encrypted communication
4. **Credential Rotation**: Update credentials regularly in SAP BTP
5. **Access Control**: Apply least privilege principle in SAP Event Mesh

## 🧪 Testing

### Unit Testing
```bash
mvn test
```

### Integration Testing
1. Configure test properties in `src/test/resources/test-config.properties`
2. Run integration tests:
```bash
mvn verify -P integration-tests
```

### Manual Testing in Anypoint Studio
1. Import connector into test Mule project
2. Configure connection with test credentials
3. Use Publish Message operation to send test message
4. Verify message in SAP Event Mesh console

## 📈 Performance Tips

1. **Connection Pooling**: Use multiple consumers for high-throughput scenarios
2. **Acknowledgment Mode**: Use `AUTO` for best performance, `CLIENT` for reliability
3. **Token Caching**: Connector automatically caches tokens to reduce OAuth2 calls
4. **Message Batching**: Process messages in batches when possible
5. **Error Handling**: Implement proper error handling to avoid message loss

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Qpid JMS Client for AMQP 1.0 protocol support
- MuleSoft Extension API and documentation
- SAP Event Mesh for event-driven architecture

## 📧 Support

For issues, questions, or contributions:
- **GitHub Issues**: [Create an issue](https://github.com/NimraZafar0802/sap-eventmesh-amqp-connector/issues)
- **Discussions**: [Join discussions](https://github.com/NimraZafar0802/sap-eventmesh-amqp-connector/discussions)

## 🗺️ Roadmap

- [ ] Add support for topics (pub/sub pattern)
- [ ] Implement message filtering capabilities
- [ ] Add metrics and monitoring integration
- [ ] Support for durable subscriptions
- [ ] Enhanced error recovery mechanisms
- [ ] Performance optimization for high-volume scenarios

---

**Built with ❤️ by Nimra Zafar**

**Version**: 1.0.14  
**Last Updated**: November 2025
