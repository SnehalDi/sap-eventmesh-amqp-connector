# Contributing to SAP Event Mesh AMQP Connector

First off, thank you for considering contributing to the SAP Event Mesh AMQP Connector! It's people like you that make this connector better for everyone.

## 🌟 How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues to avoid duplicates. When creating a bug report, include as many details as possible:

**Bug Report Template:**
```markdown
**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Configure connector with '...'
2. Execute operation '...'
3. See error

**Expected behavior**
A clear description of what you expected to happen.

**Environment:**
- Anypoint Studio version: [e.g., 7.15]
- Mule Runtime version: [e.g., 4.4.0]
- Java version: [e.g., JDK 17]
- Connector version: [e.g., 1.0.14]

**Error Messages/Logs:**
```
Paste relevant error messages or logs here
```

**Additional context**
Add any other context about the problem here.
```

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, include:

- **Clear title** summarizing the enhancement
- **Detailed description** of the proposed functionality
- **Use cases** explaining why this would be useful
- **Possible implementation** if you have ideas

### Pull Requests

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** following the coding standards below
3. **Test your changes** thoroughly
4. **Update documentation** if needed
5. **Commit with clear messages** following the commit guidelines
6. **Submit a pull request**

## 💻 Development Setup

### Prerequisites
- JDK 8 or higher (JDK 17 recommended)
- Maven 3.6.0 or higher
- Anypoint Studio 7.15 or higher
- Git

### Setting Up Your Development Environment

1. **Clone the repository:**
```bash
git clone https://github.com/NimraZafar0802/sap-eventmesh-amqp-connector.git
cd sap-eventmesh-amqp-connector
```

2. **Build the project:**
```bash
mvn clean install
```

3. **Import into Anypoint Studio:**
   - File → Import → Anypoint Studio → Anypoint Connector Project from External Location
   - Browse to the cloned directory

### Project Structure
```
src/main/java/
└── com/mycompany/mule/connectors/sapAMQPConnector/internal/
    ├── SapAmqpConnectorConfiguration.java
    ├── SapAmqpConnectorConnection.java
    ├── SapAmqpConnectorConnectionProvider.java
    ├── SapAmqpConnectorExtension.java
    ├── SapAmqpConnectorOperations.java
    └── SapAmqpConnectorMessageSource.java
```

## 📝 Coding Standards

### Java Code Style

- **Indentation**: 4 spaces (no tabs)
- **Line length**: Maximum 120 characters
- **Naming conventions**:
  - Classes: PascalCase (e.g., `SapAmqpConnectorOperations`)
  - Methods: camelCase (e.g., `publishMessage`)
  - Constants: UPPER_SNAKE_CASE (e.g., `DEFAULT_TIMEOUT`)
  - Private fields: camelCase with descriptive names

### Code Quality

- Write self-documenting code with clear variable names
- Add JavaDoc comments for public methods and classes
- Handle exceptions appropriately
- Log meaningful information at appropriate levels (DEBUG, INFO, WARN, ERROR)

### Example:
```java
/**
 * Publishes a message to the specified SAP Event Mesh queue.
 * 
 * @param config The connector configuration containing connection details
 * @param connection The active connection to SAP Event Mesh
 * @param queueName The name of the destination queue
 * @param payload The message content to publish
 * @throws ConnectionException if authentication or connection fails
 */
@DisplayName("Publish Message")
@MediaType(value = ANY, strict = false)
public void publishMessage(
        @Config SapAmqpConnectorConfiguration config,
        @Connection SapAmqpConnectorConnection connection,
        @DisplayName("Queue Name") String queueName,
        @Content @DisplayName("Message Payload") Object payload) 
        throws ConnectionException {
    // Implementation
}
```

## 🧪 Testing Guidelines

### Before Submitting
- Build passes: `mvn clean install`
- All existing tests pass
- New features include tests
- Manual testing performed in Anypoint Studio

### Test Categories
1. **Unit Tests**: Test individual methods and classes
2. **Integration Tests**: Test with actual SAP Event Mesh instance
3. **Manual Tests**: UI and flow testing in Anypoint Studio

## 📋 Commit Message Guidelines

Follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types
- **feat**: New feature
- **fix**: Bug fix
- **docs**: Documentation changes
- **style**: Code style changes (formatting, etc.)
- **refactor**: Code refactoring
- **test**: Adding or updating tests
- **chore**: Maintenance tasks

### Examples
```
feat(operations): add support for topic publishing

- Implemented publishToTopic operation
- Added topic name parameter
- Updated connection handling for topics

Closes #123
```

```
fix(auth): resolve token refresh race condition

Fixed issue where concurrent operations could cause
duplicate token fetch requests during refresh.

Fixes #456
```

## 🔄 Pull Request Process

1. **Update documentation** for any changed functionality
2. **Add tests** for new features
3. **Ensure CI passes** (all tests must pass)
4. **Update CHANGELOG.md** with your changes
5. **Request review** from maintainers
6. **Address review comments** promptly

## 📖 Documentation Standards

- Update README.md for significant feature additions
- Add inline comments for complex logic
- Include usage examples for new operations
- Update configuration reference as needed

## 🤔 Questions?

Feel free to:
- Open an issue for questions
- Start a discussion in GitHub Discussions
- Reach out to maintainers

## 🏆 Recognition

Contributors will be recognized in:
- README.md acknowledgments section
- Release notes for significant contributions
- Project documentation

## 📜 Code of Conduct

### Our Pledge
We are committed to providing a welcoming and inspiring community for all.

### Our Standards
- Be respectful and inclusive
- Accept constructive criticism gracefully
- Focus on what's best for the community
- Show empathy towards others

### Unacceptable Behavior
- Harassment or discrimination of any kind
- Trolling, insulting, or derogatory comments
- Public or private harassment
- Publishing others' private information

## 📞 Contact

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and community discussion

---

Thank you for contributing! Your efforts help make this connector better for the entire MuleSoft and SAP integration community. 🙏
