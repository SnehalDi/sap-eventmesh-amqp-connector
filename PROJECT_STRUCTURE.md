# Project Structure Guide
## SAP Event Mesh AMQP 1.0 Custom Connector

This document explains the complete project structure and how files should be organized in your repository.

---

## 📂 Complete Repository Structure

```
sap-eventmesh-amqp-connector/
│
├── .git/                                  # Git version control (auto-generated, hidden)
│
├── .gitignore                             # Files to ignore in Git
├── README.md                              # Main documentation
├── LICENSE                                # MIT License
├── CONTRIBUTING.md                        # Contribution guidelines
├── GIT_SETUP_GUIDE.md                    # Git setup instructions
├── GIT_COMMANDS.md                       # Quick reference commands
├── example.properties                     # Configuration template (safe to commit)
│
├── pom.xml                                # Maven project configuration
├── mule-artifact.json                     # Mule artifact descriptor
│
├── src/                                   # Source code directory
│   ├── main/
│   │   ├── java/                          # Java source files
│   │   │   └── com/
│   │   │       └── mycompany/
│   │   │           └── mule/
│   │   │               └── connectors/
│   │   │                   └── sapAMQPConnector/
│   │   │                       └── internal/
│   │   │                           ├── SapAmqpConnectorConfiguration.java
│   │   │                           ├── SapAmqpConnectorConnection.java
│   │   │                           ├── SapAmqpConnectorConnectionProvider.java
│   │   │                           ├── SapAmqpConnectorExtension.java
│   │   │                           ├── SapAmqpConnectorOperations.java
│   │   │                           └── SapAmqpConnectorMessageSource.java
│   │   │
│   │   └── resources/                     # Resource files
│   │       ├── mule-artifact.json         # Mule configuration
│   │       └── META-INF/
│   │           └── mule-artifact/
│   │               └── mule-artifact.json # Additional metadata
│   │
│   └── test/                              # Test code directory
│       ├── java/                          # Java test files
│       │   └── com/
│       │       └── mycompany/
│       │           └── mule/
│       │               └── connectors/
│       │                   └── sapAMQPConnector/
│       │                       └── internal/
│       │                           └── SapAmqpConnectorOperationsTest.java
│       │
│       └── resources/                     # Test resources
│           └── test-config.properties     # Test configuration (in .gitignore)
│
├── docs/                                  # Additional documentation (optional)
│   ├── architecture.md                    # Architecture overview
│   ├── configuration-guide.md             # Detailed configuration
│   └── troubleshooting.md                 # Common issues and solutions
│
├── examples/                              # Example Mule flows (optional)
│   ├── publish-message-example.xml
│   ├── consume-message-example.xml
│   └── listener-example.xml
│
└── target/                                # Build output (in .gitignore)
    ├── classes/
    ├── generated-sources/
    ├── maven-archiver/
    ├── maven-status/
    └── sap-amqp-connector-1.0.14-mule-plugin.jar
```

---

## 📄 File Descriptions

### Root Level Files

| File | Purpose | Commit to Git? |
|------|---------|----------------|
| `.gitignore` | Specifies files Git should ignore | ✅ Yes |
| `README.md` | Main project documentation | ✅ Yes |
| `LICENSE` | Open source license (MIT) | ✅ Yes |
| `CONTRIBUTING.md` | Guidelines for contributors | ✅ Yes |
| `pom.xml` | Maven build configuration | ✅ Yes |
| `mule-artifact.json` | Mule runtime descriptor | ✅ Yes |
| `example.properties` | Configuration template | ✅ Yes |
| `sap-config.properties` | Actual credentials | ❌ No (in .gitignore) |

### Source Files (`src/main/java/`)

| File | Responsibility |
|------|----------------|
| `SapAmqpConnectorExtension.java` | Main connector extension entry point |
| `SapAmqpConnectorConfiguration.java` | Configuration parameters (URI, credentials) |
| `SapAmqpConnectorConnection.java` | Connection state and token management |
| `SapAmqpConnectorConnectionProvider.java` | Connection lifecycle management |
| `SapAmqpConnectorOperations.java` | Publish and consume operations |
| `SapAmqpConnectorMessageSource.java` | Message listener (event source) |

---

## 🗂️ How to Organize Your Local Project

### Step 1: Create Base Directory Structure

If starting fresh, create this structure:

```cmd
mkdir sap-eventmesh-amqp-connector
cd sap-eventmesh-amqp-connector
mkdir src\main\java\com\mycompany\mule\connectors\sapAMQPConnector\internal
mkdir src\main\resources
mkdir src\test\java\com\mycompany\mule\connectors\sapAMQPConnector\internal
mkdir src\test\resources
mkdir docs
mkdir examples
```

### Step 2: Place Files in Correct Locations

**Root Directory:**
```cmd
copy path\to\downloads\README.md .
copy path\to\downloads\.gitignore .
copy path\to\downloads\LICENSE .
copy path\to\downloads\CONTRIBUTING.md .
copy path\to\downloads\pom.xml .
copy path\to\downloads\example.properties .
```

**Java Source Files:**
```cmd
copy SapAmqpConnectorConfiguration.java src\main\java\com\mycompany\mule\connectors\sapAMQPConnector\internal\
copy SapAmqpConnectorConnection.java src\main\java\com\mycompany\mule\connectors\sapAMQPConnector\internal\
copy SapAmqpConnectorConnectionProvider.java src\main\java\com\mycompany\mule\connectors\sapAMQPConnector\internal\
copy SapAmqpConnectorExtension.java src\main\java\com\mycompany\mule\connectors\sapAMQPConnector\internal\
copy SapAmqpConnectorOperations.java src\main\java\com\mycompany\mule\connectors\sapAMQPConnector\internal\
copy SapAmqpConnectorMessageSource.java src\main\java\com\mycompany\mule\connectors\sapAMQPConnector\internal\
```

**Resource Files:**
```cmd
copy mule-artifact.json src\main\resources\
```

### Step 3: Create Configuration File (DO NOT COMMIT!)

```cmd
copy example.properties sap-config.properties
notepad sap-config.properties
```
Fill in your actual credentials.

---

## 🚫 Files That Should NEVER Be in Git

### Automatically Ignored (via .gitignore)

```
# Build artifacts
target/
*.class
*.jar (except gradle-wrapper.jar)

# IDE files
.idea/
*.iml
.settings/
.classpath
.project

# Credentials (IMPORTANT!)
sap-config.properties
*.properties (except example.properties)
*-secrets.json
*.env

# OS files
.DS_Store
Thumbs.db

# Logs
*.log
logs/

# Temporary files
*.tmp
*.temp
temp/
```

---

## ✅ Files That SHOULD Be in Git

### Essential Files
- All `.java` source files
- `pom.xml`
- `mule-artifact.json`
- `README.md`
- `LICENSE`
- `.gitignore`
- `example.properties` (template only!)

### Documentation Files
- `CONTRIBUTING.md`
- `GIT_SETUP_GUIDE.md`
- Any documentation in `docs/` folder

### Example Files (Optional)
- Example Mule XML flows in `examples/`
- Architecture diagrams in `docs/`

---

## 📦 Maven Build Output

When you run `mvn clean install`, Maven creates:

```
target/
├── classes/                               # Compiled .class files
├── generated-sources/                     # Auto-generated code
├── maven-archiver/                        # Maven metadata
├── maven-status/                          # Build status
├── sap-amqp-connector-1.0.14.jar         # Base JAR
└── sap-amqp-connector-1.0.14-mule-plugin.jar  # Mule plugin (THIS IS YOUR CONNECTOR!)
```

**Important**: The `target/` directory is in `.gitignore` and should NEVER be committed!

---

## 🔍 Verification Checklist

Before your first Git commit, verify:

### ✅ File Organization
- [ ] All Java files in correct package structure
- [ ] `pom.xml` in root directory
- [ ] `README.md` in root directory
- [ ] `.gitignore` in root directory
- [ ] No credentials in any committed file

### ✅ Git Ignore Working
Run these commands and verify output:

```cmd
REM This should show staged files (green)
git status

REM These should NOT appear in git status:
REM - target/ directory
REM - *.class files
REM - sap-config.properties
REM - *.log files
```

### ✅ Build Success
```cmd
mvn clean install
```
Should complete with `BUILD SUCCESS`

### ✅ No Credentials in Code
```cmd
findstr /S /I "password" *.java
findstr /S /I "secret" *.java
```
Should only find: `getClientSecret()` method references, NOT actual values!

---

## 📁 Optional Additions

### Add Architecture Diagram
Create `docs/architecture.md` with:
- Component diagram
- Data flow diagram
- Sequence diagram for authentication

### Add Example Flows
Create example Mule XML files in `examples/`:
- `publish-example.xml`
- `consume-example.xml`
- `listener-example.xml`

### Add Tests
Create test classes in `src/test/java/`:
- Unit tests for operations
- Integration tests
- Mock-based tests

---

## 🎯 Working with Anypoint Studio

### Import Existing Connector Project

1. Open Anypoint Studio
2. File → Import → Anypoint Studio → Anypoint Connector Project from External Location
3. Browse to your `sap-eventmesh-amqp-connector` directory
4. Click Finish

### Studio Project Structure

Anypoint Studio will add these files (should be in .gitignore):
```
.classpath
.project
.settings/
.mule/
```

These are IDE-specific and should NOT be committed to Git.

---

## 🔄 Keeping Repository Clean

### Regular Maintenance

```cmd
REM Remove untracked files (be careful!)
git clean -n          # Preview what will be deleted
git clean -fd         # Actually delete

REM Remove files from Git but keep locally
git rm --cached filename
git rm -r --cached folder/

REM Update .gitignore for existing files
git rm -r --cached .
git add .
git commit -m "chore: update gitignore"
```

---

## 📊 Repository Size Management

### Keep Repository Lean

- Build artifacts in `target/` are automatically ignored
- JAR files are automatically ignored (connector will be rebuilt)
- Large binary files should use Git LFS if needed
- Historical credential leaks should be purged immediately

### If Repository Gets Too Large

```cmd
REM Check repository size
git count-objects -vH

REM Find large files
git rev-list --objects --all | git cat-file --batch-check='%(objecttype) %(objectname) %(objectsize) %(rest)' | sort -k3 -n
```

---

## 🎓 Best Practices Summary

1. ✅ **Commit Source Code**: All `.java` files, `pom.xml`, documentation
2. ❌ **Don't Commit**: Build artifacts, credentials, IDE files, logs
3. 📝 **Document Everything**: README, code comments, configuration examples
4. 🔐 **Security First**: Never commit credentials, even temporarily
5. 🏗️ **Organize Logically**: Follow Maven standard directory layout
6. 🧪 **Include Tests**: Keep test code in separate `src/test/` directory
7. 📦 **Ignore Build Output**: Let Maven regenerate on each machine

---

## 📞 Need Help?

If your project structure doesn't match this guide:

1. Check Maven standard directory layout
2. Verify `pom.xml` has correct paths
3. Rebuild project: `mvn clean install`
4. Reimport in Anypoint Studio

---

**Remember**: A well-organized repository is easier to maintain, collaborate on, and understand!

---

*Last Updated: November 2024*  
*Created by: Nimra Zafar*
