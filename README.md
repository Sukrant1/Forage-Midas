# 🏦 Midas Flow — JPMC Advanced Software Engineering (Forage)

> A Spring Boot microservice that processes financial transactions in real-time using Apache Kafka, persists user balances via JPA/H2, and exposes a REST API to query account balances.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Running the Application](#running-the-application)
  - [Running Tests](#running-tests)
- [Task Breakdown](#task-breakdown)
- [API Reference](#api-reference)
- [Key Components](#key-components)
- [Program Context](#program-context)

---

## Overview

**Midas Flow** (`midas-core`) is the core backend service built as part of the **JPMorgan Chase Advanced Software Engineering** virtual experience program on [Forage](https://www.theforage.com/). The project simulates a real-world fintech payment pipeline where:

- Users have accounts with starting balances.
- Transactions (sender → recipient, amount) are streamed through **Apache Kafka**.
- A **Kafka listener** consumes each transaction, validates it, and updates balances in a database.
- A **REST endpoint** allows querying any user's current balance by their ID.

The program is structured as five progressive tasks that build upon each other, guiding you from basic application setup all the way to a fully functional, event-driven transaction processing system.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3.2.5 |
| Messaging | Apache Kafka (Embedded Kafka for tests) |
| Persistence | Spring Data JPA + H2 (in-memory database) |
| HTTP Client | Spring RestTemplate |
| Build Tool | Maven (via Maven Wrapper `mvnw`) |
| Testing | JUnit 5, Spring Boot Test |
| Serialization | Jackson (`@JsonIgnoreProperties`) |

---

## Project Structure

```
Forage-Midas-flow/
├── src/
│   ├── main/
│   │   └── java/com/jpmc/midascore/
│   │       ├── MidasCoreApplication.java       # Spring Boot entry point
│   │       ├── component/
│   │       │   └── DatabaseConduit.java         # Service layer for DB operations
│   │       ├── entity/
│   │       │   └── UserRecord.java              # JPA entity: User (id, name, balance)
│   │       ├── foundation/
│   │       │   ├── Transaction.java             # DTO: Kafka transaction payload
│   │       │   └── Balance.java                 # DTO: REST API balance response
│   │       └── repository/
│   │           └── UserRepository.java          # Spring Data CrudRepository
│   └── test/
│       ├── java/com/jpmc/midascore/
│       │   ├── TaskOneTests.java                # Task 1: Application boot verification
│       │   ├── TaskTwoTests.java                # Task 2: Kafka consumer integration
│       │   ├── TaskThreeTests.java              # Task 3: Balance processing (Waldorf)
│       │   ├── TaskFourTests.java               # Task 4: Transaction validation (Wilbur)
│       │   ├── TaskFiveTests.java               # Task 5: REST API balance querying
│       │   ├── KafkaProducer.java               # Test helper: sends transactions to Kafka
│       │   ├── UserPopulator.java               # Test helper: seeds users into DB
│       │   ├── FileLoader.java                  # Test helper: reads test data files
│       │   └── BalanceQuerier.java              # Test helper: queries REST balance endpoint
│       └── resources/
│           └── test_data/                       # Encoded transaction & user data files
├── services/
│   └── transaction-incentive-api.jar            # External incentive service (Task 5)
├── application.yml                              # Spring Boot application configuration
├── pom.xml                                      # Maven project descriptor
└── mvnw / mvnw.cmd                              # Maven wrapper scripts
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Test Suite (Embedded)                  │
│  KafkaProducer ──► Kafka Topic ──► [Your Kafka Listener] │
│  UserPopulator ──────────────────► H2 Database           │
│  BalanceQuerier ◄──────────────── REST API /balance      │
└──────────────────────────────────────────────────────────┘
                            │
                    ┌───────▼───────┐
                    │  midas-core   │
                    │  Spring Boot  │
                    │  Application  │
                    └───────────────┘
                       │         │
               ┌───────┘         └────────┐
        ┌──────▼──────┐          ┌────────▼──────┐
        │  JPA / H2   │          │  REST API     │
        │  Database   │          │  /balance     │
        └─────────────┘          └───────────────┘
```

- **Kafka Listener**: Consumes `Transaction` objects (senderId, recipientId, amount) from the configured topic.
- **Business Logic**: Validates sender funds, deducts from sender, credits recipient.
- **Persistence**: Uses `UserRepository` (Spring Data) via `DatabaseConduit` to read/write `UserRecord` entities.
- **REST API**: Exposes `GET /balance?userId={id}` returning a `Balance` JSON object.
- **Transaction Incentive API**: An external JAR (`services/transaction-incentive-api.jar`) called during Task 5 to apply bonus incentives to transactions.

---

## Getting Started

### Prerequisites

| Requirement | Version |
|---|---|
| Java JDK | 17+ |
| Maven | Bundled via `mvnw` wrapper |
| IDE | IntelliJ IDEA (recommended) or VS Code |

> **No external Kafka installation needed** — tests use Spring's **Embedded Kafka** (`@EmbeddedKafka`).

### Running the Application

```bash
# Clone the repository
git clone <your-repo-url>
cd Forage-Midas-flow

# Build the project
./mvnw clean install -DskipTests

# Run the application
./mvnw spring-boot:run
```

On Windows:
```cmd
mvnw.cmd spring-boot:run
```

The application will start on the port configured in `application.yml` (default: `33400` based on the test suite).

### Running Tests

Run each task test individually to verify your implementation:

```bash
# Task 1 — Application boot
./mvnw test -Dtest=TaskOneTests

# Task 2 — Kafka consumer
./mvnw test -Dtest=TaskTwoTests

# Task 3 — Balance processing
./mvnw test -Dtest=TaskThreeTests

# Task 4 — Transaction validation
./mvnw test -Dtest=TaskFourTests

# Task 5 — REST API
./mvnw test -Dtest=TaskFiveTests
```

On Windows, replace `./mvnw` with `mvnw.cmd`.

---

## Task Breakdown

| Task | Goal | Key Implementation |
|---|---|---|
| **Task 1** | Boot the Spring Boot application successfully | Configure `application.yml`, resolve dependencies |
| **Task 2** | Consume transactions from a Kafka topic | Implement a `@KafkaListener` to receive `Transaction` objects |
| **Task 3** | Persist transactions to database & update balances | Use `UserRepository` to fetch users and update balances via `DatabaseConduit` |
| **Task 4** | Validate transactions (prevent negative balances) | Add guard logic to reject transactions where sender has insufficient funds |
| **Task 5** | Expose a REST API to query user balances | Implement `GET /balance?userId={id}` endpoint returning a `Balance` response + integrate the incentive API |

---

## API Reference

### `GET /balance`

Returns the current balance for a given user.

**Query Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `userId` | `long` | The ID of the user to query |

**Response:**

```json
{
  "amount": 1234.56
}
```

**Example:**
```bash
curl "http://localhost:33400/balance?userId=1"
```

---

## Key Components

### `Transaction.java`
DTO representing a Kafka message payload:
```java
{
  senderId:    long   // ID of the sending user
  recipientId: long   // ID of the receiving user
  amount:      float  // Amount to transfer
}
```

### `UserRecord.java`
JPA entity persisted in H2 (in-memory):
```java
{
  id:      long    // Auto-generated primary key
  name:    String  // User's name
  balance: float   // Current account balance
}
```

### `Balance.java`
REST response DTO:
```java
{
  amount: float  // Current balance amount
}
```

### `DatabaseConduit.java`
Spring component acting as a service layer over `UserRepository` — abstracts database save operations.

### `UserRepository.java`
Spring Data `CrudRepository` with a custom finder:
```java
UserRecord findById(long id);
```

---

## Program Context

This project is part of the **JPMorgan Chase & Co. Advanced Software Engineering** virtual experience on [Forage](https://www.theforage.com/). It simulates the kind of real-world, event-driven financial systems engineers work on at JPMC — covering Kafka-based transaction streaming, JPA persistence, REST API design, and incremental feature development guided by automated test verifiers.

---

*Built with ☕ Java & Spring Boot as part of the JPMC Forage Program.*
