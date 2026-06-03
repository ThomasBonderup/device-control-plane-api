# Combotto Control Plane API — Architecture

## Overview

Combotto Control Plane API is a Java 21 + Spring Boot backend for managing operational state around the Combotto audit platform.

It acts as the **control plane** for inventory, relationships, and workflow-oriented APIs, while the existing Scala audit engine remains responsible for technical audit execution, evidence collection, findings generation, and remediation output.

The current focus of the control plane is:

- certificate inventory
- asset inventory
- certificate-to-asset bindings
- operational summary views

---

## System responsibilities

The control plane is responsible for managing persistent operational state and exposing APIs that support day-to-day platform usage.

### This service owns

- certificates
- assets
- certificate bindings
- certificate filtering and summary views
- operational metadata such as renewal status and ownership
- relational queries over inventory state

### This service does not own

- audit execution
- evidence collection
- TLS probe execution
- findings generation
- remediation backlog generation

Those responsibilities remain with the Scala audit engine and Rust probe components.

---

## Service boundaries

Combotto is intentionally split into two conceptual planes.

### Control plane (this service)

Implemented in Java + Spring Boot.

Responsibilities:

- inventory management
- resource relationships
- workflow-oriented APIs
- operational summaries
- future user-facing and customer-facing platform endpoints

### Execution plane (existing Scala audit engine)

Implemented separately in Scala.

Responsibilities:

- running audits
- evaluating technical rules
- collecting and processing evidence
- generating findings and remediation output

This separation keeps operational workflow concerns distinct from audit execution logic.

---

## Domain model

The current domain model centers around three main entities.

### Certificate

Represents a certificate tracked in inventory.

Key fields include:

- tenantId
- name
- commonName
- issuer
- serialNumber
- sha256Fingerprint
- notBefore / notAfter
- status
- renewalStatus
- owner

### Asset

Represents a managed platform asset owned by Combotto Monitor.

Key fields include:

- id
- companyId
- assetType
- name
- externalRef
- parentAssetId
- serialNumber / hardwareModel
- protocol
- siteLabel
- metadataJson
- isDeleted

Firmware state is owned by `control_plane.device_firmware_state`; asset responses do not read or expose `public.assets.firmware_version`.

### CertificateBinding

Represents the relationship between a certificate and an asset.

A binding exists because the relationship itself carries meaning, such as:

- binding type
- endpoint
- port
- creation time

This is modeled as a first-class entity rather than a simple many-to-many join.

---

## Application structure

The service currently follows a layered Spring Boot structure.

```text
src/main/java/com/combotto/controlplane/
  api/         # request and response DTOs
  controller/  # REST controllers
  service/     # application/service layer
  repository/  # Spring Data JPA repositories
  model/       # JPA entities and enums
  common/      # shared exceptions and error handling
```

### Layer responsibilities

- **Controller layer**

  - receives HTTP requests
  - validates and maps request input
  - delegates to services
  - returns HTTP responses

- **Service layer**

  - contains application logic
  - coordinates repositories
  - enforces resource existence and workflow rules
  - maps entities to API responses

- **Repository layer**

  - provides persistence access through Spring Data JPA
  - uses derived queries and JPQL where needed

- **Model layer**
  - defines JPA entities and enums
  - represents persistent state

### Request flow

A typical request follows this path:

1. client sends HTTP request
2. Spring MVC routes request to controller
3. controller validates input and delegates to service
4. service loads or mutates entities through repositories
5. repository interacts with PostgreSQL via JPA/Hibernate
6. service maps entities to response DTOs
7. controller returns response to client

### Example: create certificate binding

`POST /api/certificates/{id}/bindings`

1. controller receives `certificateId` and request body
2. service loads certificate and asset
3. service creates `CertificateBindingEntity`
4. repository persists the binding
5. service returns `CertificateBindingResponse`
6. controller returns `201 Created`

---

## Persistence model

The service uses PostgreSQL as the system of record.

### Schema management

- Flyway owns schema evolution for the `control_plane` schema
- Hibernate validates schema at startup
- `ddl-auto` is set to `validate`
- Combotto Monitor owns `public.assets`; this service only references it

### Current tables

- `control_plane.certificates`
- `control_plane.certificate_bindings`
- `control_plane.certificate_renewal_status_history`
- `public.assets` (external, owned by Combotto Monitor)

### Relationship model

`certificate_bindings` references both:

- `control_plane.certificates(id)`
- `public.assets(id)`

This allows one certificate to be used by multiple assets and one asset to reference multiple certificates.

---

## Current API surface

### Certificates

- `POST /api/certificates`
- `GET /api/certificates`
- `GET /api/certificates/{id}`
- `PATCH /api/certificates/{id}`
- `DELETE /api/certificates/{id}`

### Assets

- `GET /api/assets`
- `GET /api/assets/{id}`
- `GET /api/assets/{id}/bindings`
- `GET /api/assets/{id}/certificates`

### Certificate bindings

- `POST /api/certificates/{id}/bindings`
- `GET /api/certificates/{id}/bindings`

### Operational views

- `GET /api/certificates/summary`

---

## Planned evolution

The current implementation focuses on the control-plane foundation.

Planned next steps include:

- reverse lookup from asset to bindings/certificates
- richer operational views (expiring soon, owner assignment, renewal workflow)
- pagination and sorting
- JWT authentication and authorization
- audit run orchestration and history
- integration with Scala audit engine outputs
- event-driven integration patterns where appropriate

Kafka or eventing is intentionally deferred until there is a clear product need for asynchronous workflows or streaming projections.

---

## Design principles

### Clear service boundaries

Keep operational workflow logic in the control plane and technical audit execution in the Scala engine.

### Keep the domain explicit

Model relationships such as certificate bindings as first-class concepts.

### Start simple, evolve intentionally

Use a modular monolith approach first. Introduce more advanced patterns only when real product needs justify them.

### Prefer real persistence-backed tests

Integration tests with Spring Boot, MockMvc, and Testcontainers are preferred over mock-heavy tests for core API slices.

### Use the database as the source of truth

PostgreSQL is the primary source of truth for control-plane state. Future messaging or projections should build on top of this rather than replace it prematurely.

---

### 1. System boundary diagram

### 2. Request flow diagram
