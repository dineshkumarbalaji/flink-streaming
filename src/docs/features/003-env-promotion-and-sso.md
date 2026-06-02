# Feature Document: Environment Promotion and SSO Security

**Document Version:** 1.1  
**Status:** Propopsed / Planned for Next Major Release  
**Related Components:** `JobController`, UI, Config Templating, Security Module

## 1. Overview
This feature introduces two major architectural enhancements to the Flink Streaming Framework:
1. **Environment Promotion Lifecycle (Dev -> SIT -> UAT -> Prod):** Establishing a robust deployment workflow where jobs are authored and stabilized in DEV, exported, and flawlessly deployed to higher environments.
2. **UI Security & Authentication:** Securing the UI dashboard and REST APIs using Single Sign-On (SSO) with standard OAuth2 / OIDC logic to manage user credentials and application access roles.

---

## 2. Environment Promotion (DEV -> SIT -> UAT -> PROD)

### 2.1 Context
Currently, jobs submitted through the UI are saved as plain JSON config files in the `configs/` directory. Deploying these locally saved jobs to production securely and reliably is necessary for governance. 

### 2.2 Proposed Implementation

The promotion lifecycle will be an **Export/Import configuration workflow**, avoiding direct, manual job creation in production systems.

* **Configuration Variable Extraction:** Job configurations (`<jobName>.json`) will support environment variables and placeholders (e.g., `${KAFKA_BOOTSTRAP_SERVERS}`). 
* **DEV Interface (Author & Stabilize):** The DEV UI remains unchanged, enabling developers to build, validate, and stabilize new Flink streaming jobs.
* **Export Feature:** A new UI capability allows users to export a job's stabilized configuration to a local JSON file. 
* **Import & Deploy Feature (SIT/UAT/PROD):** The higher environment UIs will include a "Deploy via Config" form where users upload their `.json` config.
* **Property Interpolation:** The `JobController` will automatically perform placeholder substitution. For instance, an imported job with `${KAFKA_BOOTSTRAP_SERVERS}` will safely inherit the `prod-kafka:9092` broker strings dictated by the static `application.yml` deployed out of band in the respective environment. 
* **Immutability:** To enforce deployment governance, the "Create Job" form from scratch can optionally be disabled in Prod.

---

## 3. UI Login & SSO Security Setup

### 3.1 Context
As environments escalate to Prod, the REST endpoints exposing `/api/jobs/*` and the UI need authenticated access control.

### 3.2 Proposed Implementation

Security will be driven directly via **Spring Security (OAuth2)** to proxy user logins to your organization's Identity Provider (IdP).

* **Dependencies Context:** `spring-boot-starter-security` and `spring-boot-starter-oauth2-client` will be added to the project's POM.
* **SSO Identity Provider Implementation:** The framework will act as an OAuth2 Relying Party enforcing users to sign in. The explicit backend IdP limits anonymous API usage effectively.
* **Roles (RBAC):** SSO groups returned within access tokens can route securely into Role-Based Access Controls handling authorizations:
  - `ROLE_DEVELOPER`: Creation permissions mapped to DEV instances.
  - `ROLE_DEPLOYER`: Validated deployments capabilities limited strictly to imports inside SIT/UAT/PROD instances.
* **Audit Tracing Improvements:** Passing authenticated user details out of the `SecurityContext` directly to the `StreamingJobOrchestrator` allows tracking `user_xyz` to job creations and cancellations.

## 4. Dependencies & Impact
* Requires Spring Security and UI state alterations.
* Minor changes to `application.yml` structure required per-environment.
* Adjustments parsing job `Config` mapping in Flink `JobController`. 

## 5. Next Steps
Once the previous version (v1.1) is entirely stabilized under load, we will refer back to this document to kick off development, beginning with integrating the Spring Security OAuth dependency layer.
