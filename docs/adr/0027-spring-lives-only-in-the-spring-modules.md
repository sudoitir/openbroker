# ADR-0027: Spring lives only in the Spring modules

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

OpenBroker is framework-free: `openbroker-api` depends on nothing but JSpecify, and drivers use no DI container.
A Spring Boot starter brings Spring into the reactor, and a careless dependency could pull it into the API or a driver,
forcing it on every adopter.

## Decision

Spring appears only in `openbroker-spring-boot-autoconfigure` and `openbroker-spring-boot-starter`. The parent pom's
Maven Enforcer `bannedDependencies` rule bans `org.springframework*` everywhere else, and the autoconfigure module
imports Spring Boot's dependency management in its own pom, never in the parent. A build profile that adds Spring to
`openbroker-api` proves the rule fails the build.

## Consequences

- Adopters without Spring see no Spring artifacts or versions.
- Spring Boot's versions do not leak into the other modules' dependency management.
- A future framework integration (Quarkus, Micronaut) follows the same pattern in its own modules.

## Alternatives considered

- A review-only convention: one missed review puts Spring into the API.
- Importing Spring Boot's BOM in the parent: every module would inherit Spring's versions for shared libraries.
