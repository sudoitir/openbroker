## MODIFIED Requirements

### Requirement: Kinds declare their capabilities and attributes
Every kind SHALL declare its verbs, its supported orderings and an attribute schema that marks each attribute as config, status or metric, with an instrument and unit for metrics. Every kind SHALL also carry its system's display label, singular and plural, in sentence case, so that a consumer never derives a label from the kind id.

#### Scenario: An ordering the kind does not support
- **WHEN** a consumer asks for a list ordered by a field the kind does not declare
- **THEN** the call fails with `INVALID_ARGUMENT`

#### Scenario: A kind names itself
- **WHEN** a consumer lists the kinds of any driver
- **THEN** every kind has a non-blank singular and plural label in the system's own words, and the TCK fails a driver whose kind has none

## ADDED Requirements

### Requirement: Credentials take the forms systems use
Credentials SHALL be one of a closed set: a username and password, a bearer token with its expiry, or none (as with mutual TLS). A credential source SHALL be asked for the current credentials on every (re)connect. A driver SHALL refuse a form it cannot use with `INVALID_ARGUMENT`, naming the forms it accepts, and never reveal the secret.

#### Scenario: A token where a password is expected
- **WHEN** a consumer connects with a bearer token to a driver that accepts only a username and password
- **THEN** the connection fails with `INVALID_ARGUMENT` naming the accepted forms, and the token appears in no message

#### Scenario: A token is refreshed
- **WHEN** a credential source returns a new bearer token before the old one expires
- **THEN** the next connection to a broker uses the new token without closing open links
