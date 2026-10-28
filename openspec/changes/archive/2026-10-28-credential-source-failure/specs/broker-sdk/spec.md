## MODIFIED Requirements

### Requirement: Credentials take the forms systems use
Credentials SHALL be one of a closed set: a username and password, a bearer token with its expiry, or none (as with mutual TLS). A credential source SHALL be asked for the current credentials on every (re)connect. A driver SHALL refuse a form it cannot use with `INVALID_ARGUMENT`, naming the forms it accepts, and never reveal the secret. A credential source that fails with anything other than a `BrokerException` SHALL reach the driver as `UNAVAILABLE`, not sent, naming the plane and never the failure's text, so the driver backs off and asks again instead of losing the thread that asked.

#### Scenario: A token where a password is expected
- **WHEN** a consumer connects with a bearer token to a driver that accepts only a username and password
- **THEN** the connection fails with `INVALID_ARGUMENT` naming the accepted forms, and the token appears in no message

#### Scenario: A token is refreshed
- **WHEN** a credential source returns a new bearer token before the old one expires
- **THEN** the next connection to a broker uses the new token without closing open links

#### Scenario: A credential source fails
- **WHEN** a credential source throws an exception that is not a `BrokerException` while a driver reconnects a link
- **THEN** the driver sees `UNAVAILABLE`, not sent, without the exception's text, and backs off and retries the link
