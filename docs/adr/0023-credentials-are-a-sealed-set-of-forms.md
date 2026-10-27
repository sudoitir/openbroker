# ADR-0023: Credentials are a sealed set of forms

- **Status**: accepted
- **Date**: 2026-10-27
- **Deciders**: Mahdi Amirabdollahi

## Context

`Credentials` was a username and a password, with an empty pair standing for "none". Systems
authenticate in more ways: a bearer token that expires (OAuth), a signed request (SigV4), a client
certificate, or nothing. A username and a password cannot carry a token and its expiry, and an empty
string as a sentinel is a trap.

## Decision

- `Credentials` is a sealed interface with three forms:
  - `UsernamePassword(String username, Secret password)`;
  - `BearerToken(Secret token, Instant expiresAt)`, where `Instant.MAX` is a token that does not expire;
  - `None`, which replaces the empty pair, `none()` and `isNone()`. Mutual TLS is `None` with a
    `TlsProvider`.
- A `CredentialSource` is still asked on every connect and reconnect, so a rotated secret or a fresh
  token takes effect without a restart.
- A driver handles the forms it supports and refuses the others with `INVALID_ARGUMENT`, naming the
  forms it accepts, before anything is sent. Artemis accepts `UsernamePassword` and `None`, and
  switches on the form in one place.
- No form's `toString()` shows a secret.
- Request signing waits until a driver needs it; the sealed set grows then, which is a breaking change
  allowed in `0.x`.

## Consequences

- A driver's authentication is an exhaustive `switch`, so a new form cannot be forgotten.
- Adopters of `0.2.0` write `Credentials.usernamePassword(...)` and `new Credentials.None()`.

## Alternatives considered

- **A bag of strings.** No type says which keys a driver reads, and secrets could slip into plain text.
- **One record with optional fields.** Every combination is legal to build and most are meaningless.
- **An open interface.** A driver could not switch exhaustively, and a consumer could pass a form no
  driver knows.
