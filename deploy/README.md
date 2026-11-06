# Running the gateways with TLS

The gateway refuses to start without an environment-provided bearer token and a
readable TLS certificate/private-key pair. Supply `KAFKA3O_GATEWAY_TOKEN` as a
securely generated random bearer token of at least 32 characters. Token length
alone does not ensure randomness; never commit a token, private key, or a file
containing them. Every holder of a gateway token can perform all its read and
write operations; there are no per-user roles.

The token also authorizes Kafka broker/topic configuration changes, partition
increases, and replica reassignment where the Kafka principal has permission.
Partition increases are irreversible and reassignments can move substantial
data or change durability. Treat the gateway as an administrative endpoint.
Sensitive and restart-required configuration is not editable through it.

Set `KAFKA3O_HTTP__TLS__CERTIFICATEFILE` and `KAFKA3O_HTTP__TLS__KEYFILE` to
paths readable by the gateway process. The certificate must match the hostname
in the UI's gateway URL and be trusted by the user's browser. For local
development, use a certificate for `localhost` signed by a locally trusted CA.
Gateway TLS does not secure the gateway's separate connection to Kafka.

## Securing the Kafka connection

The Kafka connection remains plaintext by default for existing deployments.
To use TLS, set `kafka.tls.enabled: true` or
`KAFKA3O_KAFKA__TLS__ENABLED=true`. The system trust store is used unless
`kafka.tls.caFile` or `KAFKA3O_KAFKA__TLS__CAFILE` names a PEM CA bundle; a
configured bundle replaces, rather than adds to, system roots. Every broker's
advertised hostname or IP must match its certificate. Certificate and hostname
verification cannot be disabled.

For SASL, set `kafka.sasl.mechanism` to `PLAIN`, `SCRAM-SHA-256`, or
`SCRAM-SHA-512`, and set `kafka.sasl.username`. These keys can also be supplied
as `KAFKA3O_KAFKA__SASL__MECHANISM` and
`KAFKA3O_KAFKA__SASL__USERNAME`. Supply the password only as
`KAFKA3O_KAFKA_SASL_PASSWORD`, not in YAML or the nested environment key
`KAFKA3O_KAFKA__SASL__PASSWORD`. The gateway rejects incomplete or invalid
security configuration rather than silently falling back to plaintext.

TLS and SASL can be enabled independently, but always use TLS on an untrusted
network: SASL PLAIN without TLS exposes the password, and SASL alone does not
encrypt Kafka data. Mutual TLS client certificates are not currently supported.
Configure secrets using your deployment platform's secret management; process
environments and mounted files may be visible to administrators.

## Persistent EKS-D test brokers

For the two long-lived, secured single-broker test clusters on `eksd-01`, see
the [persistent Kafka test guide](kubernetes/test-kafka/README.md). It covers
internal and SSH-forwarded access, read-only checks, and important storage and
credential limitations.

## Two-gateway Compose demo

For the two-gateway Compose demo:

1. Provision the trusted certificate and private key on the **Docker host**.
   The distroless gateway runs as nonroot UID 65532; arrange narrowly scoped
   permissions so that user can read the key.
2. Set `KAFKA3O_TLS_CERT_FILE` and `KAFKA3O_TLS_KEY_FILE` to absolute paths on
   that host. Compose bind-mounts the files read-only. For a remote Docker
   daemon, these paths refer to the remote host, not the machine running the
   Compose command. Do not mount a private key into unrelated containers.
3. Generate separate random tokens for `KAFKA3O_DEV_TOKEN` and
   `KAFKA3O_PROD_TOKEN` in the Compose command's environment. Keep them out of
   source files, command history, output, and logs. Docker administrators can
   inspect container environments and mounted files.
4. Start with `docker compose -f deploy/compose.dev.yaml up -d --build`. Run
   the UI on `http://localhost:5173` or a trusted HTTPS origin and configure its
   allowed origin in the gateway's CORS settings if different.
5. In the UI, open **Gateway credentials** and enter each gateway's token.
   The UI stores tokens in browser session storage, not in `config.json` or
   URLs. Session storage is accessible to scripts running in that origin, so
   protect the UI from XSS and serve it only from a trusted host.

Certificate trust errors normally appear in browsers as a gateway connection
failure. Check the URL hostname, certificate chain and trust store, and ensure
the gateway and UI use HTTPS as described above.
