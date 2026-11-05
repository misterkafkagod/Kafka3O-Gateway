# Running the gateways with TLS

The gateway refuses to start without an environment-provided bearer token and a
readable TLS certificate/private-key pair. Supply `KAFKA3O_GATEWAY_TOKEN` as a
securely generated random bearer token of at least 32 characters. Token length
alone does not ensure randomness; never commit a token, private key, or a file
containing them. Every holder of a gateway token can perform all its read and
write operations; there are no per-user roles.

Set `KAFKA3O_HTTP__TLS__CERTIFICATEFILE` and `KAFKA3O_HTTP__TLS__KEYFILE` to
paths readable by the gateway process. The certificate must match the hostname
in the UI's gateway URL and be trusted by the user's browser. For local
development, use a certificate for `localhost` signed by a locally trusted CA.
Gateway TLS does not secure the gateway's separate connection to Kafka; Kafka
TLS/SASL support is still planned.

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
