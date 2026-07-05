# Secret Injection — ${VAR:-default} / @aq.secret()

Demonstrates injecting secrets into Blueprints without hardcoding them in YAML.

## How it works

This snippet uses **YAML-level env resolution** (`${PIPELINE_SALT:-demo_salt}`)
so it runs out of the box. For real secret injection, use `@aq.secret('KEY')`
which resolves via the configured resolver (default: environment variables) at
compile time and is **automatically redacted** from logs and the observability
store.

## Providers

```yaml
# Default (env): reads os.environ['KEY']
@aq.secret('DB_PASSWORD')     → os.environ['DB_PASSWORD']

# AWS Secrets Manager (requires boto3)
@aq.secret('my-service/password')

# GCP Secret Manager
@aq.secret('MY_SECRET')

# Azure Key Vault
@aq.secret('my-secret-name')
```

Set the provider in `aqueduct.yml`:
```yaml
secrets:
  provider: aws
```

## How to Run

```bash
# Works without any env var (falls back to "demo_salt"):
aqueduct run blueprint.yml
python3 inspect_results.py

# For real secret injection, set PIPELINE_SALT and use @aq.secret:
export PIPELINE_SALT=my_secret_salt_value
```
