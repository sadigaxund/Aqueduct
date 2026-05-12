# ISSUE-001: Spark Hadoop Configuration Propagation Failure

## Status
- **Type**: Bug / Architectural Gap
- **Severity**: High (Blocks S3/GCS/Minio connectivity)
- **Phase**: 19 (Gallery Population)
- **Found by**: User testing `02_ingress_parquet_s3` snippet

## Description
When using the `Ingress` module with `s3a://` or other Hadoop-compatible file systems, passing configuration via `options` in the module config is insufficient for the `S3AFileSystem` and other Hadoop-level implementations. 

Spark's `DataFrameReader.option()` does not consistently propagate `fs.s3a.*` or other Hadoop properties to the underlying `hadoopConfiguration` used by the JVM's FileSystem objects. This results in errors like:
`org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException: SimpleAWSCredentialsProvider: No AWS credentials in the Hadoop configuration`
even when valid credentials are provided in the YAML `options`.

## Root Cause
The `aqueduct.executor.spark.ingress.read_ingress` function only applies options to the `DataFrameReader`. For certain storage backends, these MUST be set on the `sparkContext.hadoopConfiguration()` to be effective.

## Proposed Fix
Modify `aqueduct/executor/spark/ingress.py` to propagate specific Hadoop-related keys to the `hadoopConfiguration` before the `load()` call.

```python
    h_conf = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in cfg.get("options", {}).items():
        s_key = str(key)
        s_val = str(value)
        reader = reader.option(s_key, s_val)
        
        # Propagate Hadoop-specific options
        if s_key.startswith(("fs.s3a.", "fs.gs.", "fs.azure.", "spark.hadoop.")):
            actual_key = s_key.replace("spark.hadoop.", "")
            h_conf.set(actual_key, s_val)
```

## Workaround
Users must set these options in the top-level `spark_config` block of the blueprint using the `spark.hadoop.` prefix:
```yaml
spark_config:
  spark.hadoop.fs.s3a.access.key: "..."
  spark.hadoop.fs.s3a.secret.key: "..."
```
Note: This workaround applies settings globally to the Spark session, which may be undesirable in multi-tenant environments.
