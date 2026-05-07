# Demo 03: Self-Healing Process

This demo illustrates Aqueduct's ability to automatically detect and fix blueprint errors using an LLM.

## Scenario
The blueprint `blueprints/example.yml` is configured to read `data/users.csv` as a **Parquet** file. This will cause a Spark exception during execution.

## How to Run

1.  **Configure your LLM**:
    Set your environment variables for your preferred provider (Anthropic, OpenAI, or Ollama):
    ```bash
    export AQUEDUCT_LLM_PROVIDER="openai"
    export AQUEDUCT_LLM_MODEL="gpt-4o"
    export AQUEDUCT_LLM_BASE_URL="https://api.openai.com/v1"
    export OPENAI_API_KEY="your-key-here"
    ```

2.  **Run the blueprint**:
    ```bash
    aqueduct run blueprints/example.yml
    ```

3.  **Observe the Healing**:
    - The first run will fail with a Spark error: `NOT_A_PARQUET_FILE`.
    - Aqueduct will capture the failure context and send it to the LLM.
    - The LLM will generate a patch to change `format: parquet` to `format: csv`.
    - Because `approval_mode: auto` is set in the blueprint, the patch will be applied and the run will be restarted automatically.
    - The second run should succeed.

4.  **Verify the Patch**:
    - Check the `patches/` directory to see the generated patch.
    - Check `blueprints/example.yml` to see the applied fix.
    - View the run history:
      ```bash
      aqueduct runs
      ```
