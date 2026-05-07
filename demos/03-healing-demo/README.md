# Demo 03: Self-Healing Process

This demo shows Aqueduct's ability to automatically detect and fix blueprint errors using an LLM.

## Quick Start

1.  **Configure LLM**: Set your credentials so Aqueduct can talk to the LLM. 
    You need to set `AQUEDUCT_LLM_PROVIDER`, `AQUEDUCT_LLM_MODEL`, and your API key (`OPENAI_API_KEY` or `ANTHROPIC_API_KEY`).

    **Example (OpenAI)**:
    ```bash
    export AQUEDUCT_LLM_PROVIDER="openai_compat" AQUEDUCT_LLM_MODEL="gpt-4o" AQUEDUCT_LLM_BASE_URL="https://api.openai.com/v1" OPENAI_API_KEY="sk-..."
    ```


2.  **Run the demo**:

    ```bash
    aqueduct run blueprints/example.yml
    ```

3.  **Observe the Healing**:
    - The first run will fail because it tries to read a CSV file as Parquet.
    - Aqueduct will automatically ask the LLM for a fix.
    - The patch will be applied and the run will restart (and succeed) automatically.

## How it works
The blueprint `blueprints/example.yml` has `approval_mode: auto` enabled. When a module fails, Aqueduct captures the error, sends it to your configured LLM, and applies the suggested fix if it passes basic validation.

Check the `patches/` directory after the run to see the generated patch file.

