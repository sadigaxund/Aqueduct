# Logic & Architecture Pattern Samples

These samples show how to combine multiple modules to achieve specific architectural goals.

## Error Handling
- [ ] `dead_letter_office.yml`: A pattern for capturing and storing all row-level failures across the pipeline.
- [ ] `partial_success_retry.yml`: Configuring retry policies that ignore specific transient error types.

## Reusability
- [ ] `arcade_nested_context.yml`: Passing dynamic context through multiple layers of Arcades.
- [ ] `shared_udf_package.yml`: Best practice for organizing and importing a shared UDF library.

## State Management
- [ ] `idempotent_backfill.yml`: Using `@aq.date` and context profiles to run historical slices.
- [ ] `watermark_progression.yml`: Reading a high-watermark from the Depot and updating it after an Egress.

## Advanced LLM Loops
- [ ] `agent_custom_prompt.yml`: Customizing the system prompt for the self-healing agent.
- [ ] `automated_patch_review.yml`: Using a Regulator to gate the application of LLM-generated patches.

## Security
- [ ] `secret_fallback.yml`: Using `@aq.secret` with a local environment variable fallback for CI/CD.
