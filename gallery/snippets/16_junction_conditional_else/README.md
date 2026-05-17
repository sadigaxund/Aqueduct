# Junction with Else Branch

Demonstrates how to split a data stream into multiple branches using conditional logic, including a "catch-all" branch.

## Key Concept: Junction
A **Junction** is a fan-out module that takes one input and produces multiple named outputs (ports). 

### The `_else_` keyword
In `conditional` mode, the `_else_` keyword is a special constant that matches all rows that were **not** captured by any other branch. 
- It prevents data loss by ensuring every row has a destination.
- It simplifies logic by removing the need to manually write the complement of all other filters.


## How to Run

1. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

2. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

## Configuration
In `blueprint.yml`:
```yaml
- id: route_tickets
  type: Junction
  config:
    mode: conditional
    branches:
      - id: active
        condition: "status IN ('NEW', 'PENDING')"
      - id: other
        condition: "_else_"
```

The `edges` must specify the `port` corresponding to the branch `id`:
```yaml
- from: route_tickets
  to: save_other
  port: other
```
