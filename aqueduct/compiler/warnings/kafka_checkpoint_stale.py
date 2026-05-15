"""Channel with `checkpoint: true` fed by a Kafka Ingress.

Kafka is a moving target — each Spark micro-batch sees newer data. A cache /
checkpoint freezes the upstream snapshot for a second consumer, so the second
consumer reads stale data without realising it. The user almost certainly
wants the live stream on both branches.
"""

from __future__ import annotations

from typing import Any

RULE_ID = "kafka_checkpoint_stale"


def check(manifest: Any) -> list[str]:
    out: list[str] = []
    by_id = {m.id: m for m in manifest.modules}

    # Map module → list of upstream (main-port) module IDs
    upstream: dict[str, list[str]] = {}
    for e in manifest.edges:
        if getattr(e, "port", "main") != "main":
            continue
        upstream.setdefault(e.to_id, []).append(e.from_id)

    for m in manifest.modules:
        if m.type != "Channel":
            continue
        if not getattr(m, "checkpoint", False):
            continue
        for up_id in upstream.get(m.id, []):
            up = by_id.get(up_id)
            if up is None or up.type != "Ingress":
                continue
            fmt = (up.config or {}).get("format", "")
            if fmt == "kafka":
                out.append(
                    f"Channel '{m.id}' has checkpoint=true but its upstream "
                    f"Ingress '{up_id}' uses format=kafka. The checkpoint will "
                    "freeze a single micro-batch for downstream consumers — "
                    "they will see stale stream data. Consider removing the "
                    "checkpoint or running each consumer as a separate "
                    "streaming job."
                )
    return out
