# README media assets

Drop the README's demo assets here, then uncomment the matching `<img>` blocks in
the root `README.md` (and delete the "coming soon" captions).

## Expected files

| Path | What | How |
|---|---|---|
| `heal-loop.gif` | Hero demo — `aqueduct run` → schema-drift ✗ → agent patch → gates → re-run ✓ | `asciinema rec heal-loop.cast` then `agg heal-loop.cast docs/media/heal-loop.gif` · ~20s, looping, no narration |
| `dashboard/runs.png` | Runs tab → a run selected, module-metrics detail | screenshot (static PNG) |
| `dashboard/lineage.png` | Lineage tab → column-lineage Sankey + SQL changelog | screenshot |
| `dashboard/healing.png` | Healing → Patches → a patch's before/after diff | screenshot |
| `dashboard/performance.png` | Performance tab → cost-vs-data + plan complexity | screenshot |

Static PNGs for the dashboard (dense tables/charts read sharper than a GIF).
GIF only for the CLI heal loop (that's where the motion is the point).

Full video walkthrough: host on YouTube / asciinema and link from the README —
don't embed (GitHub won't autoplay and it bloats the repo).
