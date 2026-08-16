import yaml
from rich.console import Console

console = Console()


def main():
    with open("blueprint.yml") as f:
        bp = yaml.safe_load(f)

    enrich = next(m for m in bp["modules"] if m["id"] == "enrich")
    query = enrich["config"]["query"]

    console.print("[bold cyan]Current 'enrich' query:[/bold cyan]")
    console.print(f"  {query}\n")

    if "total_amt" in query:
        console.print(
            "[bold green]✓[/bold green] The valid patch "
            "(sample_patches/03_valid_fix.json) was applied: 'total' was "
            "corrected to 'total_amt'."
        )
    else:
        console.print(
            "[bold yellow]⚠[/bold yellow] The query still references the "
            "non-existent 'total' column. Apply sample_patches/03_valid_fix.json:\n"
            "  aqueduct patch apply sample_patches/03_valid_fix.json --blueprint blueprint.yml\n"
            "  aqueduct patch commit --blueprint blueprint.yml"
        )

    output = next(m for m in bp["modules"] if m["id"] == "output")
    console.print(f"\n[bold cyan]Current 'output' path:[/bold cyan] {output['config']['path']}")
    console.print(
        "[dim]If sample_patches/02_disallowed_path.json had been let through, "
        "this would read '/etc/aqueduct_exfil.parquet' instead. It never "
        "gets this far because agent.guardrails.allowed_paths rejects it "
        "before the Blueprint file is ever touched.[/dim]"
    )


if __name__ == "__main__":
    main()
