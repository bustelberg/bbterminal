from __future__ import annotations

from quick_insight.web.db import q


def main() -> None:
    df = q(
        """
        SELECT DISTINCT metric_code
        FROM metric
        ORDER BY metric_code
        """
    )

    if df.empty:
        print("No metrics found.")
        return

    metrics = df["metric_code"].astype(str).tolist()

    print(f"\nFound {len(metrics)} unique metrics:\n")

    for m in metrics:
        print(m)


if __name__ == "__main__":
    main()
