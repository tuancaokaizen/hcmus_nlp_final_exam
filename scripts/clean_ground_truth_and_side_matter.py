#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI batch cleaner for ground_truth / side_matter (Excel/CSV/JSONL).

Công cụ batch làm sạch GT/SM — core nằm ở common.fen_gt_clean.
Exam default: keep Han punctuation (，。！？).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_JOBS = Path(__file__).resolve().parents[1] / "dags" / "jobs"
if str(_JOBS) not in sys.path:
    sys.path.insert(0, str(_JOBS))

from common.fen_gt_clean import clean_ground_truth, clean_side_matter  # noqa: E402

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None  # type: ignore


def process_dataset_file(
    input_path: str,
    output_path: str | None = None,
    gt_col: str = "ground_truth",
    sm_col: str = "side_matter",
    *,
    keep_han_punct: bool = True,
) -> bool:
    """Clean GT/SM columns in a dataset file / Làm sạch cột GT/SM trong file dataset."""
    if pd is None:
        print("[ERR] pandas required for CLI batch clean", file=sys.stderr)
        return False
    if not os.path.exists(input_path):
        print(f"[ERR] missing input: {input_path}", file=sys.stderr)
        return False

    ext = os.path.splitext(input_path)[1].lower()
    if ext == ".jsonl":
        records = []
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
        df = pd.DataFrame(records)
    elif ext == ".csv":
        df = pd.read_csv(input_path)
    else:
        df = pd.read_excel(input_path)

    actual_gt = next((c for c in (gt_col, "clean_main_ground_truth") if c in df.columns), None)
    actual_sm = next(
        (c for c in (sm_col, "clean_side_matter_ground_truth") if c in df.columns), None
    )
    cleaned_gt: list[str] = []
    cleaned_sm: list[str] = []
    for _, row in df.iterrows():
        raw_gt = str(row[actual_gt]) if actual_gt and pd.notna(row[actual_gt]) else ""
        c_gt, _ = clean_ground_truth(raw_gt, keep_han_punct=keep_han_punct)
        cleaned_gt.append(c_gt)
        raw_sm = str(row[actual_sm]) if actual_sm and pd.notna(row[actual_sm]) else ""
        cleaned_sm.append(clean_side_matter(raw_sm))

    if actual_gt:
        df[actual_gt] = cleaned_gt
    if actual_sm:
        df[actual_sm] = cleaned_sm

    if not output_path:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_cleaned{ext}"
    out_ext = os.path.splitext(output_path)[1].lower()
    if out_ext == ".jsonl":
        with open(output_path, "w", encoding="utf-8") as f:
            for rec in df.to_dict(orient="records"):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    elif out_ext == ".csv":
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
    else:
        df.to_excel(output_path, index=False)
    print(f"[ok] wrote {len(df)} rows → {output_path}")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean ground_truth / side_matter columns")
    parser.add_argument("--input", "-i", required=True)
    parser.add_argument("--output", "-o", default=None)
    parser.add_argument("--gt-col", default="ground_truth")
    parser.add_argument("--sm-col", default="side_matter")
    parser.add_argument(
        "--strip-han-punct",
        action="store_true",
        help="Also strip Chinese punctuation (legacy hard mode)",
    )
    args = parser.parse_args()
    ok = process_dataset_file(
        args.input,
        args.output,
        args.gt_col,
        args.sm_col,
        keep_han_punct=not args.strip_han_punct,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
