from typing import List, Dict, Any
from collections import defaultdict
import csv
import os

class EntityTableCSVExporter:
    """
    Export predicted NER entities into a CSV file.

    Input spans format:
      [
        {"start": 0, "end": 9, "label": "ORG", "text": "ACME Corp"},
        {"start": 25, "end": 41, "label": "INCIDENT", "text": "security incident"}
      ]

    Output CSV:
      ORG,INCIDENT
      ACME Corp,security incident
    """

    # -----------------------------------------------------
    # Public API
    # -----------------------------------------------------
    def export(
        self,
        pred_spans: List[Dict[str, Any]],
        output_file: str,
        *,
        sort_by_text_position: bool = True,
        unique: bool = False,
        delimiter: str = ",",
        include_header: bool = True,
        create_dirs: bool = True,
    ) -> str:
        """
        Export predicted spans as a CSV file.

        Args:
            pred_spans: List of predicted span dicts
            output_file: Path where CSV will be written
            sort_by_text_position: Keep spans ordered by appearance
            unique: Deduplicate identical texts per label
            delimiter: CSV delimiter (default ',')
            include_header: Include header row
            create_dirs: Create parent directories if missing

        Returns:
            Path to the written CSV file
        """
        if create_dirs:
            os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)

        grouped = self._group_by_label(pred_spans)

        if sort_by_text_position:
            for lab in grouped:
                grouped[lab].sort(key=lambda x: x["start"])

        if unique:
            grouped = self._deduplicate(grouped)

        columns, rows = self._build_table(grouped)

        self._write_csv(
            output_file=output_file,
            columns=columns,
            rows=rows,
            delimiter=delimiter,
            include_header=include_header,
        )

        return output_file

    # -----------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------
    @staticmethod
    def _group_by_label(
        spans: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        grouped = defaultdict(list)
        for sp in spans:
            label = sp.get("label")
            text = sp.get("text")
            if label and text:
                grouped[label].append(sp)
        return dict(grouped)

    @staticmethod
    def _deduplicate(
        grouped: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        out = {}
        for lab, items in grouped.items():
            seen = set()
            uniq = []
            for sp in items:
                if sp["text"] not in seen:
                    uniq.append(sp)
                    seen.add(sp["text"])
            out[lab] = uniq
        return out

    @staticmethod
    def _build_table(
        grouped: Dict[str, List[Dict[str, Any]]]
    ):
        columns = sorted(grouped.keys())
        max_rows = max((len(v) for v in grouped.values()), default=0)

        rows = []
        for i in range(max_rows):
            row = {}
            for col in columns:
                row[col] = grouped[col][i]["text"] if i < len(grouped[col]) else ""
            rows.append(row)

        return columns, rows

    @staticmethod
    def _write_csv(
        output_file: str,
        columns: List[str],
        rows: List[Dict[str, str]],
        delimiter: str,
        include_header: bool,
    ):
        with open(output_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=columns,
                delimiter=delimiter,
                quoting=csv.QUOTE_MINIMAL,
            )

            if include_header:
                writer.writeheader()

            for row in rows:
                writer.writerow(row)

    def to_column_dict(
        self,
        pred_spans: List[Dict[str, Any]],
        *,
        sort_by_text_position: bool = True,
        unique: bool = False,
    ) -> Dict[str, List[str]]:
        """
        Convert predicted spans into a column-oriented dictionary.

        Output format:
            {
              "ORG": ["ACME Corp", "Globex"],
              "INCIDENT": ["security incident"]
            }
        """
        grouped = self._group_by_label(pred_spans)

        if sort_by_text_position:
            for lab in grouped:
                grouped[lab].sort(key=lambda x: x["start"])

        if unique:
            grouped = self._deduplicate(grouped)

        return {
            label: [sp["text"] for sp in spans]
            for label, spans in grouped.items()
        }

