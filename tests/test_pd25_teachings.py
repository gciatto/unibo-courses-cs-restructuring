import csv
import unittest

from scraping.pd25 import (
    ROOT_DIR,
    candidate_failures,
    format_row_context,
    load_teachings_by_key,
    read_pd25_rows,
    row_key,
    warn_unsupported_tipo,
)


FAILURES_CSV_PATH = ROOT_DIR / "tests" / "pd25_failures.csv"
FAILURE_REPORT_COLUMNS = [
    "__row_number",
    "__match_key",
    "__candidate_count",
    "__failure_kind",
    "__failure_details",
]


def failure_report_fieldnames(rows: list[tuple[int, dict[str, str]]]) -> list[str]:
    if not rows:
        return FAILURE_REPORT_COLUMNS
    return FAILURE_REPORT_COLUMNS + list(rows[0][1].keys())


class TestPd25Teachings(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rows = read_pd25_rows()
        cls.teachings_by_key = load_teachings_by_key()
        cls.failure_rows: list[dict[str, str]] = []

    @classmethod
    def tearDownClass(cls) -> None:
        with FAILURES_CSV_PATH.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=failure_report_fieldnames(cls.rows))
            writer.writeheader()
            writer.writerows(cls.failure_rows)

    @classmethod
    def record_failure(
        cls,
        *,
        row_number: int,
        row: dict[str, str],
        key: tuple[str, str, str],
        candidate_count: int,
        failure_kind: str,
        failure_details: str,
    ) -> None:
        cls.failure_rows.append(
            {
                "__row_number": str(row_number),
                "__match_key": repr(key),
                "__candidate_count": str(candidate_count),
                "__failure_kind": failure_kind,
                "__failure_details": failure_details,
                **row,
            },
        )

    def test_rows_are_represented_by_yaml_teachings(self):
        for row_number, row in self.rows:
            with self.subTest(
                row=row_number,
                teaching=row.get("cod Materia"),
                teacher=row.get("cognome docente"),
                course=row.get("Materia reale"),
            ):
                warn_unsupported_tipo(row_number, row)
                key = row_key(row)
                candidates = self.teachings_by_key.get(key, [])
                context = format_row_context(row_number, row)
                if not candidates:
                    failure_details = f"No YAML teaching found for {context} using key={key!r}"
                    self.record_failure(
                        row_number=row_number,
                        row=row,
                        key=key,
                        candidate_count=0,
                        failure_kind="no_yaml_candidate",
                        failure_details=failure_details,
                    )
                    self.fail(failure_details)

                failures_by_candidate = [
                    candidate_failures(row=row, row_number=row_number, path=path, payload=payload)
                    for path, payload in candidates
                ]
                if any(not failures for failures in failures_by_candidate):
                    continue

                details = "\n".join(failure for failures in failures_by_candidate for failure in failures)
                self.record_failure(
                    row_number=row_number,
                    row=row,
                    key=key,
                    candidate_count=len(candidates),
                    failure_kind="candidate_mismatch",
                    failure_details=details,
                )
                self.fail(f"No YAML teaching candidate satisfies {context}.\n{details}")


if __name__ == "__main__":
    unittest.main()
