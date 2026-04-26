import pathlib
import csv


DIR_DATA = pathlib.Path(__file__).parent
FILE_CONTACTS = DIR_DATA / "contacts.csv"
FILE_COURSE_HEADERS = DIR_DATA / "contact_headers.csv"


def read_column(path, header, limit=None):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=",")
        assert reader.fieldnames is not None, "contacts.csv has no header row"
        assert header in reader.fieldnames, f"Missing expected column: {header!r}"

        count = 0
        for row in reader:
            value = row[header].strip()
            if value:
                yield reader.line_num, value
                count += 1
            if limit is not None and count >= limit:
                break