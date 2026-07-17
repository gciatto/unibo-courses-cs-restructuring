#!/usr/bin/env python3
import sys
import yaml
from collections import defaultdict

def load_yaml(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        records = yaml.safe_load(f.read())
    if not isinstance(records, list):
        raise ValueError(f"Expected a top-level YAML list in {path!r}, got {type(records).__name__}")
    return records


def dump_yaml(data: list, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)

def build_department_courses(records: list) -> dict:
    """
    Returns: { department_name: { (id, name, credits): course_dict, ... }, ... }
    Uniqueness key is the tuple (id, name, credits).
    """
    dept_courses: dict[str, dict] = defaultdict(dict)

    for record in records:
        dept    = (record.get("department") or "").strip()
        courses = record.get("courses") or []
        name    = record.get("name") or "Unknown"
        uid    = record.get("uid") or "Unknown"

        for course in courses:
            cid     = str(course.get("id",      "")).strip()
            cname   = str(course.get("name",    "")).strip()
            credits = course.get("credits")
            key     = (cid, cname, credits)

            if key not in dept_courses[dept]:
                dept_courses[dept][key] = {"teachers": [ { "name": name, "uid": uid } ], "id": cid, "name": cname, "credits": credits}
            else: 
                dept_courses[dept][key]["teachers"].append( { "name": name, "uid": uid } )

    return dept_courses


def build_output(dept_courses: dict) -> list:
    output = []
    for dept_name in sorted(dept_courses.keys()):
        courses = list(dept_courses[dept_name].values())
        # Sort by id then name for deterministic output
        courses.sort(key=lambda c: (c["id"], c["name"]))
        output.append({"department": dept_name, "courses": courses})
    return output


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python file.py <input.yaml> <output.yaml>",
              file=sys.stderr)
        sys.exit(1)

    input_path, output_path = sys.argv[1], sys.argv[2]

    print(f"Reading  {input_path!r} ...")
    records = load_yaml(input_path)
    print(f"  → {len(records)} person records loaded.")

    dept_courses = build_department_courses(records)
    output       = build_output(dept_courses)

    dump_yaml(output, output_path)

    total = sum(len(d["courses"]) for d in output)
    print(f"Writing  {output_path!r} ...")
    print(f"  → {len(output)} departments, {total} unique courses.")
    print("Done.")


if __name__ == "__main__":
    main()
