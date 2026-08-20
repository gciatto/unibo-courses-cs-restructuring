"""Load every source course associated with each DISI teacher."""

import csv
import os
import re
import sys
import yaml

# Path to the repository root (default: directory containing this script)
REPO_ROOT = ".."

CONTACTS_CSV   = os.path.join(REPO_ROOT, "data", "contacts.csv")
COURSES_DIR    = os.path.join(REPO_ROOT, "data", "courses")
TARGET_DEPT    = "disi"
# TARGET_DEPT    = "Dipartimento di Informatica - Scienza e Ingegneria"


def extract_email_name(email: str) -> str:
    return email.split("@")[0] if "@" in email else email


def parse_course_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    course_title = data.get("course_title", {}) or {}
    course_credits = data.get("credits", {}) or [0]
    programmes = data.get("programmes", {}) or [0]
    return {
        "id":   str(course_title.get("id", "")).strip(),
        "name": str(course_title.get("name", "Unknown")).strip().lower(),
        "credits": course_credits[0] if isinstance(course_credits, (list, tuple)) else course_credits,
        "programmes": programmes,
    }

def main():
    if not os.path.isfile(CONTACTS_CSV):
        sys.exit(f"ERROR: contacts file not found: {CONTACTS_CSV}")

    with open(CONTACTS_CSV, encoding="utf-8", newline="") as fh:
        contacts = list(csv.DictReader(fh))

    disi = [
        c for c in contacts
        if c.get("department", "").strip() == TARGET_DEPT
    ]

    if not os.path.isdir(COURSES_DIR):
        sys.exit(f"ERROR: courses directory not found: {COURSES_DIR}")

    output = []

    for contact in disi:
        email = contact.get("email", "").strip()
        if not email:
            continue

        email_name = extract_email_name(email)
        contact_dir = os.path.join(COURSES_DIR, email_name)

        if not os.path.isdir(contact_dir):
            continue

        yaml_files = []
        for dirpath, _dirs, files in os.walk(contact_dir):
            for fname in sorted(files):
                if fname.startswith("course-") and fname.endswith(".yml"):
                    yaml_files.append(os.path.join(dirpath, fname))

        if not yaml_files:
            continue

        courses = []
        for ypath in yaml_files:
            fname = os.path.basename(ypath)
            file_id = re.sub(r"[^\d]", "", fname)

            try:
                info = parse_course_yaml(ypath)
                course_id = info["id"]
                course_name = info["name"].strip().replace('"', "'")
                course_credits = info["credits"]
                programmes = info["programmes"]
            except Exception as exc:
                course_id = file_id
                course_name = f"Unknown (parse error: {exc})"
                course_credits = f"Unknown (parse error: {exc})"
                programmes = f"Unknown (parse error: {exc})"

            course = {
                "id": course_id,
                "name": course_name,
                "credits": course_credits,
                "programmes": programmes,
            }

            # Each source file is a distinct catalogue record. Similar syllabi
            # can legitimately belong to different course IDs, programmes, or
            # credit values, so similarity must not be used for deduplication.
            courses.append(course)

        person = {
            "name": contact.get("name", "Unknown").strip(),
            "uid": contact.get("uid", "Unknown").strip(),
            "department": contact.get("department", "Unknown").strip().replace('"', "'"),
            "courses": [
                {
                    "id": course["id"],
                    "name": course["name"],
                    "credits": course["credits"],
                    "programmes": [{"code": str(p.get("code")), "dept": p.get("department")} for p in course["programmes"]]
                }
                for course in courses
            ],
        }

        output.append(person)

    with open("courses_disi.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump(
            output,
            fh,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
            default_style="'"
        )


if __name__ == "__main__":
    main()
