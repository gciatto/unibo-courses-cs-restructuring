"""
Loads contacts from data/contacts.csv on the local file system, filters out
those from "Dipartimento di Informatica - Scienza e Ingegneria", then checks
if each contact has a folder under data/courses/<email_name>/ and prints the
courses found therein.
"""

import csv
import os
import re
import sys
import yaml

# Path to the repository root (default: directory containing this script)
REPO_ROOT = ".."

CONTACTS_CSV   = os.path.join(REPO_ROOT, "data", "contacts.csv")
COURSES_DIR    = os.path.join(REPO_ROOT, "data", "courses")
TARGET_DEPT    = "Dipartimento di Informatica - Scienza e Ingegneria"


def extract_email_name(email: str) -> str:
    return email.split("@")[0] if "@" in email else email


def parse_course_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    course_title = data.get("course_title", {}) or {}
    return {
        "id":   str(course_title.get("id", "")).strip(),
        "name": str(course_title.get("name", "Unknown")).strip(),
    }


def main():
    if not os.path.isfile(CONTACTS_CSV):
        sys.exit(f"ERROR: contacts file not found: {CONTACTS_CSV}")

    with open(CONTACTS_CSV, encoding="utf-8", newline="") as fh:
        contacts = list(csv.DictReader(fh))

    non_disi = [
        c for c in contacts
        if c.get("dipartimento", "").strip() != TARGET_DEPT
    ]

    if not os.path.isdir(COURSES_DIR):
        sys.exit(f"ERROR: courses directory not found: {COURSES_DIR}")

    for contact in non_disi:
        email = contact.get("email", "").strip()
        if not email:
            continue

        email_name = extract_email_name(email)
        contact_dir = os.path.join(COURSES_DIR, email_name)

        if not os.path.isdir(contact_dir):
            continue  # This contact has no folder

        yaml_files = []
        for dirpath, _dirs, files in os.walk(contact_dir):
            for fname in sorted(files):
                if fname.endswith(".yml"):
                    yaml_files.append(os.path.join(dirpath, fname))

        if not yaml_files:
            continue

        courses = []
        for ypath in yaml_files:
            fname = os.path.basename(ypath)
            file_id = re.sub(r"[^\d]", "", fname)  # digits from filename as fallback

            try:
                info = parse_course_yaml(ypath)
                course_id   = info["id"]
                course_name = info["name"]
            except Exception as exc:
                course_id   = file_id
                course_name = f"Unknown (parse error: {exc})"

            courses.append((course_id, course_name))

        name = contact.get("nome", "Unknown").strip()
        dept = contact.get("dipartimento", "Unknown").strip()
        print(f"\nName: {name}")
        print(f"Department: {dept}")
        print("Courses:")
        for cid, cname in courses:
            print(f"  - {cid}, {cname}")


if __name__ == "__main__":
    main()