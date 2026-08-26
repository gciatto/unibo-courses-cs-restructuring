"""
Loads contacts from data/contacts.csv on the local file system, filters out
those from "Dipartimento di Informatica - Scienza e Ingegneria", then checks
if each contact has a folder under data/courses/<email_name>/ and prints the
courses found therein that are not taught by at least one member of DISI.
"""

import csv
import os
import re
import sys
import yaml
from difflib import SequenceMatcher
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Path to the repository root (default: directory containing this script)
REPO_ROOT = ".."

CONTACTS_CSV   = os.path.join(REPO_ROOT, "data", "contacts.csv")
COURSES_DIR    = os.path.join(REPO_ROOT, "data", "courses")
TARGET_DEPT    = "disi"


def load_disi_courses() -> set:
    disi_courses = set()
    with open("courses_disi.yaml", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    for person in data:
        for course in person.get("courses",[]) or []:
            cid  = str(course.get("id", {})) or "Unknown"
            name = course.get("name", {}).strip().replace("\"","'").lower() or "Unknown"
            ccredits = course.get("credits", 0) or 0
            codes = frozenset([ p.get("code") for p in course.get("programmes") ])
            course = (cid, name, ccredits, codes)
            disi_courses.add( course )
    return disi_courses

def extract_email_name(email: str) -> str:
    return email.split("@")[0] if "@" in email else email


def parse_course_yaml(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    course_title = data.get("course_title", {}) or {}
    course_credits = data.get("credits", 0) or 0
    programmes = data.get("programmes", {}) or [0]
    syllabus = data.get("syllabus", {}) or [0]
    return {
        "id":   str(course_title.get("id", "")).strip(),
        "name": str(course_title.get("name", "Unknown")).strip(),
        "credits": course_credits[0] if isinstance(course_credits, (list, tuple)) else course_credits,
        "programmes": programmes,
        "syllabus": syllabus
    }


# UTILITIES FOR COURSE SYLLABI SIMILARITY SCORE

def _flatten_text(obj, texts=None):
    """Recursively walk a nested dict/list/str structure and collect all string leaves."""
    if texts is None:
        texts = []
    if isinstance(obj, dict):
        for v in obj.values():
            _flatten_text(v, texts)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            _flatten_text(item, texts)
    elif isinstance(obj, str):
        texts.append(obj)
    return texts


def _normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"https?://\S+", " ", text)          # strip URLs
    text = re.sub(r"[^a-zà-öø-ÿ0-9\s]", " ", text)      # strip punctuation
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _syllabus_to_text(syllabus: dict) -> str:
    """
    Flattens a syllabus record (e.g., keyed by language e.g.
    'it'/'en', each containing 'title' and a 'contents' dict of sections)
    into one normalized string of all its textual content.
    """
    raw_texts = _flatten_text(syllabus)
    joined = " ".join(raw_texts)
    return _normalize(joined)


def syllabus_similarity(syllabus_a: dict, syllabus_b: dict,
                         method: str = "tfidf") -> float:
    """
    Compute a similarity score in [0, 1] between two course syllabus records.
    The function works with any nested dict/list/str structure and simply
    recursively extracts and concatenates every string value found.

    Parameters
    ----------
    method :
        "tfidf"    -> TF-IDF cosine similarity (default, good for topical overlap)
        "sequence" -> difflib SequenceMatcher ratio on normalized text (good for
                      near-duplicate / translation-pair detection at character level)
    """

    text_a = _syllabus_to_text(syllabus_a)
    text_b = _syllabus_to_text(syllabus_b)

    if not text_a or not text_b:
        return 0.0
    if text_a == text_b:
        return 1.0

    if method == "sequence":
        return SequenceMatcher(None, text_a, text_b).ratio()

    vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
    tfidf_matrix = vectorizer.fit_transform([text_a, text_b])
    score = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])[0][0]
    return float(round(score, 4))

# ---------------------------------------------

def main():
    disi_courses = load_disi_courses()

    if not os.path.isfile(CONTACTS_CSV):
        sys.exit(f"ERROR: contacts file not found: {CONTACTS_CSV}")

    with open(CONTACTS_CSV, encoding="utf-8", newline="") as fh:
        contacts = list(csv.DictReader(fh))

    non_disi = [
        c for c in contacts
        if c.get("department", "").strip() != TARGET_DEPT
    ]

    if not os.path.isdir(COURSES_DIR):
        sys.exit(f"ERROR: courses directory not found: {COURSES_DIR}")

    output = []

    for contact in non_disi:
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
                course_name = info["name"].strip().replace('"', "'").lower()
                course_credits = info["credits"]
                syllabus = info["syllabus"]
                programmes = info["programmes"]
            except Exception as exc:
                course_id = file_id
                course_name = f"Unknown (parse error: {exc})"
                course_credits = f"Unknown (parse error: {exc})"
                syllabus = f"Unknown (parse error: {exc})"
                programmes = f"Unknown (parse error: {exc})"

            course = {
                "id": course_id,
                "name": course_name,
                "credits": course_credits,
                "programmes": programmes,
                "syllabus": syllabus,
            }

            if (course_id, course_name, course_credits, frozenset([ p.get("code") for p in programmes])) not in disi_courses: # type: ignore
                include = True
                for other_course in courses:
                    if syllabus_similarity(other_course["syllabus"], course["syllabus"]) > 0.95:  # type: ignore
                        include = False
                        break
                if include:
                    courses.append(course)

        if courses:
            name = contact.get("name", "Unknown").strip()
            uid = contact.get("uid", "Unknown").strip()
            dept = contact.get("department", "Unknown").strip().replace('"', "'")

            output.append({
                "name": name,
                "uid": uid,
                "department": dept,
                "courses": [
                    {
                        "id": course["id"],
                        "name": course["name"],
                        "credits": course["credits"],
                        "programmes": [
                            {
                                "code": str(programme.get("code")),
                                "dept": programme.get("department"),
                            }
                            for programme in course["programmes"]
                        ],
                    }
                    for course in courses
                ],
            })

    with open("courses_non_disi.yaml", "w", encoding="utf-8") as fh:
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
