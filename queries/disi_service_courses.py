import os
import re
import yaml

ROOT = ".."
TARGET_DIR = os.path.join(ROOT, "data", "programmes", "2025", "disi")

FILENAME_PATTERN = re.compile(r"^programme-(\d+)\.ya?ml$")

def find_programme_codes(target_dir):
    codes = []

    if not os.path.isdir(target_dir):
        raise FileNotFoundError(f"Directory not found: {target_dir}")

    for entry in sorted(os.listdir(target_dir)):
        full_path = os.path.join(target_dir, entry)

        if not os.path.isfile(full_path):
            continue

        match = FILENAME_PATTERN.match(entry)
        if not match:
            continue

        code_from_filename = match.group(1)

        # Cross-check against the "code" field inside the YAML content
        with open(full_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        code_from_content = str(data.get("code", "")).strip()

        if code_from_content and code_from_content != code_from_filename:
            print(
                f"Warning: filename code ({code_from_filename}) "
                f"differs from content code ({code_from_content}) in {entry}"
            )

        codes.append(code_from_filename)

    return codes

def main():
    # disi_programmes = set(find_programme_codes(TARGET_DIR))
    disi_courses = []
    with open("courses_disi.yaml", encoding="utf-8") as fh:
        disi_courses = yaml.safe_load(fh)
    service_courses = []
    for person in disi_courses:
        person_courses = []
        for course in person.get("courses",[]):
            if "disi" not in [p.get("dept") for p in course.get("programmes",[])]:
                person_courses.append( course )
        if person_courses:
            person["courses"] = person_courses
            service_courses.append( person )
  
    with open("disi_service_courses.yaml", "w", encoding="utf-8") as fh:
        yaml.safe_dump(
            service_courses,
            fh,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
    )
    
if __name__ == "__main__":
    main()