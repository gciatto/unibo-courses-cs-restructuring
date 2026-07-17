from collections import defaultdict
import yaml

INPUT_YAML = "disi_service_courses.yaml"
OUTPUT_YAML = "grouped_disi_service_courses_by_department.yaml"

def dept_label_from_course(course: dict) -> str:
    """
    Build one department label for a course from all distinct programme departments.
    If more than one department appears, join them in alphabetical order.
    """
    programmes = course.get("programmes") or []
    depts = {
        str(p.get("dept", "")).strip()
        for p in programmes
        if str(p.get("dept", "")).strip()
    }
    if not depts:
        return "(unknown)"
    return " + ".join(sorted(depts))


def parse_credits(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def main() -> None:
    with open(INPUT_YAML, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or []

    grouped = defaultdict(list)

    for person in data:
        person_name = (person.get("name") or "").strip()
        person_uid = str(person.get("uid", "")).strip()
        source_department = (person.get("department") or "").strip()
        courses = person.get("courses") or []

        for course in courses:
            dept_label = dept_label_from_course(course)

            grouped[dept_label].append({
                "course_id": course.get("id"),
                "course_name": course.get("name"),
                "credits": parse_credits(course.get("credits")),
                "teacher_name": person_name,
                "teacher_uid": person_uid,
                "teacher_department": source_department,
                "programmes": course.get("programmes") or [],
            })

    output = []
    for dept_label in sorted(grouped):
        entries = grouped[dept_label]
        total_credits = sum(item["credits"] for item in entries)

        output.append({
            "department": dept_label,
            "total_courses": len(entries),
            "total_credits": total_credits,
            "courses": entries,
        })

    with open(OUTPUT_YAML, "w", encoding="utf-8") as fh:
        yaml.safe_dump(
            output,
            fh,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )


if __name__ == "__main__":
    main()
