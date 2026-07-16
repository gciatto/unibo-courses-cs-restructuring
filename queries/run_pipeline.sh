source .venv/bin/activate
read -r -p "Gathering DISI courses? (it takes a while) Type yes to continue, or press Enter to skip: " reply
if [[ "$reply" == "yes" ]]; then
  echo "Gathering DISI courses"
  uv run python courses_disi.py
else
  echo "Skipping DISI courses"
fi

read -r -p "Gathering non-DISI courses? (it takes a while) Type yes to continue, or press Enter to skip: " reply
if [[ "$reply" == "yes" ]]; then
  echo "Gathering non-DISI courses"
  uv run python courses_non_disi.py
else
  echo "Skipping non-DISI courses"
fi

read -r -p "Gathering DISI service courses? (it takes a while) Type yes to continue, or press Enter to skip: " reply
if [[ "$reply" == "yes" ]]; then
  echo "Gathering DISI service courses"
  uv run python disi_service_courses.py.py
else
  echo "Skipping DISI service courses"
fi

echo "Grouping non-DISI courses"
uv run python group_courses_by_department.py courses_non_disi.yaml grouped_courses_by_department.yaml
echo "Grouping service DISI courses"
uv run python group_service_courses_by_department.py
echo "Plotting non-DISI courses credits by department"
uv run python plot_credits_by_department.py grouped_courses_by_department.yaml
echo "Plotting non-DISI courses credits by department (breakdown)"
uv run python plot_people_by_department.py grouped_courses_by_department.yaml
echo "Plotting non-DISI courses people by department"
uv run python plot_people_by_department_bd.py grouped_courses_by_department.yaml
echo "Plotting service DISI courses credits by department"
uv run python plot_credits_service_courses_by_department_db.py
echo "Plotting service courses people by department (breakdown)"
uv run python plot_people_service_courses_by_department_db.py