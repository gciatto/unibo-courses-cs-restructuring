source .venv/bin/activate
read -r -p "Gathering DISI courses? (it takes a while) Type yes to continue, or press Enter to skip: " reply
if [[ "$reply" == "yes" ]]; then
  echo "Gathering DISI courses"
  uv run python gather_courses_disi.py
else
  echo "Skipping DISI courses"
fi

read -r -p "Gathering non-DISI courses? (it takes a while) Type yes to continue, or press Enter to skip: " reply
if [[ "$reply" == "yes" ]]; then
  echo "Gathering non-DISI courses"
  uv run python gather_courses_non_disi.py
else
  echo "Skipping non-DISI courses"
fi

read -r -p "Gathering DISI service courses? (it takes a while) Type yes to continue, or press Enter to skip: " reply
if [[ "$reply" == "yes" ]]; then
  echo "Gathering DISI service courses"
  uv run python gather_disi_service_courses.py
else
  echo "Skipping DISI service courses"
fi

echo "Grouping non-DISI courses"
uv run python group_non_disi_courses_by_department.py courses_non_disi.yaml grouped_non_disi_courses_by_department.yaml
echo "Grouping service DISI courses"
uv run python group_disi_service_courses_by_department.py
echo "Plotting DISI service course-to-programme Sankey diagram"
uv run python plot_grouped_disi_service_courses_sankey.py
echo "Plotting non-DISI course-to-programme Sankey diagram"
uv run python plot_other_people_sankey.py
echo "Plotting non-DISI courses credits by department"
uv run python plot_other_credits_by_department.py grouped_non_disi_courses_by_department.yaml
echo "Plotting non-DISI courses credits by department (breakdown)"
uv run python plot_other_people_by_department.py grouped_non_disi_courses_by_department.yaml
echo "Plotting non-DISI courses people by department"
uv run python plot_other_people_by_department_bd.py grouped_non_disi_courses_by_department.yaml
echo "Plotting service DISI courses credits by department"
uv run python plot_disi_service_courses_credits_by_department.py
echo "Plotting service courses people by department (breakdown)"
uv run python plot_disi_service_courses_people_by_department_db.py
