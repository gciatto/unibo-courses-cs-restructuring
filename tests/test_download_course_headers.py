import unittest

from scraping.download_course_headers import build_teachings_url, extract_teacher_slug, parse_teaching_courses


CONTACT_ROW = {
    "uid": "131421",
    "name": "Abatangelo, Nicola",
    "email": "nicola.abatangelo@unibo.it",
    "website": "https://www.unibo.it/sitoweb/nicola.abatangelo",
}


HTML = """
<div class="linked-data-list">
  <h4>
    <a href="/en/study/course-unit-catalogue/course-unit/2025/517744">
      96747 - Advanced Theory of Partial Differential Equations - 6 cfu
    </a>
  </h4>
  <p>Module of Mathematical Methods (I.C.)</p>
  <table>
    <tr><th>Campus:</th><td><p>Bologna</p></td></tr>
    <tr>
      <th>Degree programme:</th>
      <td><p>Second cycle degree programme (LM) in Mathematics</p></td>
    </tr>
  </table>
  <div><p>Lesson period: from November 4, 2025 to December 18, 2025</p></div>
  <div class="link-wrapper">
    <a href="/en/study/course-unit-catalogue/course-unit/2025/517744/timetable">
      Course timetable
    </a>
    <a href="https://virtuale.unibo.it/course/view.php?id=76487">
      Teaching resources on Virtuale
    </a>
  </div>
</div>
<div class="linked-data-list">
  <h4>68486 - EQUAZIONI DIFFERENZIALI ORDINARIE</h4>
  <table>
    <tr><th>Campus:</th><td><p>Bologna</p></td></tr>
    <tr>
      <th>Degree programme:</th>
      <td><p>Second cycle degree programme (LM) in Mathematics</p></td>
    </tr>
  </table>
  <div><p>Lesson period: from February 17, 2026 to April 24, 2026</p></div>
  <div class="link-wrapper"></div>
</div>
<div class="linked-data-list">
  <table>
    <tr><th>Campus:</th><td><p>Bologna</p></td></tr>
  </table>
</div>
"""


class TestDownloadCourseHeaders(unittest.TestCase):
    def test_builds_english_teachings_url_from_teacher_website(self):
        self.assertEqual(extract_teacher_slug(CONTACT_ROW["website"]), "nicola.abatangelo")
        self.assertEqual(
            build_teachings_url(CONTACT_ROW["website"], 2025),
            "https://www.unibo.it/sitoweb/nicola.abatangelo/teachings/2025",
        )

    def test_parses_clickable_and_non_clickable_english_cards(self):
        courses = parse_teaching_courses(
            HTML,
            CONTACT_ROW,
            "https://www.unibo.it/sitoweb/nicola.abatangelo/teachings/2025",
        )

        self.assertEqual(len(courses), 2)

        clickable = courses[0]
        self.assertEqual(
            clickable.course_title,
            "96747 - Advanced Theory of Partial Differential Equations - 6 cfu",
        )
        self.assertEqual(
            clickable.course_url,
            "https://www.unibo.it/en/study/course-unit-catalogue/course-unit/2025/517744",
        )
        self.assertEqual(clickable.module_of, "Module of Mathematical Methods (I.C.)")
        self.assertEqual(clickable.campus, "Bologna")
        self.assertEqual(
            clickable.degree_programme,
            "Second cycle degree programme (LM) in Mathematics",
        )
        self.assertEqual(
            clickable.lesson_period,
            "Lesson period: from November 4, 2025 to December 18, 2025",
        )
        self.assertEqual(
            clickable.schedule_url,
            "https://www.unibo.it/en/study/course-unit-catalogue/course-unit/2025/517744/timetable",
        )
        self.assertEqual(
            clickable.virtuale_url,
            "https://virtuale.unibo.it/course/view.php?id=76487",
        )

        non_clickable = courses[1]
        self.assertEqual(non_clickable.course_title, "68486 - EQUAZIONI DIFFERENZIALI ORDINARIE")
        self.assertEqual(non_clickable.course_url, "")
        self.assertEqual(
            non_clickable.lesson_period,
            "Lesson period: from February 17, 2026 to April 24, 2026",
        )


if __name__ == "__main__":
    unittest.main()
