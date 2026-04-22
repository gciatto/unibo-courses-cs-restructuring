import unittest
from download_courses import split_course_title, CourseTitle


TEST_TITLES = {
    "91258 - NATURAL LANGUAGE PROCESSING - 5 cfu": 
        CourseTitle(id="91258", 
                    name="NATURAL LANGUAGE PROCESSING", 
                    details=["5 cfu"]),
    "B0385 - NATURAL LANGUAGE PROCESSING (NLP) - 6 cfu":
        CourseTitle(id="B0385", 
                    name="NATURAL LANGUAGE PROCESSING", 
                    details=["NLP", "6 cfu"]),
    "88145 - FONDAMENTI DI INFORMATICA P-1 - 3 cfu":
        CourseTitle(id="88145", 
                    name="FONDAMENTI DI INFORMATICA P-1", 
                    details=["3 cfu"]),
    "69430 - ARCHITETTURA DEI CALCOLATORI ELETTRONICI M - 6 cfu":
        CourseTitle(id="69430",
                    name="ARCHITETTURA DEI CALCOLATORI ELETTRONICI M", 
                    details=["6 cfu"]),
    "78810 - REAL TIME SYSTEMS FOR AUTOMATION M (Modulo 4)":
        CourseTitle(id="78810",
                    name="REAL TIME SYSTEMS FOR AUTOMATION M", 
                    details=["Modulo 4"]),
    "11929 - ALGORITMI E STRUTTURE DATI (CL.A) (Modulo 2)":
        CourseTitle(id="11929",
                    name="ALGORITMI E STRUTTURE DATI", 
                    details=["CL.A", "Modulo 2"]),
    "77780 - ISTEMI EMBEDDED E INTERNET-OF-THINGS - 6 cfu":
        CourseTitle(id="77780",
                    name="ISTEMI EMBEDDED E INTERNET-OF-THINGS", 
                    details=["6 cfu"]),
}


class TestParsingCourseNames(unittest.TestCase):
    def test_parsing_course_names(self):
        for title, expected in TEST_TITLES.items():
            with self.subTest(name=title):
                self.assertEqual(split_course_title(title), expected)

if __name__ == "__main__":
    unittest.main()
