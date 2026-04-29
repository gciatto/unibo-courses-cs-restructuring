from data import read_column, FILE_CONTACTS
from resources import classify_role, roles, departments, classify_dept
import unittest


HEADER_ROLE = "role"
HEADER_DEPT = "department"
LIMIT = 1000


class TestRolesRecognition(unittest.TestCase):
    all_roles = set(roles().keys())

    def test_classify_role(self):
        for line_num, role in read_column(FILE_CONTACTS, HEADER_ROLE, limit=LIMIT):
            with self.subTest(line=line_num, role=role):
                result = classify_role(role)
                print(f"Line {line_num}: {role!r} -> {result!r}")
                self.assertIsNotNone(result, f"Failed to classify role: {role!r}")
                self.assertIn(result, self.all_roles, f"Unexpected role classification: {result!r}")


class TestDepartmentsRecognition(unittest.TestCase):
    all_departments = set(departments().keys())

    def test_classify_dept(self):
        for line_num, dept in read_column(FILE_CONTACTS, HEADER_DEPT, limit=LIMIT):
            with self.subTest(line=line_num, dept=dept):
                result = classify_dept(dept)
                print(f"Line {line_num}: {dept!r} -> {result!r}")
                self.assertIsNotNone(result, f"Failed to classify department: {dept!r}")
                self.assertIn(result, self.all_departments, f"Unexpected department classification: {result!r}")