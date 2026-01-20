import unittest

from app.data.study_config import detect_base_columns


class StudyConfigDetectionTests(unittest.TestCase):
    def test_detects_exact_matches(self):
        respondent, weight = detect_base_columns(["respondent_id", "weight"])
        self.assertEqual(respondent, "respondent_id")
        self.assertEqual(weight, "weight")

    def test_prefers_exact_over_contains(self):
        respondent, weight = detect_base_columns(["id", "panelist_id", "weight_factor"])
        self.assertEqual(respondent, "id")
        self.assertEqual(weight, "weight_factor")

    def test_falls_back_to_contains(self):
        respondent, weight = detect_base_columns(["panelist_id", "peso_total"])
        self.assertEqual(respondent, "panelist_id")
        self.assertEqual(weight, "peso_total")

    def test_returns_none_when_missing(self):
        respondent, weight = detect_base_columns(["q1", "q2"])
        self.assertIsNone(respondent)
        self.assertIsNone(weight)


if __name__ == "__main__":
    unittest.main()
