import unittest

import pandas as pd

from app.routers.pipeline import _apply_consideration_from_purchase_override


class ConsiderationFromPurchaseOverrideTests(unittest.TestCase):
    def test_forces_consideration_when_purchased_but_not_considered(self):
        df = pd.DataFrame(
            [
                {"respondent_id": "r1", "brand": "Corona", "stage": "consideration", "value": 0},
                {"respondent_id": "r1", "brand": "Corona", "stage": "purchase", "value": 1},
                {"respondent_id": "r2", "brand": "Corona", "stage": "consideration", "value": 1},
                {"respondent_id": "r2", "brand": "Corona", "stage": "purchase", "value": 1},
                {"respondent_id": "r3", "brand": "Corona", "stage": "consideration", "value": 0},
                {"respondent_id": "r3", "brand": "Corona", "stage": "purchase", "value": 0},
            ]
        )
        result = _apply_consideration_from_purchase_override("study_x", df)
        considered = result[result["stage"] == "consideration"].set_index("respondent_id")["value"]

        self.assertEqual(considered.loc["r1"], 1)  # forced: bought without considering
        self.assertEqual(considered.loc["r2"], 1)  # already considered, unchanged
        self.assertEqual(considered.loc["r3"], 0)  # never bought, untouched

    def test_no_purchases_leaves_dataframe_unchanged(self):
        df = pd.DataFrame(
            [
                {"respondent_id": "r1", "brand": "Corona", "stage": "consideration", "value": 0},
                {"respondent_id": "r1", "brand": "Corona", "stage": "purchase", "value": 0},
            ]
        )
        result = _apply_consideration_from_purchase_override("study_x", df)
        pd.testing.assert_frame_equal(result, df)

    def test_only_matches_same_brand(self):
        df = pd.DataFrame(
            [
                {"respondent_id": "r1", "brand": "Corona", "stage": "consideration", "value": 0},
                {"respondent_id": "r1", "brand": "Tecate", "stage": "purchase", "value": 1},
            ]
        )
        result = _apply_consideration_from_purchase_override("study_x", df)
        row = result[(result["stage"] == "consideration") & (result["brand"] == "Corona")].iloc[0]
        self.assertEqual(row["value"], 0)


if __name__ == "__main__":
    unittest.main()
