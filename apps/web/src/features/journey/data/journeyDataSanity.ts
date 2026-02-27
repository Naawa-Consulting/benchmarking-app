import { buildJourneyModel } from "./journeyDerived";
import { normalizeJourneyResults } from "./journeyTransforms";

function assert(condition: boolean, message: string) {
  if (!condition) {
    throw new Error(`[JourneyDataSanity] ${message}`);
  }
}

export function runJourneyDataSanityChecks() {
  const wideInput = [
    {
      study_id: "s1",
      brand: "Brand A",
      category: "Cat 1",
      base_n: 100,
      brand_awareness: 80,
      brand_consideration: 40,
      brand_purchase: 20,
      brand_satisfaction: 70,
      brand_recommendation: 60,
    },
  ];
  const normalizedWide = normalizeJourneyResults(wideInput);
  assert(normalizedWide.length >= 5, "Wide normalization should create stage rows.");
  assert(
    normalizedWide.every((row) => row.value >= 0 && row.value <= 1),
    "Normalized values must be scaled to 0..1."
  );

  const longInput = [
    { study_id: "s1", brand: "Brand A", stage: "Brand Awareness", value_pct: 80, base_n: 100 },
    { study_id: "s1", brand: "Brand A", stage: "Brand Purchase", value_pct: 20, base_n: 100 },
  ];
  const normalizedLong = normalizeJourneyResults(longInput);
  assert(normalizedLong.length === 2, "Long normalization should preserve valid rows.");

  const model = buildJourneyModel(
    [
      {
        study_id: "s1",
        brand: "Brand A",
        category: "Cat 1",
        base_n: 100,
        brand_awareness: 80,
        brand_consideration: 40,
        brand_purchase: 20,
        brand_satisfaction: 70,
        brand_recommendation: 60,
      },
      {
        study_id: "s2",
        brand: "Brand A",
        category: "Cat 1",
        base_n: 120,
        brand_awareness: 70,
        // Missing consideration on purpose to validate no zero-imputation.
        brand_purchase: 25,
        brand_satisfaction: 68,
        brand_recommendation: 55,
      },
      {
        study_id: "s3",
        brand: "Brand B",
        category: "Cat 1",
        base_n: 90,
        brand_awareness: 75,
        brand_consideration: 35,
        brand_purchase: 15,
        brand_satisfaction: 65,
        brand_recommendation: 50,
        promoters_pct: 32,
        detractors_pct: 10,
      },
    ],
    null,
    { includeAdAwareness: false, benchmarkScope: "category" }
  );
  assert(
    model.stagesOrdered.includes("Brand Awareness") && !model.stagesOrdered.includes("Ad Awareness"),
    "includeAdAwareness=false should remove Ad Awareness from ordered funnel."
  );
  const firstBrand = model.brandStageAggregates.find((item) => item.brandName === "Brand A");
  assert(Boolean(firstBrand), "Brand aggregate should exist.");
  if (firstBrand) {
    const purchaseCoverage = firstBrand.stageAggregates.find((item) => item.stage === "Brand Purchase");
    assert((purchaseCoverage?.stageCoverageStudies || 0) >= 2, "Coverage should count only studies with that stage.");
  }
  const secondBrand = model.brandStageAggregates.find((item) => item.brandName === "Brand B");
  assert(secondBrand?.nps.meta.metricType === "official", "NPS should be official when promoters/detractors are present.");
  const firstIndex = firstBrand ? model.journeyIndexByBrand[firstBrand.key] : null;
  assert(Boolean(firstIndex), "Journey index should be computed for each brand aggregate.");
  if (firstIndex) {
    assert(
      firstIndex.value == null || (firstIndex.value >= 0 && firstIndex.value <= 100),
      "Journey index should remain in 0..100 range."
    );
  }
  const firstHealth = firstBrand ? model.funnelHealthByBrand[firstBrand.key] : null;
  assert(Boolean(firstHealth), "Funnel health should exist for each brand aggregate.");
  if (firstHealth && firstHealth.maxDropPts != null) {
    assert(firstHealth.maxDropPts >= 0, "Funnel health max drop should be non-negative in points.");
  }
}
