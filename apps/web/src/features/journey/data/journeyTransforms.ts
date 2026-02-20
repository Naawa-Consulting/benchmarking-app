import {
  JOURNEY_STAGES,
  STAGE_ALIAS_TO_CANONICAL,
  type JourneyDims,
  type JourneyStage,
  type JourneyStageRow,
} from "./journeySchema";

const WIDE_STAGE_COLUMN_ALIASES: Record<string, JourneyStage> = {
  brand_awareness: "Brand Awareness",
  ad_awareness: "Ad Awareness",
  brand_consideration: "Brand Consideration",
  brand_purchase: "Brand Purchase",
  brand_satisfaction: "Brand Satisfaction",
  brand_recommendation: "Brand Recommendation",
};

const STAGE_COLUMN_CANDIDATES = ["stage", "journey_stage", "stage_name", "funnel_stage"];
const VALUE_COLUMN_CANDIDATES = [
  "value",
  "value_pct",
  "pct",
  "percentage",
  "journey_pct",
  "journey_value",
];
const WEIGHT_COLUMN_CANDIDATES = ["weight", "base_n", "baseN", "sample_n", "n"];
const BRAND_COLUMN_CANDIDATES = ["brand", "brand_name", "brandName"];
const BRAND_ID_COLUMN_CANDIDATES = ["brand_id", "brandId"];
const STUDY_COLUMN_CANDIDATES = ["study_id", "studyId"];
const SECTOR_COLUMN_CANDIDATES = ["sector"];
const SUBSECTOR_COLUMN_CANDIDATES = ["subsector", "sub_sector"];
const CATEGORY_COLUMN_CANDIDATES = ["category"];
const DEMO_COLUMN_CANDIDATES = ["demo", "demographic", "demographics"];
const TIME_COLUMN_CANDIDATES = ["quarter", "time", "wave", "period"];

const lowerTrim = (value: string) => value.toLowerCase().trim();

const findField = (row: Record<string, unknown>, candidates: string[]) => {
  const keys = Object.keys(row);
  for (const key of keys) {
    const normalized = key.toLowerCase();
    if (candidates.some((candidate) => candidate.toLowerCase() === normalized)) return key;
  }
  return null;
};

const toNumber = (value: unknown): number | null => {
  if (value == null) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value.replace("%", "").trim());
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const normalizePct = (value: number): number => {
  if (value <= 1) return Math.max(0, Math.min(1, value));
  return Math.max(0, Math.min(1, value / 100));
};

const normalizeStage = (value: unknown): JourneyStage | null => {
  if (typeof value !== "string") return null;
  const direct = JOURNEY_STAGES.find((stage) => stage === value.trim());
  if (direct) return direct;
  const canonical = STAGE_ALIAS_TO_CANONICAL[lowerTrim(value).replace(/[_-]+/g, " ")];
  if (canonical) return canonical;
  const compact = STAGE_ALIAS_TO_CANONICAL[lowerTrim(value).replace(/[\s_-]+/g, "")];
  return compact || null;
};

const readField = (row: Record<string, unknown>, candidates: string[]) => {
  const key = findField(row, candidates);
  return key ? row[key] : undefined;
};

const normalizeDims = (row: Record<string, unknown>): JourneyDims => ({
  sector: (readField(row, SECTOR_COLUMN_CANDIDATES) as string | null | undefined) ?? null,
  subsector: (readField(row, SUBSECTOR_COLUMN_CANDIDATES) as string | null | undefined) ?? null,
  category: (readField(row, CATEGORY_COLUMN_CANDIDATES) as string | null | undefined) ?? null,
  demo: (readField(row, DEMO_COLUMN_CANDIDATES) as string | null | undefined) ?? null,
  time: (readField(row, TIME_COLUMN_CANDIDATES) as string | null | undefined) ?? null,
});

const normalizeWeight = (row: Record<string, unknown>): { weight: number; baseN: number | null } => {
  const key = findField(row, WEIGHT_COLUMN_CANDIDATES);
  const value = key ? toNumber(row[key]) : null;
  if (value && value > 0) return { weight: value, baseN: value };
  return { weight: 1, baseN: null };
};

const normalizeStudyAndBrand = (row: Record<string, unknown>) => {
  const studyIdRaw = readField(row, STUDY_COLUMN_CANDIDATES);
  const brandNameRaw = readField(row, BRAND_COLUMN_CANDIDATES);
  const brandIdRaw = readField(row, BRAND_ID_COLUMN_CANDIDATES);
  const studyId = typeof studyIdRaw === "string" ? studyIdRaw : String(studyIdRaw ?? "");
  const brandName = typeof brandNameRaw === "string" ? brandNameRaw : String(brandNameRaw ?? "");
  const brandId = typeof brandIdRaw === "string" ? brandIdRaw : null;
  return { studyId, brandName, brandId };
};

const rowLooksLong = (row: Record<string, unknown>) => {
  const stageCandidate = readField(row, STAGE_COLUMN_CANDIDATES);
  const valueCandidate = readField(row, VALUE_COLUMN_CANDIDATES);
  return normalizeStage(stageCandidate) !== null && toNumber(valueCandidate) !== null;
};

const rowHasAnyWideStage = (row: Record<string, unknown>) =>
  Object.keys(row).some((key) => Object.hasOwn(WIDE_STAGE_COLUMN_ALIASES, key.toLowerCase()));

export function normalizeJourneyResults(rawJourneyResults: unknown): JourneyStageRow[] {
  if (!Array.isArray(rawJourneyResults)) return [];
  const normalized: JourneyStageRow[] = [];

  for (const rawRow of rawJourneyResults) {
    if (!rawRow || typeof rawRow !== "object") continue;
    const row = rawRow as Record<string, unknown>;
    const { studyId, brandName, brandId } = normalizeStudyAndBrand(row);
    if (!studyId || !brandName) continue;
    const dims = normalizeDims(row);
    const { weight, baseN } = normalizeWeight(row);

    if (rowLooksLong(row)) {
      const stage = normalizeStage(readField(row, STAGE_COLUMN_CANDIDATES));
      const valueRaw = toNumber(readField(row, VALUE_COLUMN_CANDIDATES));
      if (!stage || valueRaw == null) continue;
      normalized.push({
        studyId,
        brandId,
        brandName,
        dims,
        stage,
        value: normalizePct(valueRaw),
        weight,
        baseN,
        raw: row,
      });
      continue;
    }

    if (!rowHasAnyWideStage(row)) continue;
    for (const [column, stage] of Object.entries(WIDE_STAGE_COLUMN_ALIASES)) {
      const key = Object.keys(row).find((k) => k.toLowerCase() === column);
      if (!key) continue;
      const valueRaw = toNumber(row[key]);
      if (valueRaw == null) continue;
      normalized.push({
        studyId,
        brandId,
        brandName,
        dims,
        stage,
        value: normalizePct(valueRaw),
        weight,
        baseN,
        raw: row,
      });
    }
  }

  return normalized;
}

