export interface Study {
  id: string;
  name: string;
  source: string;
  raw_ready?: boolean;
  curated_ready?: boolean;
  landing_file?: string;
  status?: string;
  error?: string;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
}

export interface JourneyPoint {
  stage: string;
  brand: string;
  percentage: number;
}

export interface JourneyResponse {
  study_id: string;
  points: JourneyPoint[];
  source?: string;
}

export interface MappingCandidate {
  var_code: string;
  question_text?: string | null;
  suggested_stage: string;
  confidence: number;
}

export interface MappingRow {
  var_code: string;
  stage: string;
  brand: string;
  value_true_codes: string;
}

export interface RuleCoverage {
  study_id: string;
  mapped_rows: number;
  unmapped_rows: number;
  ignored_rows: number;
  touchpoint_mapped_rows?: number | null;
  output_path?: string | null;
  examples: {
    mapped: Array<Record<string, unknown>>;
    unmapped: Array<Record<string, unknown>>;
    ignored: Array<Record<string, unknown>>;
  };
}

export interface QuestionItem {
  var_code: string;
  question_text?: string | null;
  stage_mapped?: boolean;
  brand_mapped?: boolean;
  touchpoint_mapped?: boolean;
  mapped_stage?: string | null;
  mapped_brand_example?: string | null;
  mapped_touchpoint?: string | null;
  value_preview?: {
    type: "numeric" | "string" | "mixed" | "unknown";
    top_values: Array<{ value: string; count: number }>;
    distinct: number;
  } | null;
}

export interface StudyRuleScope {
  study_id: string;
  enabled_stage_rules: string[];
  enabled_brand_extractors: string[];
  enabled_ignore_rules: string[];
}

export interface TaxonomyItem {
  sector: string;
  subsector: string;
  category: string;
}

export interface StudyClassification {
  study_id: string;
  sector: string | null;
  subsector: string | null;
  category: string | null;
}
