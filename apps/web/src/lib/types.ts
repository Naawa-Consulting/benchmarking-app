export interface Study {
  id: string;
  name: string;
  source: string;
  raw_ready?: boolean;
  local_ready?: boolean;
  mapped?: boolean;
  curated_ready?: boolean;
  published_to_supabase?: boolean;
  landing_file?: string;
  status?: string;
  error?: string;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
  market_source?: "rule" | "manual" | null;
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

export interface QuestionMapRow {
  study_id: string;
  var_code: string;
  question_text?: string | null;
  var_type?: string | null;
  stage?: string | null;
  brand_mode?: string | null;
  brand_value?: string | null;
  brand_extractor_id?: string | null;
  touchpoint_mode?: string | null;
  touchpoint_value?: string | null;
  touchpoint_rule_id?: string | null;
  source_stage?: string | null;
  source_brand?: string | null;
  source_touchpoint?: string | null;
  updated_at?: string | null;
  updated_by?: string | null;
}

export interface QuestionMapPreview {
  var_code: string;
  kind: "labels" | "samples";
  items: Array<{ code: string; label: string } | string>;
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

export interface MarketTaxonomyItem {
  market_sector: string;
  market_subsector: string;
  market_category: string;
}

export interface StudyClassification {
  study_id: string;
  sector: string | null;
  subsector: string | null;
  category: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
  market_source?: "rule" | "manual" | null;
}

export type TaxonomyView = "market" | "standard";

export interface StudyConfig {
  study_id: string;
  respondent_id?: { source?: string; var_code?: string | null };
  weight?: { source?: string; var_code?: string | null; default?: number };
}

export interface StudyVariable {
  var_code: string;
  label?: string | null;
  type?: string | null;
}

export interface StudyBasePreview {
  respondent_id: string | number | null;
  weight: number | null;
}

export interface DemographicsConfig {
  study_id: string;
  date: { mode: "none" | "var" | "constant"; var_code: string | null; constant: string | null };
  gender_var: string | null;
  age_var: string | null;
  nse_var: string | null;
  state_var: string | null;
}

export interface DemographicsValueLabel {
  value_code: string;
  value_label: string;
}

export interface DemographicsGenderMapItem {
  raw_value: string;
  raw_label: string;
  standard_value: "Male" | "Female" | "Non-binary" | "Prefer not to say" | "Unknown";
  count: number;
}

export type BbsRole = "owner" | "admin" | "analyst" | "viewer";

export interface AdminUserItem {
  id: string;
  email: string | null;
  role: BbsRole;
  created_at: string | null;
  last_sign_in_at: string | null;
  email_confirmed_at: string | null;
  can_toggle_brands: boolean;
  scope_counts: {
    market_sector: number;
    market_subsector: number;
    market_category: number;
  };
}

export interface UserAccessPayload {
  user_id: string;
  can_toggle_brands: boolean;
  scopes: {
    market_sector: string[];
    market_subsector: string[];
    market_category: string[];
  };
  available: {
    market_sector: string[];
    market_subsector: string[];
    market_category: string[];
  };
}

