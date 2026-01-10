from pydantic import BaseModel


class Study(BaseModel):
    id: str
    name: str
    source: str
    raw_ready: bool | None = None
    curated_ready: bool | None = None
    landing_file: str | None = None
    status: str | None = None
    error: str | None = None


class SeedResponse(BaseModel):
    path: str
    stats: dict


class JourneyPoint(BaseModel):
    stage: str
    brand: str
    percentage: float


class JourneyResponse(BaseModel):
    study_id: str
    points: list[JourneyPoint]
    source: str | None = None


class IngestFileResult(BaseModel):
    study_id: str
    file: str
    status: str
    rows: int | None = None
    variables: int | None = None
    reason: str | None = None
    error: str | None = None


class IngestRunResponse(BaseModel):
    status: str
    processed: list[IngestFileResult]
    skipped: list[IngestFileResult] | None = None
    errors: list[IngestFileResult] | None = None


class PreviewVariable(BaseModel):
    var_code: str
    question_text: str | None = None


class StudyPreviewResponse(BaseModel):
    study_id: str
    raw_path: str
    rows: int
    variables: int
    variables_sample: list[PreviewVariable]


class MappingCandidate(BaseModel):
    var_code: str
    question_text: str | None = None
    suggested_stage: str
    confidence: float


class MappingSuggestResponse(BaseModel):
    study_id: str
    rules: dict[str, str]
    candidates: list[MappingCandidate]


class MappingRowInput(BaseModel):
    var_code: str
    stage: str
    brand: str
    value_true_codes: str


class MappingListResponse(BaseModel):
    study_id: str
    rows: list[dict]


class MappingSaveRequest(BaseModel):
    study_id: str
    rows: list[MappingRowInput]


class MappingSaveResponse(BaseModel):
    study_id: str
    saved_rows: int
    total_rows: int
    path: str


class MartBuildResponse(BaseModel):
    study_id: str
    respondents: int
    rows: int
    brands: int
    stages: int
    path: str


class RuleSaveResponse(BaseModel):
    ok: bool
    path: str
    version: int


class RuleExample(BaseModel):
    var_code: str
    stage: str | None = None
    brand: str | None = None
    question_text: str | None = None


class RuleCoverageResponse(BaseModel):
    study_id: str
    mapped_rows: int
    unmapped_rows: int
    ignored_rows: int
    output_path: str | None = None
    examples: dict[str, list[RuleExample]]
