"use client";

import { useEffect, useMemo, useState } from "react";

import {
  ApiResult,
  applyQuestionMapSuggestionsDetailed,
  bulkUpdateQuestionMapDetailed,
  coverageRulesDetailed,
  getDemographicsConfigDetailed,
  getDemographicsDatePreview,
  getDemographicsPreviewDetailed,
  getDemographicsValueLabelsDetailed,
  getApiBaseUrl,
  getQuestionMapDetailed,
  getQuestionMapValuePreviewDetailed,
  getQuestionsDetailed,
  getJourneyStatusDetailed,
  getRulesDetailed,
  getStudyBasePreviewDetailed,
  getStudyConfigDetailed,
  getStudyRuleScopeDetailed,
  getStudyVariablesDetailed,
  getStudiesDetailed,
  getStudyClassificationDetailed,
  getTaxonomyDetailed,
  pingHealthDetailed,
  rebuildBaseDetailed,
  runRulesDetailed,
  ensureJourneyDetailed,
  saveDemographicsConfigDetailed,
  saveStudyConfigDetailed,
  saveStudyRuleScopeDetailed,
  saveStudyClassificationDetailed,
  saveRulesDetailed,
} from "../../lib/api";
import {
  DemographicsConfig,
  DemographicsValueLabel,
  QuestionItem,
  QuestionMapRow,
  RuleCoverage,
  Study,
  StudyBasePreview,
  StudyClassification,
  StudyConfig,
  StudyRuleScope,
  StudyVariable,
  TaxonomyItem,
} from "../../lib/types";

type ActionState = "idle" | "loading" | "success" | "error";

type RulesPayload = {
  version: number;
  stage_rules: Array<Record<string, unknown>>;
  brand_extractors: Array<Record<string, unknown>>;
  ignore_rules: Array<Record<string, unknown>>;
  touchpoint_rules?: Array<Record<string, unknown>>;
  defaults?: Record<string, unknown>;
};

type StageRuleForm = {
  id: string;
  stage: string;
  question_text_regex: string;
  var_code_regex: string;
  priority: number;
};

type BrandExtractorForm = {
  id: string;
  applies_if_question_text_regex: string;
  mode: "end" | "start" | "between" | "regex";
  extract_regex: string;
  extract_group: number;
  between_left: string;
  between_right: string;
  normalize: boolean;
};

type IgnoreRuleForm = {
  id: string;
  question_text_regex: string;
  var_code_regex: string;
};

type TouchpointRuleForm = {
  id: string;
  touchpoint: string;
  question_regex: string;
  var_code_regex: string;
  priority: number;
};

const STAGES = [
  { value: "awareness", label: "Brand Awareness" },
  { value: "ad_awareness", label: "Ad Awareness" },
  { value: "consideration", label: "Brand Consideration" },
  { value: "purchase", label: "Brand Purchase" },
  { value: "satisfaction", label: "Brand Satisfaction" },
  { value: "recommendation", label: "Brand Recommendation" },
  { value: "touchpoints", label: "Touchpoints" },
  { value: "none", label: "None" },
];

const STAGE_LABELS = Object.fromEntries(STAGES.map((stage) => [stage.value, stage.label]));

function stageLabel(value: string | null | undefined) {
  if (!value) return "--";
  return STAGE_LABELS[value] || value;
}

function formatJson(result: ApiResult | null) {
  if (!result) return "No response yet.";
  return JSON.stringify(
    {
      ok: result.ok,
      status: result.status,
      url: result.url,
      data: result.data,
      error: result.error,
    },
    null,
    2
  );
}

function escapeRegex(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function buildBetweenRegex(left: string, right: string) {
  return `${left}(.+?)(?:${right})`;
}

function createRuleId(prefix: string, existing: Array<Record<string, unknown>>) {
  const next = existing.length + 1;
  return `${prefix}_${String(next).padStart(3, "0")}`;
}

function safeRegexMatch(pattern: string | null | undefined, text: string) {
  if (!pattern) return false;
  try {
    const regex = new RegExp(pattern, "iu");
    return regex.test(text);
  } catch {
    return false;
  }
}

function computeQuestionStatus(questions: QuestionItem[], rules: RulesPayload) {
  const ignoreRules = rules.ignore_rules || [];
  const stageRules = rules.stage_rules || [];

  return questions.map((item) => {
    const questionText = item.question_text || "";
    const combined = `${item.var_code} ${questionText}`.trim();

    const ignored = ignoreRules.some((rule) =>
      safeRegexMatch(rule.question_text_regex as string, questionText) ||
      safeRegexMatch(rule.var_code_regex as string, item.var_code)
    );

    if (ignored) {
      return {
        ...item,
        stage_mapped: false,
        brand_mapped: false,
        touchpoint_mapped: false,
        status: "ignored" as const,
      };
    }

    const matched = stageRules.some((rule) =>
      safeRegexMatch(rule.question_text_regex as string, questionText) ||
      safeRegexMatch(rule.var_code_regex as string, item.var_code) ||
      safeRegexMatch(rule.question_text_regex as string, combined)
    );

    return {
      ...item,
      stage_mapped: matched,
      brand_mapped: false,
      touchpoint_mapped: false,
      status: matched ? "mapped" : "unmapped",
    } as const;
  });
}

export default function RulesStudioPage() {
  const apiBaseUrl = getApiBaseUrl();

  const [apiStatus, setApiStatus] = useState<"ok" | "error" | "idle">("idle");
  const [apiResult, setApiResult] = useState<ApiResult | null>(null);

  const [studies, setStudies] = useState<Study[]>([]);
  const [selectedStudyId, setSelectedStudyId] = useState<string>("");

  const [questions, setQuestions] = useState<QuestionItem[]>([]);
  const [questionsState, setQuestionsState] = useState<ActionState>("idle");

  const [questionMapRows, setQuestionMapRows] = useState<QuestionMapRow[]>([]);
  const [questionMapState, setQuestionMapState] = useState<ActionState>("idle");
  const [questionMapSearch, setQuestionMapSearch] = useState("");
  const [questionMapUnmappedOnly, setQuestionMapUnmappedOnly] = useState(false);
  const [questionMapSelection, setQuestionMapSelection] = useState<Set<string>>(new Set());
  const [bulkStage, setBulkStage] = useState("");
  const [bulkBrand, setBulkBrand] = useState("");
  const [bulkTouchpoint, setBulkTouchpoint] = useState("");
  const [previewRow, setPreviewRow] = useState<QuestionMapRow | null>(null);
  const [previewItems, setPreviewItems] = useState<string>("");

  const [rulesState, setRulesState] = useState<ActionState>("idle");
  const [rulesPayload, setRulesPayload] = useState<RulesPayload | null>(null);
  const [rulesJson, setRulesJson] = useState<string>("");
  const [rulesError, setRulesError] = useState<string | null>(null);

  const [coverageState, setCoverageState] = useState<ActionState>("idle");
  const [publishState, setPublishState] = useState<ActionState>("idle");
  const [publishDetails, setPublishDetails] = useState<ApiResult | null>(null);
  const [publishStatus, setPublishStatus] = useState<{ raw_ready: boolean; mapping_ready: boolean; curated_ready: boolean } | null>(null);
  const [showPublishDetails, setShowPublishDetails] = useState(false);

  const [coverageData, setCoverageData] = useState<RuleCoverage | null>(null);
  const [scopeState, setScopeState] = useState<ActionState>("idle");
  const [ruleScope, setRuleScope] = useState<StudyRuleScope | null>(null);
  const [scopeError, setScopeError] = useState<string | null>(null);

  const [taxonomyItems, setTaxonomyItems] = useState<TaxonomyItem[]>([]);
  const [taxonomyState, setTaxonomyState] = useState<ActionState>("idle");
  const [classificationState, setClassificationState] = useState<ActionState>("idle");
  const [classification, setClassification] = useState<StudyClassification | null>(null);
  const [sector, setSector] = useState<string>("");
  const [subsector, setSubsector] = useState<string>("");
  const [category, setCategory] = useState<string>("");
  const [classificationError, setClassificationError] = useState<string | null>(null);

  const [studyConfigState, setStudyConfigState] = useState<ActionState>("idle");
  const [studyConfig, setStudyConfig] = useState<StudyConfig | null>(null);
  const [studyVariablesState, setStudyVariablesState] = useState<ActionState>("idle");
  const [studyVariables, setStudyVariables] = useState<StudyVariable[]>([]);
  const [basePreviewState, setBasePreviewState] = useState<ActionState>("idle");
  const [basePreviewRows, setBasePreviewRows] = useState<StudyBasePreview[]>([]);
  const [baseError, setBaseError] = useState<string | null>(null);
  const [selectedRespondentVar, setSelectedRespondentVar] = useState<string>("__index__");
  const [selectedWeightVar, setSelectedWeightVar] = useState<string>("__default__");

  const [demographicsState, setDemographicsState] = useState<ActionState>("idle");
  const [demographicsConfig, setDemographicsConfig] = useState<DemographicsConfig | null>(null);
  const [demographicsError, setDemographicsError] = useState<string | null>(null);
  const [demographicsLabels, setDemographicsLabels] = useState<
    Record<string, DemographicsValueLabel[]>
  >({});
  const [demographicsPreview, setDemographicsPreview] = useState<
    Record<
      string,
      {
        rows: string[];
        min?: number | null;
        max?: number | null;
        parsed?: Array<string | null>;
        rate?: number;
      }
    >
  >({});
  const [dateMode, setDateMode] = useState<"none" | "var" | "constant">("none");
  const [dateVar, setDateVar] = useState<string>("");
  const [dateConstant, setDateConstant] = useState<string>("");

  const [runState, setRunState] = useState<ActionState>("idle");
  const [runResult, setRunResult] = useState<ApiResult | null>(null);

  const [lastResponse, setLastResponse] = useState<ApiResult | null>(null);
  const [showDetails, setShowDetails] = useState(false);

  const [activeTab, setActiveTab] = useState<"questions" | "builder" | "mapper" | "advanced">("questions");

  const [searchText, setSearchText] = useState("");
  const [filterConoce, setFilterConoce] = useState(false);
  const [filterCompra, setFilterCompra] = useState(false);
  const [filterUnmapped, setFilterUnmapped] = useState(false);

  const [stageForm, setStageForm] = useState<StageRuleForm>({
    id: "",
    stage: "awareness",
    question_text_regex: "",
    var_code_regex: "",
    priority: 100,
  });

  const [brandForm, setBrandForm] = useState<BrandExtractorForm>({
    id: "",
    applies_if_question_text_regex: "",
    mode: "end",
    extract_regex: "\\?\\s*(.+)$",
    extract_group: 1,
    between_left: "\\-\\s*",
    between_right: "\\s+Anuncios\\s+en\\s+|\\s+Anuncios\\s+de\\s+|\\s+Ads\\s+on\\s+",
    normalize: true,
  });

  const [ignoreForm, setIgnoreForm] = useState<IgnoreRuleForm>({
    id: "",
    question_text_regex: "",
    var_code_regex: "",
  });
  const [touchpointForm, setTouchpointForm] = useState<TouchpointRuleForm>({
    id: "",
    touchpoint: "",
    question_regex: "",
    var_code_regex: "",
    priority: 100,
  });
  const [editingRule, setEditingRule] = useState<{ type: "stage" | "brand" | "ignore" | "touchpoint"; id: string } | null>(null);

  const questionStatuses = useMemo(() => {
    if (questions.some((item) => item.stage_mapped !== undefined || item.brand_mapped !== undefined)) {
      return questions.map((item) => ({
        ...item,
        status: item.stage_mapped ? "mapped" : "unmapped",
      })) as Array<QuestionItem & { status: "mapped" | "unmapped" | "ignored" }>;
    }
    if (!rulesPayload) return questions.map((item) => ({ ...item, status: "unmapped" as const }));
    return computeQuestionStatus(questions, rulesPayload);
  }, [questions, rulesPayload]);

  const filteredQuestions = useMemo(() => {
    return questionStatuses.filter((item) => {
      const questionText = item.question_text || "";
      const matchesSearch =
        !searchText ||
        item.var_code.toLowerCase().includes(searchText.toLowerCase()) ||
        questionText.toLowerCase().includes(searchText.toLowerCase());

      const matchesConoce = !filterConoce || questionText.toLowerCase().includes("conoce");
      const matchesCompra = !filterCompra || questionText.toLowerCase().includes("compra");
      const matchesUnmapped = !filterUnmapped || !item.stage_mapped;

      return matchesSearch && matchesConoce && matchesCompra && matchesUnmapped;
    });
  }, [questionStatuses, searchText, filterConoce, filterCompra, filterUnmapped]);

  const sectors = useMemo(() => {
    return Array.from(new Set(taxonomyItems.map((item) => item.sector))).sort();
  }, [taxonomyItems]);

  const subsectors = useMemo(() => {
    if (!sector) return [];
    return Array.from(
      new Set(taxonomyItems.filter((item) => item.sector === sector).map((item) => item.subsector))
    ).sort();
  }, [taxonomyItems, sector]);

  const categories = useMemo(() => {
    if (!sector || !subsector) return [];
    return Array.from(
      new Set(
        taxonomyItems
          .filter((item) => item.sector === sector && item.subsector === subsector)
          .map((item) => item.category)
      )
    ).sort();
  }, [taxonomyItems, sector, subsector]);

  useEffect(() => {
    const ping = async () => {
      const result = await pingHealthDetailed();
      setApiResult(result);
      setApiStatus(result.ok ? "ok" : "error");
    };

    ping();
  }, []);

  useEffect(() => {
    const loadStudies = async () => {
      const result = await getStudiesDetailed(true);
      if (result.ok && result.data) {
        const payload = result.data as { studies?: Study[] } | Study[];
        const items = Array.isArray(payload) ? payload : payload.studies || [];
        setStudies(items);
        setSelectedStudyId(items[0]?.id || "");
      }
    };

    loadStudies();
  }, []);

  useEffect(() => {
    const loadRules = async () => {
      setRulesState("loading");
      const result = await getRulesDetailed();
      setLastResponse(result);
      if (result.ok && result.data) {
        const payload = result.data as RulesPayload;
        payload.touchpoint_rules = payload.touchpoint_rules || [];
        setRulesPayload(payload);
        setRulesJson(JSON.stringify(payload, null, 2));
        setRulesState("success");
        setRulesError(null);
      } else {
        setRulesState("error");
      }
    };

    loadRules();
  }, []);

  useEffect(() => {
    const loadTaxonomy = async () => {
      setTaxonomyState("loading");
      const result = await getTaxonomyDetailed();
      setLastResponse(result);
      if (result.ok && result.data && typeof result.data === "object") {
        const data = result.data as { items?: TaxonomyItem[] };
        setTaxonomyItems(Array.isArray(data.items) ? data.items : []);
        setTaxonomyState("success");
      } else {
        setTaxonomyState("error");
      }
    };

    loadTaxonomy();
  }, []);

  useEffect(() => {
    if (!selectedStudyId) return;

    const loadQuestions = async () => {
      setQuestionsState("loading");
      const result = await getQuestionsDetailed(selectedStudyId, true, 1000);
      setLastResponse(result);
      if (result.ok && result.data && typeof result.data === "object") {
        const data = result.data as { items?: QuestionItem[] };
        setQuestions(Array.isArray(data.items) ? data.items : []);
        setQuestionsState("success");
      } else {
        setQuestions([]);
        setQuestionsState("error");
      }
    };

    const loadCoverage = async () => {
      setCoverageState("loading");
      const result = await coverageRulesDetailed(selectedStudyId);
      setLastResponse(result);
      if (result.ok && result.data) {
        setCoverageData(result.data as RuleCoverage);
        setCoverageState("success");
      } else {
        setCoverageData(null);
        setCoverageState("error");
      }
    };

    loadQuestions();
    loadCoverage();
  }, [selectedStudyId]);

  useEffect(() => {
    if (!selectedStudyId) return;
    const loadQuestionMap = async () => {
      setQuestionMapState("loading");
      const result = await getQuestionMapDetailed(
        selectedStudyId,
        questionMapSearch || null,
        questionMapUnmappedOnly,
        500,
        0
      );
      setLastResponse(result);
      if (result.ok && result.data && typeof result.data === "object") {
        const data = result.data as { rows?: QuestionMapRow[] };
        setQuestionMapRows(Array.isArray(data.rows) ? data.rows : []);
        setQuestionMapState("success");
      } else {
        setQuestionMapRows([]);
        setQuestionMapState("error");
      }
    };

    loadQuestionMap();
  }, [selectedStudyId, questionMapSearch, questionMapUnmappedOnly]);

  useEffect(() => {
    if (!selectedStudyId) return;
    const loadClassification = async () => {
      setClassificationState("loading");
      const result = await getStudyClassificationDetailed(selectedStudyId);
      setLastResponse(result);
      if (result.ok && result.data) {
        const data = result.data as StudyClassification;
        setClassification(data);
        setSector(data.sector || "");
        setSubsector(data.subsector || "");
        setCategory(data.category || "");
        setClassificationState("success");
        setClassificationError(null);
      } else {
        setClassificationState("error");
      }
    };

    loadClassification();
  }, [selectedStudyId]);

  useEffect(() => {
    if (!selectedStudyId) return;
    const loadBaseConfig = async () => {
      setStudyConfigState("loading");
      const result = await getStudyConfigDetailed(selectedStudyId);
      setLastResponse(result);
      if (result.ok && result.data) {
        const data = result.data as StudyConfig;
        setStudyConfig(data);
        setSelectedRespondentVar(
          data.respondent_id?.var_code ? String(data.respondent_id.var_code) : "__index__"
        );
        setSelectedWeightVar(
          data.weight?.var_code ? String(data.weight.var_code) : "__default__"
        );
        setStudyConfigState("success");
        setBaseError(null);
      } else {
        setStudyConfigState("error");
        setBaseError(result.error || "Unable to load study config.");
      }
    };

    const loadVariables = async () => {
      setStudyVariablesState("loading");
      const result = await getStudyVariablesDetailed(selectedStudyId);
      setLastResponse(result);
      if (result.ok && result.data && typeof result.data === "object") {
        const data = result.data as { variables?: StudyVariable[] };
        setStudyVariables(Array.isArray(data.variables) ? data.variables : []);
        setStudyVariablesState("success");
      } else {
        setStudyVariables([]);
        setStudyVariablesState("error");
      }
    };

    const loadBasePreview = async () => {
      setBasePreviewState("loading");
      const result = await getStudyBasePreviewDetailed(selectedStudyId, 5);
      setLastResponse(result);
      if (result.ok && result.data && typeof result.data === "object") {
        const data = result.data as { rows?: StudyBasePreview[] };
        setBasePreviewRows(Array.isArray(data.rows) ? data.rows : []);
        setBasePreviewState("success");
      } else {
        setBasePreviewRows([]);
        setBasePreviewState("error");
      }
    };

    loadBaseConfig();
    loadVariables();
    loadBasePreview();
  }, [selectedStudyId]);

  useEffect(() => {
    if (!selectedStudyId) return;
    setDemographicsLabels({});
    setDemographicsPreview({});
    const loadDemographicsConfig = async () => {
      setDemographicsState("loading");
      const result = await getDemographicsConfigDetailed(selectedStudyId);
      setLastResponse(result);
      if (result.ok && result.data) {
        const config = result.data as DemographicsConfig;
        setDemographicsConfig(config);
        setDateMode(config.date?.mode || "none");
        setDateVar(config.date?.var_code || "");
        setDateConstant(config.date?.constant || "");
        setDemographicsState("success");
        setDemographicsError(null);
      } else {
        setDemographicsConfig(null);
        setDemographicsState("error");
        setDemographicsError(result.error || "Failed to load demographics config.");
      }
    };

    loadDemographicsConfig();
  }, [selectedStudyId]);

  useEffect(() => {
    if (!selectedStudyId || !rulesPayload) return;
    loadScope(selectedStudyId);
  }, [selectedStudyId, rulesPayload]);

  const handleUseAsPattern = (question: QuestionItem) => {
    const questionText = question.question_text || "";
    const escaped = escapeRegex(questionText).replace(/\s+/g, "\\s+");
    setStageForm((prev) => ({
      ...prev,
      question_text_regex: escaped,
    }));
    setActiveTab("builder");
  };

  const refreshQuestionMap = async () => {
    if (!selectedStudyId) return;
    const result = await getQuestionMapDetailed(
      selectedStudyId,
      questionMapSearch || null,
      questionMapUnmappedOnly,
      500,
      0
    );
    setLastResponse(result);
    if (result.ok && result.data && typeof result.data === "object") {
      const data = result.data as { rows?: QuestionMapRow[] };
      setQuestionMapRows(Array.isArray(data.rows) ? data.rows : []);
      setQuestionMapState("success");
    } else {
      setQuestionMapRows([]);
      setQuestionMapState("error");
    }
  };

  const handleSelectAllQuestionMap = (checked: boolean) => {
    if (!checked) {
      setQuestionMapSelection(new Set());
      return;
    }
    const next = new Set(questionMapRows.map((row) => row.var_code));
    setQuestionMapSelection(next);
  };

  const handleToggleQuestionMapRow = (varCode: string, checked: boolean) => {
    setQuestionMapSelection((prev) => {
      const next = new Set(prev);
      if (checked) {
        next.add(varCode);
      } else {
        next.delete(varCode);
      }
      return next;
    });
  };

  const handleBulkUpdate = async (mode: { stage?: string; brand?: string; touchpoint?: string }, patch: Record<string, unknown>) => {
    if (!selectedStudyId || questionMapSelection.size === 0) return;
    const payload = {
      var_codes: Array.from(questionMapSelection),
      patch,
      mode,
      updated_by: "admin-ui",
    };
    const result = await bulkUpdateQuestionMapDetailed(selectedStudyId, payload);
    setLastResponse(result);
    if (result.ok) {
      await refreshQuestionMap();
      setQuestionMapSelection(new Set());
    }
  };

  const handleApplySuggestions = async () => {
    if (!selectedStudyId) return;
    const result = await applyQuestionMapSuggestionsDetailed(selectedStudyId, {
      targets: ["stage", "brand", "touchpoint"],
      only_empty: true,
    });
    setLastResponse(result);
    if (result.ok) {
      await refreshQuestionMap();
    }
  };

  const handlePreview = async (row: QuestionMapRow) => {
    setPreviewRow(row);
    if (!selectedStudyId) return;
    const result = await getQuestionMapValuePreviewDetailed(selectedStudyId, row.var_code, "labels", 12);
    if (result.ok && result.data && typeof result.data === "object") {
      const data = result.data as { kind?: string; items?: Array<Record<string, unknown> | string> };
      if (data.kind === "labels") {
        const lines = (data.items || []).map((item) => {
          if (typeof item === "string") return item;
          const entry = item as { code?: string; label?: string };
          return `${entry.code}: ${entry.label}`;
        });
        setPreviewItems(lines.join(", "));
      } else {
        setPreviewItems((data.items || []).map((item) => String(item)).join(", "));
      }
    } else {
      setPreviewItems("");
    }
  };

  const handleAddStageRule = () => {
    if (!rulesPayload) return;
    const next = { ...stageForm };
    if (!next.id) {
      next.id = createRuleId("stage", rulesPayload.stage_rules);
    }
    const stageRules = [...rulesPayload.stage_rules.filter((rule) => rule.id !== next.id), next];
    const updated = { ...rulesPayload, stage_rules: stageRules };
    setRulesPayload(updated);
    setRulesJson(JSON.stringify(updated, null, 2));
    setEditingRule(null);
  };

  const handleAddBrandExtractor = () => {
    if (!rulesPayload) return;
    const next = { ...brandForm } as Record<string, unknown>;
    if (!next.id) {
      next.id = createRuleId("brand_extractor", rulesPayload.brand_extractors);
    }
    if (brandForm.mode === "between") {
      next.between = {
        left: brandForm.between_left,
        right: brandForm.between_right,
        group: brandForm.extract_group,
      };
    } else {
      delete next["between"];
    }
    const extractors = [
      ...rulesPayload.brand_extractors.filter((rule) => rule.id !== next.id),
      next,
    ];
    const updated = { ...rulesPayload, brand_extractors: extractors };
    setRulesPayload(updated);
    setRulesJson(JSON.stringify(updated, null, 2));
    setEditingRule(null);
  };

  const handleAddIgnoreRule = () => {
    if (!rulesPayload) return;
    const next = { ...ignoreForm };
    if (!next.id) {
      next.id = createRuleId("ignore", rulesPayload.ignore_rules);
    }
    const ignores = [...rulesPayload.ignore_rules.filter((rule) => rule.id !== next.id), next];
    const updated = { ...rulesPayload, ignore_rules: ignores };
    setRulesPayload(updated);
    setRulesJson(JSON.stringify(updated, null, 2));
    setEditingRule(null);
  };

  const handleAddTouchpointRule = () => {
    if (!rulesPayload) return;
    const next = { ...touchpointForm };
    if (!next.id) {
      next.id = createRuleId("touchpoint", rulesPayload.touchpoint_rules || []);
    }
    const touchpointRules = [
      ...(rulesPayload.touchpoint_rules || []).filter((rule) => rule.id !== next.id),
      next,
    ];
    const updated = { ...rulesPayload, touchpoint_rules: touchpointRules };
    setRulesPayload(updated);
    setRulesJson(JSON.stringify(updated, null, 2));
    setEditingRule(null);
  };

  const handleEditRule = (
    type: "stage" | "brand" | "ignore" | "touchpoint",
    rule: Record<string, unknown>
  ) => {
    const id = String(rule.id || "");
    setEditingRule({ type, id });
    if (type === "stage") {
      setStageForm({
        id,
        stage: String(rule.stage || "awareness"),
        question_text_regex: String(rule.question_text_regex || ""),
        var_code_regex: String(rule.var_code_regex || ""),
        priority: Number(rule.priority || 100),
      });
      setActiveTab("builder");
      return;
    }
    if (type === "brand") {
      const mode = String(rule.mode || "regex") as BrandExtractorForm["mode"];
      const extractRegex =
        mode === "start"
          ? String(rule.extract_regex || "^\\s*(.+?)\\s*[-:]")
          : mode === "end"
          ? String(rule.extract_regex || "\\?\\s*(.+)$")
          : String(rule.extract_regex || "");
      const between = (rule.between as { left?: string; right?: string; group?: number }) || {};
      setBrandForm({
        id,
        applies_if_question_text_regex: String(
          rule.applies_if_question_regex || rule.applies_if_question_text_regex || ""
        ),
        mode,
        extract_regex: extractRegex,
        extract_group: Number(rule.extract_group || 1),
        between_left: String(between.left || "\\-\\s*"),
        between_right: String(
          between.right || "\\s+Anuncios\\s+en\\s+|\\s+Anuncios\\s+de\\s+|\\s+Ads\\s+on\\s+"
        ),
        normalize: Boolean(rule.normalize !== false),
      });
      setActiveTab("builder");
      return;
    }
    if (type === "touchpoint") {
      setTouchpointForm({
        id,
        touchpoint: String(rule.touchpoint || ""),
        question_regex: String(rule.question_regex || rule.question_text_regex || ""),
        var_code_regex: String(rule.var_code_regex || ""),
        priority: Number(rule.priority || 100),
      });
      setActiveTab("builder");
      return;
    }
    setIgnoreForm({
      id,
      question_text_regex: String(rule.question_text_regex || ""),
      var_code_regex: String(rule.var_code_regex || ""),
    });
    setActiveTab("builder");
  };

  const handleCancelEdit = () => {
    setEditingRule(null);
  };

  const handleDeleteRule = (type: "stage" | "brand" | "ignore" | "touchpoint", id: string) => {
    if (!rulesPayload) return;
    if (type === "stage") {
      const updated = {
        ...rulesPayload,
        stage_rules: rulesPayload.stage_rules.filter((rule) => rule.id !== id),
      };
      setRulesPayload(updated);
      setRulesJson(JSON.stringify(updated, null, 2));
      return;
    }
    if (type === "brand") {
      const updated = {
        ...rulesPayload,
        brand_extractors: rulesPayload.brand_extractors.filter((rule) => rule.id !== id),
      };
      setRulesPayload(updated);
      setRulesJson(JSON.stringify(updated, null, 2));
      return;
    }
    if (type === "touchpoint") {
      const updated = {
        ...rulesPayload,
        touchpoint_rules: (rulesPayload.touchpoint_rules || []).filter((rule) => rule.id !== id),
      };
      setRulesPayload(updated);
      setRulesJson(JSON.stringify(updated, null, 2));
      return;
    }
    const updated = {
      ...rulesPayload,
      ignore_rules: rulesPayload.ignore_rules.filter((rule) => rule.id !== id),
    };
    setRulesPayload(updated);
    setRulesJson(JSON.stringify(updated, null, 2));
  };

  const handleSaveRules = async () => {
    setRulesState("loading");
    try {
      const parsed = JSON.parse(rulesJson) as RulesPayload;
      const result = await saveRulesDetailed(parsed);
      setRulesPayload(parsed);
      setLastResponse(result);
      setRulesState(result.ok ? "success" : "error");
      setRulesError(null);
    } catch (error) {
      setRulesError(error instanceof Error ? error.message : "Invalid JSON");
      setRulesState("error");
    }
  };

  const handleReloadRules = async () => {
    setRulesState("loading");
    const result = await getRulesDetailed();
    setLastResponse(result);
    if (result.ok && result.data) {
      const payload = result.data as RulesPayload;
      payload.touchpoint_rules = payload.touchpoint_rules || [];
      setRulesPayload(payload);
      setRulesJson(JSON.stringify(payload, null, 2));
      setRulesState("success");
      setRulesError(null);
    } else {
      setRulesState("error");
    }
  };

  const handleRecomputeCoverage = async () => {
    if (!selectedStudyId) return;
    setCoverageState("loading");
    const result = await coverageRulesDetailed(selectedStudyId);
    setLastResponse(result);
    if (result.ok && result.data) {
      setCoverageData(result.data as RuleCoverage);
      setCoverageState("success");
    } else {
      setCoverageData(null);
      setCoverageState("error");
    }
  };

  const buildDefaultScope = (payload: RulesPayload, studyId: string): StudyRuleScope => {
    return {
      study_id: studyId,
      enabled_stage_rules: payload.stage_rules.map((rule) => String(rule.id)),
      enabled_brand_extractors: payload.brand_extractors.map((rule) => String(rule.id)),
      enabled_ignore_rules: payload.ignore_rules.map((rule) => String(rule.id)),
    };
  };

  const loadScope = async (studyId: string) => {
    if (!studyId) return;
    setScopeState("loading");
    const result = await getStudyRuleScopeDetailed(studyId);
    setLastResponse(result);
    if (result.ok && result.data) {
      setRuleScope(result.data as StudyRuleScope);
      setScopeState("success");
      setScopeError(null);
    } else if (rulesPayload) {
      setRuleScope(buildDefaultScope(rulesPayload, studyId));
      setScopeState("error");
      setScopeError("Failed to load scope, using defaults.");
    } else {
      setScopeState("error");
    }
  };

  const handleToggleScope = (type: "stage" | "brand" | "ignore", id: string) => {
    if (!ruleScope) return;
    const key =
      type === "stage"
        ? "enabled_stage_rules"
        : type === "brand"
        ? "enabled_brand_extractors"
        : "enabled_ignore_rules";
    const current = new Set(ruleScope[key]);
    if (current.has(id)) {
      current.delete(id);
    } else {
      current.add(id);
    }
    setRuleScope({ ...ruleScope, [key]: Array.from(current) } as StudyRuleScope);
  };

  const handleEnableAllScope = () => {
    if (!rulesPayload || !selectedStudyId) return;
    setRuleScope(buildDefaultScope(rulesPayload, selectedStudyId));
  };

  const handleDisableAllScope = () => {
    if (!selectedStudyId) return;
    setRuleScope({
      study_id: selectedStudyId,
      enabled_stage_rules: [],
      enabled_brand_extractors: [],
      enabled_ignore_rules: [],
    });
  };

  const handleSaveScope = async () => {
    if (!selectedStudyId || !ruleScope) return;
    setScopeState("loading");
    const result = await saveStudyRuleScopeDetailed(selectedStudyId, ruleScope);
    setLastResponse(result);
    if (result.ok) {
      setScopeState("success");
      setScopeError(null);
      handleRecomputeCoverage();
      const questionsResult = await getQuestionsDetailed(selectedStudyId, true, 1000);
      if (questionsResult.ok && questionsResult.data && typeof questionsResult.data === "object") {
        const data = questionsResult.data as { items?: QuestionItem[] };
        setQuestions(Array.isArray(data.items) ? data.items : []);
      }
    } else {
      setScopeState("error");
      setScopeError(result.error || "Failed to save scope.");
    }
  };

  const handleSaveClassification = async () => {
    if (!selectedStudyId) return;
    setClassificationState("loading");
    const payload = {
      sector: sector || null,
      subsector: subsector || null,
      category: category || null,
    };
    const result = await saveStudyClassificationDetailed(selectedStudyId, payload);
    setLastResponse(result);
    if (result.ok && result.data) {
      const data = result.data as StudyClassification;
      setClassification(data);
      setClassificationState("success");
      setClassificationError(null);
      setStudies((prev) =>
        prev.map((study) =>
          study.id === selectedStudyId
            ? { ...study, sector: data.sector, subsector: data.subsector, category: data.category }
            : study
        )
      );
    } else {
      setClassificationState("error");
      setClassificationError(result.error || "Failed to save classification.");
    }
  };

  const handleClearClassification = async () => {
    setSector("");
    setSubsector("");
    setCategory("");
    await handleSaveClassification();
  };

  const handleSaveStudyConfig = async () => {
    if (!selectedStudyId) return;
    setStudyConfigState("loading");
    const payload = {
      respondent_id_var: selectedRespondentVar,
      weight_var: selectedWeightVar,
      source: "manual",
    };
    const result = await saveStudyConfigDetailed(selectedStudyId, payload);
    setLastResponse(result);
    if (result.ok && result.data) {
      setStudyConfig(result.data as StudyConfig);
      setStudyConfigState("success");
      setBaseError(null);
    } else {
      setStudyConfigState("error");
      setBaseError(result.error || "Failed to save study config.");
    }
  };

  const handleRebuildBase = async () => {
    if (!selectedStudyId) return;
    setBasePreviewState("loading");
    const result = await rebuildBaseDetailed(selectedStudyId, true);
    setLastResponse(result);
    if (result.ok) {
      setBasePreviewState("success");
      refreshPublishStatus();
      const preview = await getStudyBasePreviewDetailed(selectedStudyId, 5);
      if (preview.ok && preview.data && typeof preview.data === "object") {
        const data = preview.data as { rows?: StudyBasePreview[] };
        setBasePreviewRows(Array.isArray(data.rows) ? data.rows : []);
      }
    } else {
      setBasePreviewState("error");
    }
  };

  const handleSaveDemographics = async () => {
    if (!selectedStudyId || !demographicsConfig) return;
    setDemographicsState("loading");
    if (dateMode === "constant" && dateConstant && !/^\d{4}-\d{2}-\d{2}$/.test(dateConstant)) {
      setDemographicsState("error");
      setDemographicsError("Date constant must be YYYY-MM-DD.");
      return;
    }
    const result = await saveDemographicsConfigDetailed(selectedStudyId, {
      date: {
        mode: dateMode,
        var_code: dateMode === "var" ? dateVar || null : null,
        constant: dateMode === "constant" ? dateConstant || null : null,
      },
      gender_var: demographicsConfig.gender_var,
      age_var: demographicsConfig.age_var,
      nse_var: demographicsConfig.nse_var,
      state_var: demographicsConfig.state_var,
    });
    setLastResponse(result);
    if (result.ok && result.data) {
      setDemographicsConfig(result.data as DemographicsConfig);
      setDemographicsState("success");
      setDemographicsError(null);
    } else {
      setDemographicsState("error");
      setDemographicsError(result.error || "Failed to save demographics.");
    }
  };

  const handleClearDemographics = async () => {
    if (!selectedStudyId) return;
    const cleared: DemographicsConfig = {
      study_id: selectedStudyId,
      date: { mode: "none", var_code: null, constant: null },
      gender_var: null,
      age_var: null,
      nse_var: null,
      state_var: null,
    };
    setDemographicsConfig(cleared);
    setDateMode("none");
    setDateVar("");
    setDateConstant("");
    setDemographicsState("loading");
    const result = await saveDemographicsConfigDetailed(selectedStudyId, {
      date: { mode: "none", var_code: null, constant: null },
      gender_var: null,
      age_var: null,
      nse_var: null,
      state_var: null,
    });
    setLastResponse(result);
    if (result.ok) {
      setDemographicsState("success");
      setDemographicsError(null);
    } else {
      setDemographicsState("error");
      setDemographicsError(result.error || "Failed to clear demographics.");
    }
  };

  const handleSelectDemographicsVar = async (key: keyof DemographicsConfig, value: string) => {
    if (!demographicsConfig) return;
    const next = { ...demographicsConfig, [key]: value || null };
    setDemographicsConfig(next);

    if (key === "age_var" && value) {
      const preview = await getDemographicsPreviewDetailed(selectedStudyId, value, 10);
      if (preview.ok && preview.data && typeof preview.data === "object") {
        const data = preview.data as { rows?: string[]; min?: number | null; max?: number | null };
        setDemographicsPreview((prev) => ({
          ...prev,
          age_var: { rows: data.rows || [], min: data.min, max: data.max },
        }));
      }
    }

    if ((key === "gender_var" || key === "nse_var" || key === "state_var") && value) {
      const labels = await getDemographicsValueLabelsDetailed(selectedStudyId, value);
      if (labels.ok && labels.data && typeof labels.data === "object") {
        const data = labels.data as { items?: DemographicsValueLabel[] };
        setDemographicsLabels((prev) => ({
          ...prev,
          [key]: Array.isArray(data.items) ? data.items : [],
        }));
      }
    }
  };

  const handleDateModeChange = async (mode: "none" | "var" | "constant") => {
    setDateMode(mode);
    if (mode !== "var") {
      setDateVar("");
    }
    if (mode !== "constant") {
      setDateConstant("");
    }
    if (mode === "var" && dateVar) {
      const preview = await getDemographicsDatePreview(selectedStudyId, mode, dateVar, null, 10);
      if (preview.ok && preview.data && typeof preview.data === "object") {
        const data = preview.data as {
          raw_samples?: string[];
          parsed_samples?: string[];
          parse_success_rate?: number;
        };
        setDemographicsPreview((prev) => ({
          ...prev,
          date: {
            rows: data.raw_samples || [],
            parsed: data.parsed_samples || [],
            rate: data.parse_success_rate ?? 0,
          },
        }));
      }
    }
  };

  const handlePublish = async (force: boolean) => {
    if (!selectedStudyId) return;
    setPublishState("loading");
    const result = await ensureJourneyDetailed(selectedStudyId, force);
    setPublishDetails(result);
    setPublishState(result.ok ? "success" : "error");
    if (result.ok) {
      handleRecomputeCoverage();
      refreshPublishStatus();
    }
  };

  const refreshPublishStatus = async () => {
    if (!selectedStudyId) return;
    const result = await getJourneyStatusDetailed(selectedStudyId);
    if (result.ok && result.data && typeof result.data === "object") {
      const data = result.data as { raw_ready?: boolean; mapping_ready?: boolean; curated_ready?: boolean };
      setPublishStatus({
        raw_ready: Boolean(data.raw_ready),
        mapping_ready: Boolean(data.mapping_ready),
        curated_ready: Boolean(data.curated_ready),
      });
    }
  };
  const handleRunRules = async () => {
    if (!selectedStudyId) return;
    setRunState("loading");
    const result = await runRulesDetailed(selectedStudyId);
    setRunResult(result);
    setLastResponse(result);
    setRunState(result.ok ? "success" : "error");
    if (result.ok) {
      handleRecomputeCoverage();
    }
  };

  return (
    <main className="space-y-6">
      <section className="main-surface rounded-3xl p-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <h2 className="text-3xl font-semibold">Rules Studio</h2>
            <p className="mt-2 text-slate">
              Build mapping rules from questions (stage + brand extraction).
            </p>
          </div>
          <div className="flex items-center gap-2 rounded-full border border-ink/10 bg-white px-4 py-2 text-xs">
            <span
              className={`h-2 w-2 rounded-full ${
                apiStatus === "ok"
                  ? "bg-emerald-500"
                  : apiStatus === "error"
                  ? "bg-red-500"
                  : "bg-slate-300"
              }`}
            />
            <span className="text-slate">{apiBaseUrl}</span>
          </div>
        </div>
      </section>

      <section className="grid gap-6 lg:grid-cols-[0.35fr_0.65fr]">
        <aside className="space-y-6">
          <div className="main-surface rounded-3xl p-6 space-y-4">
            <div>
              <p className="text-sm font-medium text-slate">Study</p>
              <select
                className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-4 py-2"
                value={selectedStudyId}
                onChange={(event) => setSelectedStudyId(event.target.value)}
              >
                {studies.map((study) => (
                  <option key={study.id} value={study.id}>
                    {study.id}
                  </option>
                ))}
              </select>
              <p className="mt-2 text-xs text-slate">
                {classification?.sector
                  ? `${classification.sector} → ${classification.subsector || "—"} → ${
                      classification.category || "—"
                    }`
                  : "Unassigned"}
              </p>
            </div>

            <div>
              <p className="text-sm font-medium text-slate">Search questions</p>
              <input
                className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-4 py-2 text-sm"
                value={searchText}
                onChange={(event) => setSearchText(event.target.value)}
                placeholder="Filter by keyword or var_code"
              />
            </div>

            <div className="flex flex-wrap gap-2">
              <button
                className={`rounded-full border px-3 py-1 text-xs ${
                  filterConoce
                    ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700"
                    : "border-ink/10"
                }`}
                onClick={() => setFilterConoce((prev) => !prev)}
                type="button"
              >
                Contains: conoce
              </button>
              <button
                className={`rounded-full border px-3 py-1 text-xs ${
                  filterCompra
                    ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700"
                    : "border-ink/10"
                }`}
                onClick={() => setFilterCompra((prev) => !prev)}
                type="button"
              >
                Contains: compra
              </button>
              <button
                className={`rounded-full border px-3 py-1 text-xs ${
                  filterUnmapped
                    ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700"
                    : "border-ink/10"
                }`}
                onClick={() => setFilterUnmapped((prev) => !prev)}
                type="button"
              >
                Unmapped only
              </button>
            </div>
          </div>

          <div className="main-surface rounded-3xl p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">Study Base Fields</h3>
              <span className="text-xs text-slate">{studyConfigState}</span>
            </div>
            <p className="text-xs text-slate">
              Choose respondent id and weight for this study.
            </p>
            {baseError && <p className="text-xs text-red-600">{baseError}</p>}
            <div>
              <p className="text-xs text-slate">Respondent ID</p>
              <select
                className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                value={selectedRespondentVar}
                onChange={(event) => setSelectedRespondentVar(event.target.value)}
              >
                <option value="__index__">__index__ (Row index fallback)</option>
                {studyVariables.map((item) => (
                  <option key={`rid-${item.var_code}`} value={item.var_code}>
                    {item.var_code}
                  </option>
                ))}
              </select>
              <p className="mt-1 text-[10px] text-slate">
                Source: {studyConfig?.respondent_id?.source || "auto"}
              </p>
            </div>
            <div>
              <p className="text-xs text-slate">Weight</p>
              <select
                className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                value={selectedWeightVar}
                onChange={(event) => setSelectedWeightVar(event.target.value)}
              >
                <option value="__default__">__default__ (1.0)</option>
                {studyVariables.map((item) => (
                  <option key={`w-${item.var_code}`} value={item.var_code}>
                    {item.var_code}
                  </option>
                ))}
              </select>
              <p className="mt-1 text-[10px] text-slate">
                Source: {studyConfig?.weight?.source || "auto"}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-medium text-white"
                onClick={handleSaveStudyConfig}
                type="button"
                disabled={studyVariablesState === "loading"}
              >
                Save
              </button>
              <button
                className="rounded-full border border-ink/10 px-4 py-2 text-xs font-medium"
                onClick={handleRebuildBase}
                type="button"
              >
                Rebuild base (apply changes)
              </button>
            </div>
            <div className="rounded-2xl border border-ink/10 bg-white p-3">
              <div className="flex items-center justify-between">
                <p className="text-xs font-medium text-slate">Preview</p>
                <span className="text-[10px] text-slate">{basePreviewState}</span>
              </div>
              {basePreviewRows.length === 0 ? (
                <p className="mt-2 text-xs text-slate">No preview rows available.</p>
              ) : (
                <div className="mt-2 space-y-1 text-xs text-slate">
                  {basePreviewRows.map((row, idx) => (
                    <div key={`base-${idx}`} className="flex items-center justify-between">
                      <span>{row.respondent_id ?? "--"}</span>
                      <span>{row.weight ?? "--"}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          <div className="main-surface rounded-3xl p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">Demographics Mapping</h3>
              <span className="text-xs text-slate">{demographicsState}</span>
            </div>
            <p className="text-xs text-slate">
              Map date, gender, age, NSE, and state using SAV value labels.
            </p>
            {demographicsError && <p className="text-xs text-red-600">{demographicsError}</p>}
            <div className="space-y-3 text-xs">
              <div>
                <p className="text-xs text-slate">Date</p>
                <div className="mt-2 flex flex-wrap gap-2 text-[10px]">
                  <span className="rounded-full border px-2 py-1 text-slate">
                    Date: {dateMode}
                  </span>
                </div>
                <select
                  className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                  value={dateMode}
                  onChange={(event) =>
                    handleDateModeChange(event.target.value as "none" | "var" | "constant")
                  }
                >
                  <option value="none">None</option>
                  <option value="var">From variable</option>
                  <option value="constant">Constant date</option>
                </select>
                {dateMode === "var" && (
                  <>
                    <select
                      className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                      value={dateVar}
                      onChange={async (event) => {
                        const value = event.target.value;
                        setDateVar(value);
                        const preview = await getDemographicsDatePreview(
                          selectedStudyId,
                          "var",
                          value,
                          null,
                          10
                        );
                        if (preview.ok && preview.data && typeof preview.data === "object") {
                          const data = preview.data as {
                            raw_samples?: string[];
                            parsed_samples?: Array<string | null>;
                            parse_success_rate?: number;
                          };
                          setDemographicsPreview((prev) => ({
                            ...prev,
                            date: {
                              rows: data.raw_samples || [],
                              parsed: data.parsed_samples || [],
                              rate: data.parse_success_rate ?? 0,
                            },
                          }));
                        }
                      }}
                    >
                      <option value="">Unassigned</option>
                      {studyVariables.map((item) => (
                        <option key={`date-${item.var_code}`} value={item.var_code}>
                          {item.var_code}
                        </option>
                      ))}
                    </select>
                    {demographicsPreview.date && demographicsPreview.date.rows.length > 0 && (
                      <div className="mt-2 rounded-xl border border-ink/10 bg-white p-2 text-[10px] text-slate">
                        <div>Raw: {demographicsPreview.date.rows.join(", ")}</div>
                        <div>
                          Parsed:{" "}
                          {(demographicsPreview.date.parsed || [])
                            .map((value) => value ?? "--")
                            .join(", ")}
                        </div>
                        <div>
                          Success rate: {Math.round((demographicsPreview.date.rate || 0) * 100)}%
                        </div>
                      </div>
                    )}
                  </>
                )}
                {dateMode === "constant" && (
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={dateConstant}
                    onChange={(event) => setDateConstant(event.target.value)}
                    placeholder="YYYY-MM-DD"
                  />
                )}
              </div>
              <div>
                <p className="text-xs text-slate">Gender</p>
                <select
                  className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                  value={demographicsConfig?.gender_var || ""}
                  onChange={(event) => handleSelectDemographicsVar("gender_var", event.target.value)}
                >
                  <option value="">Unassigned</option>
                  {studyVariables.map((item) => (
                    <option key={`gender-${item.var_code}`} value={item.var_code}>
                      {item.var_code}
                    </option>
                  ))}
                </select>
                {demographicsLabels.gender_var && demographicsLabels.gender_var.length > 0 && (
                  <div className="mt-2 max-h-24 overflow-auto rounded-xl border border-ink/10 bg-white p-2 text-[10px] text-slate">
                    {demographicsLabels.gender_var.slice(0, 12).map((label) => (
                      <div key={`gender-${label.value_code}`}>
                        {label.value_code}: {label.value_label}
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <div>
                <p className="text-xs text-slate">Age (numeric)</p>
                <select
                  className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                  value={demographicsConfig?.age_var || ""}
                  onChange={(event) => handleSelectDemographicsVar("age_var", event.target.value)}
                >
                  <option value="">Unassigned</option>
                  {studyVariables.map((item) => (
                    <option key={`age-${item.var_code}`} value={item.var_code}>
                      {item.var_code}
                    </option>
                  ))}
                </select>
                {demographicsPreview.age_var && demographicsPreview.age_var.rows.length > 0 && (
                  <div className="mt-2 rounded-xl border border-ink/10 bg-white p-2 text-[10px] text-slate">
                    <div>Sample: {demographicsPreview.age_var.rows.join(", ")}</div>
                    <div>
                      Range: {demographicsPreview.age_var.min ?? "--"} - {demographicsPreview.age_var.max ?? "--"}
                    </div>
                  </div>
                )}
              </div>
              <div>
                <p className="text-xs text-slate">NSE</p>
                <select
                  className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                  value={demographicsConfig?.nse_var || ""}
                  onChange={(event) => handleSelectDemographicsVar("nse_var", event.target.value)}
                >
                  <option value="">Unassigned</option>
                  {studyVariables.map((item) => (
                    <option key={`nse-${item.var_code}`} value={item.var_code}>
                      {item.var_code}
                    </option>
                  ))}
                </select>
                {demographicsLabels.nse_var && demographicsLabels.nse_var.length > 0 && (
                  <div className="mt-2 max-h-24 overflow-auto rounded-xl border border-ink/10 bg-white p-2 text-[10px] text-slate">
                    {demographicsLabels.nse_var.slice(0, 12).map((label) => (
                      <div key={`nse-${label.value_code}`}>
                        {label.value_code}: {label.value_label}
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <div>
                <p className="text-xs text-slate">State</p>
                <select
                  className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                  value={demographicsConfig?.state_var || ""}
                  onChange={(event) => handleSelectDemographicsVar("state_var", event.target.value)}
                >
                  <option value="">Unassigned</option>
                  {studyVariables.map((item) => (
                    <option key={`state-${item.var_code}`} value={item.var_code}>
                      {item.var_code}
                    </option>
                  ))}
                </select>
                {demographicsLabels.state_var && demographicsLabels.state_var.length > 0 && (
                  <div className="mt-2 max-h-24 overflow-auto rounded-xl border border-ink/10 bg-white p-2 text-[10px] text-slate">
                    {demographicsLabels.state_var.slice(0, 12).map((label) => (
                      <div key={`state-${label.value_code}`}>
                        {label.value_code}: {label.value_label}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-medium text-white"
                onClick={handleSaveDemographics}
                type="button"
              >
                Save demographics
              </button>
              <button
                className="rounded-full border border-ink/10 px-4 py-2 text-xs font-medium"
                onClick={handleClearDemographics}
                type="button"
              >
                Clear
              </button>
            </div>
          </div>

          <div className="main-surface rounded-3xl p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">Coverage</h3>
              <span className="text-xs text-slate">{coverageState}</span>
            </div>
            <div className="grid gap-3 text-sm">
              <div className="rounded-2xl border border-ink/10 bg-white p-3">
                <p className="text-xs text-slate">Mapped</p>
                <p className="text-xl font-semibold text-ink">
                  {coverageData?.mapped_rows ?? "-"}
                </p>
              </div>
              <div className="rounded-2xl border border-ink/10 bg-white p-3">
                <p className="text-xs text-slate">Unmapped</p>
                <p className="text-xl font-semibold text-ink">
                  {coverageData?.unmapped_rows ?? "-"}
                </p>
              </div>
              <div className="rounded-2xl border border-ink/10 bg-white p-3">
                <p className="text-xs text-slate">Ignored</p>
                <p className="text-xl font-semibold text-ink">
                  {coverageData?.ignored_rows ?? "-"}
                </p>
              </div>
            </div>
            <button
              className="rounded-full border border-ink/10 px-4 py-2 text-xs font-medium"
              onClick={handleRecomputeCoverage}
              type="button"
            >
              Recompute coverage
            </button>
          </div>

          <div className="main-surface rounded-3xl p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">Study Classification</h3>
              <span className="text-xs text-slate">{classificationState}</span>
            </div>
            {classificationError && <p className="text-xs text-red-600">{classificationError}</p>}
            <div>
              <p className="text-xs text-slate">Sector</p>
              <select
                className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                value={sector}
                onChange={(event) => {
                  setSector(event.target.value);
                  setSubsector("");
                  setCategory("");
                }}
              >
                <option value="">Unassigned</option>
                {sectors.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <p className="text-xs text-slate">Subsector</p>
              <select
                className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                value={subsector}
                onChange={(event) => {
                  setSubsector(event.target.value);
                  setCategory("");
                }}
                disabled={!sector}
              >
                <option value="">Unassigned</option>
                {subsectors.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <p className="text-xs text-slate">Category</p>
              <select
                className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                value={category}
                onChange={(event) => setCategory(event.target.value)}
                disabled={!sector || !subsector}
              >
                <option value="">Unassigned</option>
                {categories.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-medium text-white"
                onClick={handleSaveClassification}
                type="button"
                disabled={taxonomyState === "loading"}
              >
                Save classification
              </button>
              <button
                className="rounded-full border border-ink/10 px-4 py-2 text-xs font-medium"
                onClick={handleClearClassification}
                type="button"
              >
                Clear
              </button>
            </div>
          </div>

          <div className="main-surface rounded-3xl p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">Rule Scope for this Study</h3>
              <span className="text-xs text-slate">{scopeState}</span>
            </div>
            {scopeError && <p className="text-xs text-amber-600">{scopeError}</p>}
            <div className="space-y-3 text-xs">
              <div>
                <p className="text-xs font-semibold text-slate">Stage Rules</p>
                <div className="mt-2 space-y-2">
                  {rulesPayload?.stage_rules?.map((rule) => {
                    const id = String(rule.id);
                    const checked = ruleScope
                      ? ruleScope.enabled_stage_rules.includes(id)
                      : true;
                    return (
                      <label key={id} className="flex items-start gap-2 rounded-xl border border-ink/10 bg-white px-3 py-2">
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => handleToggleScope("stage", id)}
                        />
                        <span>
                          <span className="block text-ink">{id}</span>
                          <span className="block text-[10px] text-slate">
                            {stageLabel(String(rule.stage))} · {String(rule.question_text_regex || "-")}
                          </span>
                        </span>
                      </label>
                    );
                  })}
                </div>
              </div>
              <div>
                <p className="text-xs font-semibold text-slate">Brand Extractors</p>
                <div className="mt-2 space-y-2">
                  {rulesPayload?.brand_extractors?.map((rule) => {
                    const id = String(rule.id);
                    const checked = ruleScope
                      ? ruleScope.enabled_brand_extractors.includes(id)
                      : true;
                    return (
                      <label key={id} className="flex items-start gap-2 rounded-xl border border-ink/10 bg-white px-3 py-2">
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => handleToggleScope("brand", id)}
                        />
                        <span>
                          <span className="block text-ink">{id}</span>
                          <span className="block text-[10px] text-slate">
                            {String(rule.applies_if_question_text_regex || "-")}
                          </span>
                        </span>
                      </label>
                    );
                  })}
                </div>
              </div>
              <div>
                <p className="text-xs font-semibold text-slate">Ignore Rules</p>
                <div className="mt-2 space-y-2">
                  {rulesPayload?.ignore_rules?.map((rule) => {
                    const id = String(rule.id);
                    const checked = ruleScope
                      ? ruleScope.enabled_ignore_rules.includes(id)
                      : true;
                    return (
                      <label key={id} className="flex items-start gap-2 rounded-xl border border-ink/10 bg-white px-3 py-2">
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => handleToggleScope("ignore", id)}
                        />
                        <span>
                          <span className="block text-ink">{id}</span>
                          <span className="block text-[10px] text-slate">
                            {String(rule.question_text_regex || "-")}
                          </span>
                        </span>
                      </label>
                    );
                  })}
                </div>
              </div>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                className="rounded-full border border-ink/10 px-4 py-2 text-xs font-medium"
                onClick={handleEnableAllScope}
                type="button"
              >
                Enable all
              </button>
              <button
                className="rounded-full border border-ink/10 px-4 py-2 text-xs font-medium"
                onClick={handleDisableAllScope}
                type="button"
              >
                Disable all
              </button>
              <button
                className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-medium text-white"
                onClick={handleSaveScope}
                type="button"
              >
                Save scope
              </button>
            </div>
          </div>

          <div className="main-surface rounded-3xl p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">Publish Journey Results</h3>
              <span className="text-xs text-slate">{publishState}</span>
            </div>
            <p className="text-xs text-slate">
              Build mapping and curated mart so Journey can display real results.
            </p>
            <div className="flex flex-wrap gap-2">
              <span className="rounded-full border px-3 py-1 text-[10px]">
                RAW: {publishStatus?.raw_ready ? "ready" : "missing"}
              </span>
              <span className="rounded-full border px-3 py-1 text-[10px]">
                Mapping: {publishStatus?.mapping_ready ? "ready" : "missing"}
              </span>
              <span className="rounded-full border px-3 py-1 text-[10px]">
                Curated: {publishStatus?.curated_ready ? "ready" : "missing"}
              </span>
            </div>
            <div className="flex flex-wrap gap-2">
              <button
                className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-medium text-white"
                onClick={() => handlePublish(false)}
                type="button"
              >
                Build / Publish
              </button>
              <button
                className="rounded-full border border-ink/10 px-4 py-2 text-xs font-medium"
                onClick={() => handlePublish(true)}
                type="button"
              >
                Rebuild (force)
              </button>
            </div>
            <button
              className="text-xs font-medium text-slate"
              onClick={() => setShowPublishDetails((prev) => !prev)}
              type="button"
            >
              {showPublishDetails ? "Hide" : "Last build details"}
            </button>
            {showPublishDetails && publishDetails && (
              <pre className="max-h-48 overflow-auto rounded-2xl bg-black/90 p-3 text-[10px] text-white">
                {formatJson(publishDetails)}
              </pre>
            )}
          </div>
        </aside>

        <section className="space-y-6">
          <div className="main-surface rounded-3xl p-6">
            <div className="flex flex-wrap gap-2">
              <button
                className={`rounded-full px-4 py-2 text-sm ${
                  activeTab === "questions"
                    ? "bg-emerald-600 text-white"
                    : "border border-ink/10 bg-white"
                }`}
                onClick={() => setActiveTab("questions")}
                type="button"
              >
                Questions
              </button>
              <button
                className={`rounded-full px-4 py-2 text-sm ${
                  activeTab === "builder"
                    ? "bg-emerald-600 text-white"
                    : "border border-ink/10 bg-white"
                }`}
                onClick={() => setActiveTab("builder")}
                type="button"
              >
                Rule Builder
              </button>
              <button
                className={`rounded-full px-4 py-2 text-sm ${
                  activeTab === "mapper"
                    ? "bg-emerald-600 text-white"
                    : "border border-ink/10 bg-white"
                }`}
                onClick={() => setActiveTab("mapper")}
                type="button"
              >
                Question Mapper
              </button>
              <button
                className={`rounded-full px-4 py-2 text-sm ${
                  activeTab === "advanced"
                    ? "bg-emerald-600 text-white"
                    : "border border-ink/10 bg-white"
                }`}
                onClick={() => setActiveTab("advanced")}
                type="button"
              >
                Advanced JSON
              </button>
            </div>
          </div>

          {activeTab === "questions" && (
            <div className="main-surface rounded-3xl p-6">
              <div className="flex items-center justify-between">
                <h3 className="text-xl font-semibold">Questions</h3>
                <span className="text-xs text-slate">{questionsState}</span>
              </div>
              <div className="mt-4 space-y-3 text-sm">
                {filteredQuestions.length === 0 ? (
                  <p className="text-sm text-slate">No questions match the current filters.</p>
                ) : (
                  filteredQuestions.map((item) => (
                    <div
                      key={item.var_code}
                      className="flex flex-col gap-2 rounded-2xl border border-ink/10 bg-white p-4"
                    >
                      <div className="flex items-center justify-between">
                        <p className="text-xs text-slate">{item.var_code}</p>
                        <div className="flex items-center gap-2 text-[10px]">
                          <span
                            className={`rounded-full px-2 py-0.5 ${
                              item.stage_mapped
                                ? "bg-emerald-500/10 text-emerald-700"
                                : "bg-slate-100 text-slate-500"
                            }`}
                          >
                            Stage: {stageLabel(item.mapped_stage)}
                          </span>
                          <span
                            className={`rounded-full px-2 py-0.5 ${
                              item.brand_mapped
                                ? "bg-emerald-500/10 text-emerald-700"
                                : "bg-slate-100 text-slate-500"
                            }`}
                          >
                            Brand: {item.brand_mapped ? item.mapped_brand_example || "ok" : "--"}
                          </span>
                          <span
                            className={`rounded-full px-2 py-0.5 ${
                              item.touchpoint_mapped
                                ? "bg-emerald-500/10 text-emerald-700"
                                : "bg-slate-100 text-slate-500"
                            }`}
                          >
                            Touchpoint: {item.mapped_touchpoint || "--"}
                          </span>
                        </div>
                      </div>
                      <p className="text-sm text-ink">
                        {item.question_text || "(no question text)"}
                      </p>
                      <p className="text-xs text-slate">
                        {item.value_preview
                          ? `Values: ${
                              item.value_preview.top_values.length
                                ? item.value_preview.top_values
                                    .map((value) => `${value.value} (${value.count})`)
                                    .join(", ")
                                : "n/a"
                            } | distinct=${item.value_preview.distinct}`
                          : "Values: n/a"}
                      </p>
                      <button
                        className="self-start text-xs font-medium text-emerald-700"
                        onClick={() => handleUseAsPattern(item)}
                        type="button"
                      >
                        Use as pattern
                      </button>
                    </div>
                  ))
                )}
              </div>
            </div>
          )}

          {activeTab === "mapper" && (
            <div className="grid gap-6 lg:grid-cols-[2.2fr_1fr]">
              <div className="main-surface rounded-3xl p-6">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <h3 className="text-xl font-semibold">Question Mapper</h3>
                    <p className="text-xs text-slate">
                      Manual mappings are preserved; rule suggestions fill blanks only.
                    </p>
                  </div>
                  <button
                    className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-medium text-white"
                    onClick={handleApplySuggestions}
                    type="button"
                  >
                    Apply suggestions (fill blanks)
                  </button>
                </div>

                <div className="mt-4 flex flex-wrap items-center gap-3 text-xs">
                  <input
                    className="w-56 rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    placeholder="Search questions..."
                    value={questionMapSearch}
                    onChange={(event) => setQuestionMapSearch(event.target.value)}
                  />
                  <label className="flex items-center gap-2 text-xs text-slate">
                    <input
                      checked={questionMapUnmappedOnly}
                      onChange={(event) => setQuestionMapUnmappedOnly(event.target.checked)}
                      type="checkbox"
                    />
                    Unmapped only
                  </label>
                  <span className="text-xs text-slate">Status: {questionMapState}</span>
                </div>

                <div className="mt-4 flex flex-wrap gap-3 rounded-2xl border border-ink/10 bg-white p-3 text-xs">
                  <div className="flex items-center gap-2">
                    <select
                      className="rounded-lg border border-ink/10 bg-white px-2 py-1"
                      value={bulkStage}
                      onChange={(event) => setBulkStage(event.target.value)}
                    >
                      <option value="">Stage...</option>
                      {STAGES.filter((stage) => stage.value !== "none").map((stage) => (
                        <option key={stage.value} value={stage.value}>
                          {stage.label}
                        </option>
                      ))}
                    </select>
                    <button
                      className="rounded-lg border border-ink/10 px-2 py-1"
                      onClick={() =>
                        handleBulkUpdate(
                          { stage: "manual" },
                          { stage: bulkStage || null }
                        )
                      }
                      type="button"
                    >
                      Apply Stage
                    </button>
                    <button
                      className="rounded-lg border border-ink/10 px-2 py-1"
                      onClick={() => handleBulkUpdate({ stage: "clear" }, { stage: null })}
                      type="button"
                    >
                      Clear Stage
                    </button>
                  </div>
                  <div className="flex items-center gap-2">
                    <input
                      className="rounded-lg border border-ink/10 bg-white px-2 py-1"
                      placeholder="Brand"
                      value={bulkBrand}
                      onChange={(event) => setBulkBrand(event.target.value)}
                    />
                    <button
                      className="rounded-lg border border-ink/10 px-2 py-1"
                      onClick={() =>
                        handleBulkUpdate(
                          { brand: "manual" },
                          { brand_value: bulkBrand || null }
                        )
                      }
                      type="button"
                    >
                      Apply Brand
                    </button>
                    <button
                      className="rounded-lg border border-ink/10 px-2 py-1"
                      onClick={() => handleBulkUpdate({ brand: "clear" }, { brand_value: null })}
                      type="button"
                    >
                      Clear Brand
                    </button>
                  </div>
                  <div className="flex items-center gap-2">
                    <input
                      className="rounded-lg border border-ink/10 bg-white px-2 py-1"
                      placeholder="Touchpoint"
                      value={bulkTouchpoint}
                      onChange={(event) => setBulkTouchpoint(event.target.value)}
                    />
                    <button
                      className="rounded-lg border border-ink/10 px-2 py-1"
                      onClick={() =>
                        handleBulkUpdate(
                          { touchpoint: "manual" },
                          { touchpoint_value: bulkTouchpoint || null }
                        )
                      }
                      type="button"
                    >
                      Apply Touchpoint
                    </button>
                    <button
                      className="rounded-lg border border-ink/10 px-2 py-1"
                      onClick={() =>
                        handleBulkUpdate({ touchpoint: "clear" }, { touchpoint_value: null })
                      }
                      type="button"
                    >
                      Clear Touchpoint
                    </button>
                  </div>
                </div>

                <div className="mt-4 max-h-[520px] overflow-auto rounded-2xl border border-ink/10">
                  <table className="w-full border-collapse text-xs">
                    <thead>
                      <tr className="border-b border-ink/10 bg-white text-left">
                        <th className="px-3 py-2">
                          <input
                            type="checkbox"
                            checked={
                              questionMapRows.length > 0 &&
                              questionMapSelection.size === questionMapRows.length
                            }
                            onChange={(event) => handleSelectAllQuestionMap(event.target.checked)}
                          />
                        </th>
                        <th className="px-3 py-2">Var</th>
                        <th className="px-3 py-2">Question</th>
                        <th className="px-3 py-2">Stage</th>
                        <th className="px-3 py-2">Brand</th>
                        <th className="px-3 py-2">Touchpoint</th>
                      </tr>
                    </thead>
                    <tbody>
                      {questionMapRows.map((row) => (
                        <tr
                          key={row.var_code}
                          className="border-b border-ink/5 hover:bg-emerald-500/5"
                          onClick={() => handlePreview(row)}
                        >
                          <td className="px-3 py-2">
                            <input
                              type="checkbox"
                              checked={questionMapSelection.has(row.var_code)}
                              onChange={(event) =>
                                handleToggleQuestionMapRow(row.var_code, event.target.checked)
                              }
                            />
                          </td>
                          <td className="px-3 py-2 text-slate">{row.var_code}</td>
                          <td className="px-3 py-2">{row.question_text || "--"}</td>
                          <td className="px-3 py-2">
                            <span className="rounded-full bg-emerald-500/10 px-2 py-0.5 text-[10px] text-emerald-700">
                              {stageLabel(String(row.stage || ""))}
                            </span>
                            <span className="ml-2 text-[10px] text-slate">
                              {row.source_stage || "empty"}
                            </span>
                          </td>
                          <td className="px-3 py-2">
                            <span className="rounded-full bg-emerald-500/10 px-2 py-0.5 text-[10px] text-emerald-700">
                              {row.brand_value || "--"}
                            </span>
                            <span className="ml-2 text-[10px] text-slate">
                              {row.source_brand || "empty"}
                            </span>
                          </td>
                          <td className="px-3 py-2">
                            <span className="rounded-full bg-emerald-500/10 px-2 py-0.5 text-[10px] text-emerald-700">
                              {row.touchpoint_value || "--"}
                            </span>
                            <span className="ml-2 text-[10px] text-slate">
                              {row.source_touchpoint || "empty"}
                            </span>
                          </td>
                        </tr>
                      ))}
                      {questionMapRows.length === 0 && (
                        <tr>
                          <td className="px-3 py-4 text-center text-slate" colSpan={6}>
                            No question map rows yet.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="main-surface rounded-3xl p-6 space-y-3">
                <h4 className="text-lg font-semibold">Value Preview</h4>
                {previewRow ? (
                  <>
                    <p className="text-xs text-slate">{previewRow.var_code}</p>
                    <p className="text-sm">{previewRow.question_text || "--"}</p>
                    <div className="rounded-2xl border border-ink/10 bg-white p-3 text-xs text-slate">
                      {previewItems || "No values available."}
                    </div>
                  </>
                ) : (
                  <p className="text-xs text-slate">Select a row to preview values.</p>
                )}
              </div>
            </div>
          )}

          {activeTab === "builder" && (
            <div className="space-y-6">
              <div className="main-surface rounded-3xl p-6 space-y-4">
                <h3 className="text-xl font-semibold">Stage Rule</h3>
                <div>
                  <p className="text-xs text-slate">Rule name (id)</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={stageForm.id}
                    onChange={(event) =>
                      setStageForm((prev) => ({ ...prev, id: event.target.value }))
                    }
                    placeholder="stage_awareness_001"
                  />
                </div>
                <div className="grid gap-3 md:grid-cols-2">
                  <div>
                    <p className="text-xs text-slate">Stage</p>
                    <select
                      className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                      value={stageForm.stage}
                      onChange={(event) =>
                        setStageForm((prev) => ({ ...prev, stage: event.target.value }))
                      }
                    >
                      {STAGES.map((stage) => (
                        <option key={stage.value} value={stage.value}>
                          {stage.label}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <p className="text-xs text-slate">Priority</p>
                    <input
                      className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                      type="number"
                      value={stageForm.priority}
                      onChange={(event) =>
                        setStageForm((prev) => ({
                          ...prev,
                          priority: Number(event.target.value),
                        }))
                      }
                    />
                  </div>
                </div>
                <div>
                  <p className="text-xs text-slate">Question text regex</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={stageForm.question_text_regex}
                    onChange={(event) =>
                      setStageForm((prev) => ({
                        ...prev,
                        question_text_regex: event.target.value,
                      }))
                    }
                  />
                </div>
                <div>
                  <p className="text-xs text-slate">Var code regex (optional)</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={stageForm.var_code_regex}
                    onChange={(event) =>
                      setStageForm((prev) => ({ ...prev, var_code_regex: event.target.value }))
                    }
                  />
                </div>
                <button
                  className="rounded-full bg-emerald-600 px-5 py-2 text-sm font-medium text-white"
                  onClick={handleAddStageRule}
                  type="button"
                >
                  {editingRule?.type === "stage" ? "Update Stage Rule" : "Add/Update Stage Rule"}
                </button>
                {editingRule?.type === "stage" && (
                  <button
                    className="rounded-full border border-ink/10 px-5 py-2 text-sm font-medium"
                    onClick={handleCancelEdit}
                    type="button"
                  >
                    Cancel edit
                  </button>
                )}
              </div>

              <div className="main-surface rounded-3xl p-6 space-y-4">
                <h3 className="text-xl font-semibold">Brand Extraction</h3>
                <div>
                  <p className="text-xs text-slate">Rule name (id)</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={brandForm.id}
                    onChange={(event) =>
                      setBrandForm((prev) => ({ ...prev, id: event.target.value }))
                    }
                    placeholder="brand_extractor_001"
                  />
                </div>
                <div>
                  <p className="text-xs text-slate">Applies if question regex</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={brandForm.applies_if_question_text_regex}
                    onChange={(event) =>
                      setBrandForm((prev) => ({
                        ...prev,
                        applies_if_question_text_regex: event.target.value,
                      }))
                    }
                  />
                </div>
                <div>
                  <p className="text-xs text-slate">Mode</p>
                  <select
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={brandForm.mode}
                    onChange={(event) => {
                      const mode = event.target.value as BrandExtractorForm["mode"];
                      setBrandForm((prev) => {
                        if (mode === "start") {
                          return { ...prev, mode, extract_regex: "^\\s*(.+?)\\s*[-:]" };
                        }
                        if (mode === "end") {
                          return { ...prev, mode, extract_regex: "\\?\\s*(.+)$" };
                        }
                        if (mode === "between") {
                          return { ...prev, mode };
                        }
                        return { ...prev, mode };
                      });
                    }}
                  >
                    <option value="end">End of question</option>
                    <option value="start">Start of question</option>
                    <option value="between">Between delimiters</option>
                    <option value="regex">Custom regex</option>
                  </select>
                </div>
                {brandForm.mode === "between" && (
                  <div className="space-y-3">
                    <div>
                      <p className="text-xs text-slate">Left delimiter regex</p>
                      <input
                        className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                        value={brandForm.between_left}
                        onChange={(event) =>
                          setBrandForm((prev) => ({
                            ...prev,
                            between_left: event.target.value,
                          }))
                        }
                      />
                    </div>
                    <div>
                      <p className="text-xs text-slate">Right delimiter regex</p>
                      <input
                        className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                        value={brandForm.between_right}
                        onChange={(event) =>
                          setBrandForm((prev) => ({
                            ...prev,
                            between_right: event.target.value,
                          }))
                        }
                      />
                    </div>
                    <p className="text-[11px] text-slate">
                      Preview: {buildBetweenRegex(brandForm.between_left, brandForm.between_right)}
                    </p>
                  </div>
                )}
                <div className="grid gap-3 md:grid-cols-[2fr_1fr]">
                  <div>
                    <p className="text-xs text-slate">Extract regex</p>
                    <input
                      className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                      value={brandForm.extract_regex}
                      onChange={(event) =>
                        setBrandForm((prev) => ({ ...prev, extract_regex: event.target.value }))
                      }
                    />
                  </div>
                  <div>
                    <p className="text-xs text-slate">Group</p>
                    <input
                      className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                      type="number"
                      value={brandForm.extract_group}
                      onChange={(event) =>
                        setBrandForm((prev) => ({
                          ...prev,
                          extract_group: Number(event.target.value),
                        }))
                      }
                    />
                  </div>
                </div>
                <label className="flex items-center gap-2 text-xs text-slate">
                  <input
                    checked={brandForm.normalize}
                    onChange={(event) =>
                      setBrandForm((prev) => ({ ...prev, normalize: event.target.checked }))
                    }
                    type="checkbox"
                  />
                  Normalize brand labels
                </label>
                <button
                  className="rounded-full bg-emerald-600 px-5 py-2 text-sm font-medium text-white"
                  onClick={handleAddBrandExtractor}
                  type="button"
                >
                  {editingRule?.type === "brand" ? "Update Brand Extractor" : "Add/Update Brand Extractor"}
                </button>
                {editingRule?.type === "brand" && (
                  <button
                    className="rounded-full border border-ink/10 px-5 py-2 text-sm font-medium"
                    onClick={handleCancelEdit}
                    type="button"
                  >
                    Cancel edit
                  </button>
                )}
              </div>

              <div className="main-surface rounded-3xl p-6 space-y-4">
                <h3 className="text-xl font-semibold">Touchpoint Rules</h3>
                <div>
                  <p className="text-xs text-slate">Rule name (id)</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={touchpointForm.id}
                    onChange={(event) =>
                      setTouchpointForm((prev) => ({ ...prev, id: event.target.value }))
                    }
                    placeholder="tp_facebook"
                  />
                </div>
                <div>
                  <p className="text-xs text-slate">Touchpoint label</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={touchpointForm.touchpoint}
                    onChange={(event) =>
                      setTouchpointForm((prev) => ({ ...prev, touchpoint: event.target.value }))
                    }
                    placeholder="Facebook"
                  />
                </div>
                <div>
                  <p className="text-xs text-slate">Question text regex</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={touchpointForm.question_regex}
                    onChange={(event) =>
                      setTouchpointForm((prev) => ({
                        ...prev,
                        question_regex: event.target.value,
                      }))
                    }
                  />
                </div>
                <div>
                  <p className="text-xs text-slate">Var code regex (optional)</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={touchpointForm.var_code_regex}
                    onChange={(event) =>
                      setTouchpointForm((prev) => ({
                        ...prev,
                        var_code_regex: event.target.value,
                      }))
                    }
                  />
                </div>
                <div>
                  <p className="text-xs text-slate">Priority</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    type="number"
                    value={touchpointForm.priority}
                    onChange={(event) =>
                      setTouchpointForm((prev) => ({
                        ...prev,
                        priority: Number(event.target.value),
                      }))
                    }
                  />
                </div>
                <button
                  className="rounded-full bg-emerald-600 px-5 py-2 text-sm font-medium text-white"
                  onClick={handleAddTouchpointRule}
                  type="button"
                >
                  {editingRule?.type === "touchpoint"
                    ? "Update Touchpoint Rule"
                    : "Add/Update Touchpoint Rule"}
                </button>
                {editingRule?.type === "touchpoint" && (
                  <button
                    className="rounded-full border border-ink/10 px-5 py-2 text-sm font-medium"
                    onClick={handleCancelEdit}
                    type="button"
                  >
                    Cancel edit
                  </button>
                )}
              </div>

              <div className="main-surface rounded-3xl p-6 space-y-4">
                <h3 className="text-xl font-semibold">Ignore Rule</h3>
                <div>
                  <p className="text-xs text-slate">Question text regex</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={ignoreForm.question_text_regex}
                    onChange={(event) =>
                      setIgnoreForm((prev) => ({
                        ...prev,
                        question_text_regex: event.target.value,
                      }))
                    }
                  />
                </div>
                <div>
                  <p className="text-xs text-slate">Var code regex</p>
                  <input
                    className="mt-2 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    value={ignoreForm.var_code_regex}
                    onChange={(event) =>
                      setIgnoreForm((prev) => ({
                        ...prev,
                        var_code_regex: event.target.value,
                      }))
                    }
                  />
                </div>
                <button
                  className="rounded-full bg-emerald-600 px-5 py-2 text-sm font-medium text-white"
                  onClick={handleAddIgnoreRule}
                  type="button"
                >
                  {editingRule?.type === "ignore" ? "Update Ignore Rule" : "Add Ignore Rule"}
                </button>
                {editingRule?.type === "ignore" && (
                  <button
                    className="rounded-full border border-ink/10 px-5 py-2 text-sm font-medium"
                    onClick={handleCancelEdit}
                    type="button"
                  >
                    Cancel edit
                  </button>
                )}
              </div>

              <div className="main-surface rounded-3xl p-6 space-y-4">
                <h3 className="text-xl font-semibold">Rules Preview</h3>
                <div className="space-y-3 text-xs">
                  <div>
                    <p className="text-xs font-semibold text-slate">Stage Rules</p>
                    <div className="mt-2 space-y-2">
                      {rulesPayload?.stage_rules?.map((rule) => (
                        <div
                          key={String(rule.id)}
                          className="flex items-center justify-between rounded-xl border border-ink/10 bg-white px-3 py-2"
                        >
                          <div>
                            <p className="text-ink">{String(rule.id)}</p>
                            <p className="text-slate">
                              {stageLabel(String(rule.stage))} - {String(rule.question_text_regex || "-")}
                            </p>
                          </div>
                          <div className="flex items-center gap-3">
                            <button
                              className="text-xs font-medium text-emerald-700"
                              onClick={() => handleEditRule("stage", rule)}
                              type="button"
                            >
                              Edit
                            </button>
                            <button
                              className="text-xs font-medium text-red-600"
                              onClick={() => handleDeleteRule("stage", String(rule.id))}
                              type="button"
                            >
                              Delete
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div>
                    <p className="text-xs font-semibold text-slate">Brand Extractors</p>
                    <div className="mt-2 space-y-2">
                      {rulesPayload?.brand_extractors?.map((rule) => (
                        <div
                          key={String(rule.id)}
                          className="flex items-center justify-between rounded-xl border border-ink/10 bg-white px-3 py-2"
                        >
                          <div>
                            <p className="text-ink">{String(rule.id)}</p>
                            <p className="text-slate">
                              {String(
                                rule.applies_if_question_regex || rule.applies_if_question_text_regex || "-"
                              )}
                            </p>
                            <p className="text-[10px] text-slate">
                              Mode: {String(rule.mode || "regex")}
                            </p>
                          </div>
                          <div className="flex items-center gap-3">
                            <button
                              className="text-xs font-medium text-emerald-700"
                              onClick={() => handleEditRule("brand", rule)}
                              type="button"
                            >
                              Edit
                            </button>
                            <button
                              className="text-xs font-medium text-red-600"
                              onClick={() => handleDeleteRule("brand", String(rule.id))}
                              type="button"
                            >
                              Delete
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div>
                    <p className="text-xs font-semibold text-slate">Touchpoint Rules</p>
                    <div className="mt-2 space-y-2">
                      {(rulesPayload?.touchpoint_rules || []).map((rule) => (
                        <div
                          key={String(rule.id)}
                          className="flex items-center justify-between rounded-xl border border-ink/10 bg-white px-3 py-2"
                        >
                          <div>
                            <p className="text-ink">{String(rule.id)}</p>
                            <p className="text-slate">
                              {String(rule.touchpoint || "-")} ·{" "}
                              {String(rule.question_regex || rule.question_text_regex || "-")}
                            </p>
                          </div>
                          <div className="flex items-center gap-3">
                            <button
                              className="text-xs font-medium text-emerald-700"
                              onClick={() => handleEditRule("touchpoint", rule)}
                              type="button"
                            >
                              Edit
                            </button>
                            <button
                              className="text-xs font-medium text-red-600"
                              onClick={() => handleDeleteRule("touchpoint", String(rule.id))}
                              type="button"
                            >
                              Delete
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                  <div>
                    <p className="text-xs font-semibold text-slate">Ignore Rules</p>
                    <div className="mt-2 space-y-2">
                      {rulesPayload?.ignore_rules?.map((rule) => (
                        <div
                          key={String(rule.id)}
                          className="flex items-center justify-between rounded-xl border border-ink/10 bg-white px-3 py-2"
                        >
                          <div>
                            <p className="text-ink">{String(rule.id)}</p>
                            <p className="text-slate">
                              {String(rule.question_text_regex || "-")}
                            </p>
                          </div>
                          <div className="flex items-center gap-3">
                            <button
                              className="text-xs font-medium text-emerald-700"
                              onClick={() => handleEditRule("ignore", rule)}
                              type="button"
                            >
                              Edit
                            </button>
                            <button
                              className="text-xs font-medium text-red-600"
                              onClick={() => handleDeleteRule("ignore", String(rule.id))}
                              type="button"
                            >
                              Delete
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>

              <div className="main-surface rounded-3xl p-6 space-y-4">
                <div className="flex flex-wrap gap-3">
                  <button
                    className="rounded-full border border-ink/10 px-5 py-2 text-sm font-medium"
                    onClick={handleSaveRules}
                    type="button"
                  >
                    Save Rules
                  </button>
                  <button
                    className="rounded-full bg-emerald-600 px-5 py-2 text-sm font-medium text-white"
                    onClick={handleRunRules}
                    type="button"
                    disabled={!selectedStudyId}
                  >
                    Run Rules -> Generate Mapping
                  </button>
                </div>
                {runState === "success" && (
                  <p className="text-xs text-emerald-700">Rules applied successfully.</p>
                )}
                {runState === "error" && (
                  <p className="text-xs text-red-600">Rules run failed. Check details.</p>
                )}
              </div>
            </div>
          )}

          {activeTab === "advanced" && (
            <div className="main-surface rounded-3xl p-6 space-y-4">
              <h3 className="text-xl font-semibold">Rules JSON (Advanced)</h3>
              <textarea
                className="min-h-[260px] w-full rounded-2xl border border-ink/10 bg-white p-3 font-mono text-xs"
                value={rulesJson}
                onChange={(event) => setRulesJson(event.target.value)}
              />
              {rulesError && <p className="text-xs text-red-600">{rulesError}</p>}
              <div className="flex flex-wrap gap-3">
                <button
                  className="rounded-full border border-ink/10 px-5 py-2 text-sm font-medium"
                  onClick={handleReloadRules}
                  type="button"
                >
                  Reload rules
                </button>
                <button
                  className="rounded-full bg-emerald-600 px-5 py-2 text-sm font-medium text-white"
                  onClick={handleSaveRules}
                  type="button"
                >
                  Save rules
                </button>
              </div>
            </div>
          )}

          <div className="main-surface rounded-3xl p-6 space-y-3">
            <div className="flex items-center justify-between">
              <h4 className="text-lg font-semibold">Last Response</h4>
              <button
                className="text-xs font-medium text-slate"
                onClick={() => setShowDetails((prev) => !prev)}
                type="button"
              >
                {showDetails ? "Hide" : "Details"}
              </button>
            </div>
            {showDetails && (
              <pre className="max-h-64 overflow-auto rounded-2xl bg-black/90 p-4 text-xs text-white">
                {formatJson(lastResponse || apiResult || runResult)}
              </pre>
            )}
          </div>
        </section>
      </section>
    </main>
  );
}
