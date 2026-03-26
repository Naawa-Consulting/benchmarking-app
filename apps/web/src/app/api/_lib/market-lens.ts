type MarketLens = {
  market_sector: string;
  market_subsector: string;
  market_category: string;
};

type DeriveInput = {
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
};

function normalize(value: unknown) {
  if (typeof value !== "string") return "";
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function valueOr(defaultValue: string, value: unknown) {
  if (typeof value !== "string") return defaultValue;
  const trimmed = value.trim();
  return trimmed || defaultValue;
}

function isGenericMarketFallback(market: MarketLens) {
  const sector = normalize(market.market_sector);
  const subsector = normalize(market.market_subsector);
  const category = normalize(market.market_category);
  if (
    sector === "industry & manufacturing" &&
    subsector === "manufacturing" &&
    category === "general industry"
  ) {
    return true;
  }
  if (sector === "services" && subsector === "consumer services" && category === "general services") {
    return true;
  }
  if (sector === "retail & commerce" && subsector === "retail" && category === "general retail") {
    return true;
  }
  return false;
}

const CATEGORY_RULES: Record<string, MarketLens> = {
  farmacias: {
    market_sector: "Health & Wellness",
    market_subsector: "Pharma Retail",
    market_category: "Pharmacies",
  },
  hospitales: {
    market_sector: "Health & Wellness",
    market_subsector: "Healthcare Services",
    market_category: "Hospitals",
  },
  clinicas: {
    market_sector: "Health & Wellness",
    market_subsector: "Healthcare Services",
    market_category: "Clinics",
  },
  laboratorios: {
    market_sector: "Health & Wellness",
    market_subsector: "Healthcare Services",
    market_category: "Labs",
  },
  medicamentos: {
    market_sector: "Health & Wellness",
    market_subsector: "Consumer Health",
    market_category: "OTC Brands",
  },
  "tiendas de descuento": {
    market_sector: "Retail & Commerce",
    market_subsector: "Mass Retail",
    market_category: "Discount Retail",
  },
  "tiendas de ropa": {
    market_sector: "Retail & Commerce",
    market_subsector: "Fashion",
    market_category: "Apparel Stores",
  },
  "quimicos y soluciones para la construccion": {
    market_sector: "Industry & Manufacturing",
    market_subsector: "Home Improvement",
    market_category: "Materials & Supplies",
  },
  "quimicos y soluciones para la construcciã³n": {
    market_sector: "Industry & Manufacturing",
    market_subsector: "Home Improvement",
    market_category: "Materials & Supplies",
  },
};

const SUBSECTOR_RULES: Record<string, MarketLens> = {
  "e-commerce": {
    market_sector: "Retail & Commerce",
    market_subsector: "Digital Commerce",
    market_category: "E-commerce",
  },
  autoservicios: {
    market_sector: "Retail & Commerce",
    market_subsector: "Mass Retail",
    market_category: "Mass Retail",
  },
  salud: {
    market_sector: "Health & Wellness",
    market_subsector: "Consumer Health",
    market_category: "OTC Brands",
  },
  "materiales para la construccion": {
    market_sector: "Industry & Manufacturing",
    market_subsector: "Home Improvement",
    market_category: "Materials & Supplies",
  },
  "materiales para la construcciã³n": {
    market_sector: "Industry & Manufacturing",
    market_subsector: "Home Improvement",
    market_category: "Materials & Supplies",
  },
  "comercio especializado": {
    market_sector: "Retail & Commerce",
    market_subsector: "Specialty Retail",
    market_category: "Specialty Stores",
  },
};

const SECTOR_RULES: Record<string, MarketLens> = {
  comercio: {
    market_sector: "Retail & Commerce",
    market_subsector: "Retail",
    market_category: "General Retail",
  },
  servicios: {
    market_sector: "Services",
    market_subsector: "Consumer Services",
    market_category: "General Services",
  },
  industria: {
    market_sector: "Industry & Manufacturing",
    market_subsector: "Manufacturing",
    market_category: "General Industry",
  },
  "servicios financieros": {
    market_sector: "Financial Services",
    market_subsector: "Banking & Insurance",
    market_category: "Financial Products",
  },
};

export function deriveMarketLens(input: DeriveInput): MarketLens {
  const standardSector = valueOr("Unassigned", input.sector);
  const standardSubsector = valueOr(standardSector, input.subsector);
  const standardCategory = valueOr(standardSubsector, input.category);

  const categoryRule = CATEGORY_RULES[normalize(standardCategory)];
  if (categoryRule) return categoryRule;

  const subsectorRule = SUBSECTOR_RULES[normalize(standardSubsector)];
  if (subsectorRule) return subsectorRule;

  const sectorRule = SECTOR_RULES[normalize(standardSector)];
  if (sectorRule) return sectorRule;

  return {
    market_sector: standardSector,
    market_subsector: standardSubsector,
    market_category: standardCategory,
  };
}

export function resolveMarketLens(input: DeriveInput): MarketLens {
  const derived = deriveMarketLens(input);

  const explicitMarket =
    typeof input.market_sector === "string" &&
    input.market_sector.trim() &&
    typeof input.market_subsector === "string" &&
    input.market_subsector.trim() &&
    typeof input.market_category === "string" &&
    input.market_category.trim();

  if (explicitMarket) {
    const market = {
      market_sector: input.market_sector!.trim(),
      market_subsector: input.market_subsector!.trim(),
      market_category: input.market_category!.trim(),
    };
    const looksLikeStandard =
      normalize(market.market_sector) === normalize(input.sector) &&
      normalize(market.market_subsector) === normalize(input.subsector) &&
      normalize(market.market_category) === normalize(input.category);
    if (!looksLikeStandard) {
      // Auto-repair stale legacy "generic" mappings persisted in Supabase snapshots.
      if (isGenericMarketFallback(market) && !isGenericMarketFallback(derived)) {
        return derived;
      }
      return market;
    }
  }

  return derived;
}
