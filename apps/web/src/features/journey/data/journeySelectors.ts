import { type JourneyBrandAggregate, type JourneyModel, type JourneyStage } from "./journeySchema";

export function selectJourneyBrand(model: JourneyModel, brandName: string): JourneyBrandAggregate | null {
  return model.brandStageAggregates.find((item) => item.brandName === brandName) ?? null;
}

export function selectStageCoverage(model: JourneyModel, stage: JourneyStage) {
  return model.metadata.coverage.byStage.find((item) => item.stage === stage) ?? null;
}

export function selectLinkCoverage(model: JourneyModel, fromStage: JourneyStage, toStage: JourneyStage) {
  return (
    model.metadata.coverage.byLink.find((item) => item.fromStage === fromStage && item.toStage === toStage) ?? null
  );
}

