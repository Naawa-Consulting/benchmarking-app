import type { StudyOption } from "../../../components/layout/ScopeProvider";

type TrackingStudyPickerProps = {
  studies: StudyOption[];
  studiesForB: StudyOption[];
  studyA: string | null;
  studyB: string | null;
  onStudyAChange: (studyId: string | null) => void;
  onStudyBChange: (studyId: string | null) => void;
  orderedLabels: { pre: string; post: string } | null;
  orderingWarning: string | null;
  scopeHint: string | null;
};

export default function TrackingStudyPicker({
  studies,
  studiesForB,
  studyA,
  studyB,
  onStudyAChange,
  onStudyBChange,
  orderedLabels,
  orderingWarning,
  scopeHint,
}: TrackingStudyPickerProps) {
  const duplicateSelection = Boolean(studyA && studyB && studyA === studyB);

  return (
    <section className="main-surface rounded-3xl p-5">
      <div className="flex flex-col gap-4">
        <div>
          <h1 className="text-xl font-semibold text-ink">Trends</h1>
          <p className="text-sm text-slate">Compare exactly two studies and quantify brand lift across Journey metrics.</p>
        </div>
        <div className="grid gap-3 md:grid-cols-2">
          <label className="space-y-1">
            <span className="text-xs font-semibold uppercase tracking-[0.08em] text-slate">Base A</span>
            <select
              className="w-full rounded-full border border-ink/10 bg-white px-3 py-2 text-sm text-ink shadow-sm"
              value={studyA ?? ""}
              onChange={(event) => onStudyAChange(event.target.value || null)}
            >
              <option value="">Select study</option>
              {studies.map((study) => (
                <option key={study.study_id} value={study.study_id}>
                  {study.study_name || study.study_id}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-1">
            <span className="text-xs font-semibold uppercase tracking-[0.08em] text-slate">Base B</span>
            <select
              className="w-full rounded-full border border-ink/10 bg-white px-3 py-2 text-sm text-ink shadow-sm"
              value={studyB ?? ""}
              onChange={(event) => onStudyBChange(event.target.value || null)}
            >
              <option value="">Select study</option>
              {studiesForB.map((study) => (
                <option key={study.study_id} value={study.study_id}>
                  {study.study_name || study.study_id}
                </option>
              ))}
            </select>
          </label>
        </div>
        {studyA && <p className="text-xs text-slate">Base B se limita a la categoria de Base A.</p>}
        {scopeHint && <p className="text-xs text-slate">{scopeHint}</p>}
        {duplicateSelection && <p className="text-xs text-rose-600">Base A and Base B must be different studies.</p>}
        {orderedLabels && !duplicateSelection && (
          <div className="flex flex-wrap items-center gap-2 text-xs text-slate">
            <span className="rounded-full border border-ink/10 bg-white px-2.5 py-1">
              Pre: <span className="font-medium text-ink">{orderedLabels.pre}</span>
            </span>
            <span className="rounded-full border border-ink/10 bg-white px-2.5 py-1">
              Post: <span className="font-medium text-ink">{orderedLabels.post}</span>
            </span>
          </div>
        )}
        {orderingWarning && <p className="text-xs text-amber-700">{orderingWarning}</p>}
      </div>
    </section>
  );
}
