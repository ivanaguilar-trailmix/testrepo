# Working notes — monthly_performance (dau.ipynb / payers.ipynb)

Deferred ideas and follow-ups, kept out of the notebooks themselves.

## Open

- **Weekly charts: actual unique count vs average of daily uniques.** `chart_absolute_and_pct_change(..., freq='W')` currently resamples by averaging the already-computed *daily* unique-user counts. A true weekly unique count (anyone active at least once that week) would be a different, larger number. Add a variant for this later — not needed now.

- **Repeat-purchase rate: chart style needs review.** The dual-axis `chart_absolute_and_pct_change` charts (same style as Objective 1) don't make it easy to tell at a glance whether the repeat-purchase rate is trending better or worse. Revisit whether this is the right chart type for this metric before treating Objective 2's presentation as final.

- **Repeat-purchase rate: add a "within Dx" variant.** Current `repeat_purchase_df` (`flag_dN`) only flags a purchase landing exactly on `dt + N`. Add a second version that instead flags *any* purchase within the range up to `dt + N` (not just exactly on that day) — same distinction as "returned on day N" vs "returned within N days" in typical retention definitions.
