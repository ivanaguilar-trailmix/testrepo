from __future__ import annotations

import calendar
import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

PLATFORMS = ("ios", "android")
SCENARIOS_DIR = Path(__file__).parent.parent / "scenarios"
RESULTS_DIR   = Path(__file__).parent.parent / "results"


# ---------------------------------------------------------------------------
# Input / output data structures
# ---------------------------------------------------------------------------

@dataclass
class PlatformInputs:
    platform: str
    retention_curve: np.ndarray    # shape (365,), index 0 = D1 retention rate
    conversion_curve: np.ndarray   # shape (365,), index 0 = D1 cumulative payer rate
    monthly_cpi: dict              # {"2026-05": 2.50, ...}
    monthly_iap_arpdau: dict       # {"2026-05": 0.05, ...}
    monthly_ad_arpdau: dict        # {"2026-05": 0.02, ...}
    monthly_ua_spend: dict         # {"2026-05": 100_000, ...}
    anchor_dau: Optional[float] = None  # last observed DAU (organic base anchor)
    avg_base_age: int = 60         # assumed avg cohort age of existing users at anchor date


@dataclass
class PlatformResults:
    platform: str
    dates: pd.DatetimeIndex
    dau: np.ndarray
    organic_dau: np.ndarray       # contribution from pre-forecast user base
    new_cohort_dau: np.ndarray    # contribution from new installs during forecast
    payer_dau: np.ndarray
    iap_revenue: np.ndarray
    ad_revenue: np.ndarray
    new_installs: np.ndarray

    @property
    def total_revenue(self) -> np.ndarray:
        return self.iap_revenue + self.ad_revenue

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame({
            "date":           self.dates,
            "platform":       self.platform,
            "dau":            self.dau,
            "organic_dau":    self.organic_dau,
            "new_cohort_dau": self.new_cohort_dau,
            "payer_dau":      self.payer_dau,
            "iap_revenue":    self.iap_revenue,
            "ad_revenue":     self.ad_revenue,
            "total_revenue":  self.total_revenue,
            "new_installs":   self.new_installs,
        })


@dataclass
class SimulationResults:
    ios: PlatformResults
    android: PlatformResults
    scenario_name: str = "unnamed"

    @property
    def combined(self) -> PlatformResults:
        return PlatformResults(
            platform="combined",
            dates=self.ios.dates,
            dau=self.ios.dau + self.android.dau,
            organic_dau=self.ios.organic_dau + self.android.organic_dau,
            new_cohort_dau=self.ios.new_cohort_dau + self.android.new_cohort_dau,
            payer_dau=self.ios.payer_dau + self.android.payer_dau,
            iap_revenue=self.ios.iap_revenue + self.android.iap_revenue,
            ad_revenue=self.ios.ad_revenue + self.android.ad_revenue,
            new_installs=self.ios.new_installs + self.android.new_installs,
        )

    def to_dataframe(self, include_combined: bool = True) -> pd.DataFrame:
        frames = [self.ios.to_dataframe(), self.android.to_dataframe()]
        if include_combined:
            frames.append(self.combined.to_dataframe())
        df = pd.concat(frames, ignore_index=True)
        df["scenario"] = self.scenario_name
        return df


# ---------------------------------------------------------------------------
# Core computation helpers
# ---------------------------------------------------------------------------

def _months_to_days(start: date, n_months: int) -> int:
    """Days from start to the first day of the month that is n_months later."""
    m    = start.month - 1 + n_months
    year = start.year + m // 12
    month = m % 12 + 1
    return (date(year, month, 1) - start).days


def _monthly_to_daily(monthly_spend: dict, start_date: date, n_days: int) -> np.ndarray:
    """Spread monthly UA budgets evenly across calendar days."""
    daily = np.zeros(n_days)
    for i in range(n_days):
        d = start_date + timedelta(days=i)
        key = d.strftime("%Y-%m")
        if key in monthly_spend:
            days_in_month = calendar.monthrange(d.year, d.month)[1]
            daily[i] = monthly_spend[key] / days_in_month
    return daily


def _monthly_lookup(monthly_dict: dict, start_date: date, n_days: int) -> np.ndarray:
    """Map a monthly dict to a daily array by repeating each month's value across its days."""
    daily = np.zeros(n_days)
    for i in range(n_days):
        key = (start_date + timedelta(days=i)).strftime("%Y-%m")
        if key in monthly_dict:
            daily[i] = monthly_dict[key]
    return daily


def _cohort_dau_and_payers(
    daily_installs: np.ndarray,
    retention: np.ndarray,
    conversion: np.ndarray,
    n_days: int,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Compute daily DAU and payer DAU contribution from new cohorts using convolution.

    DAU[t]      = sum over age a: installs[t-a] * retention[a]
    payer_dau[t] = sum over age a: installs[t-a] * conversion[a]

    retention[0]  = survival on install day (D0) = 1.0
    retention[1]  = survival on D1
    conversion[0] = payer fraction on D0 (typically 0 or very small)
    """
    # D0 prepended: all installs are active on install day
    ret = np.concatenate([[1.0], retention])[:n_days]
    conv = np.concatenate([[0.0], conversion])[:n_days]

    dau = np.convolve(daily_installs, ret)[:n_days]
    payer_dau = np.convolve(daily_installs, ret * conv)[:n_days]
    return dau, payer_dau


def _organic_base_decay(
    anchor_dau: float,
    retention: np.ndarray,
    avg_base_age: int,
    n_days: int,
) -> np.ndarray:
    """
    Project the organic user base (existing players at forecast start) forward in time.

    Models the base as a cohort at average age `avg_base_age` and applies the
    same retention curve from that point onwards.
    """
    organic = np.zeros(n_days)
    organic[0] = anchor_dau

    for t in range(1, n_days):
        curr = min(avg_base_age + t - 1, len(retention) - 1)
        prev = min(avg_base_age + t - 2, len(retention) - 1)
        if prev >= 0 and retention[prev] > 0:
            daily_survival = retention[curr] / retention[prev]
        else:
            daily_survival = 0.0
        organic[t] = organic[t - 1] * daily_survival

    return organic


# ---------------------------------------------------------------------------
# Per-platform runner
# ---------------------------------------------------------------------------

def _run_platform(inputs: PlatformInputs, start_date: date, n_days: int) -> PlatformResults:
    daily_spend   = _monthly_to_daily(inputs.monthly_ua_spend,   start_date, n_days)
    daily_cpi     = _monthly_lookup(inputs.monthly_cpi,          start_date, n_days)
    daily_iap     = _monthly_lookup(inputs.monthly_iap_arpdau,   start_date, n_days)
    daily_ad      = _monthly_lookup(inputs.monthly_ad_arpdau,    start_date, n_days)

    daily_installs = np.where(daily_cpi > 0, daily_spend / daily_cpi, 0.0)

    new_cohort_dau, payer_dau = _cohort_dau_and_payers(
        daily_installs, inputs.retention_curve, inputs.conversion_curve, n_days
    )

    if inputs.anchor_dau is not None and inputs.anchor_dau > 0:
        organic_dau = _organic_base_decay(
            inputs.anchor_dau, inputs.retention_curve, inputs.avg_base_age, n_days
        )
    else:
        organic_dau = np.zeros(n_days)

    dau         = new_cohort_dau + organic_dau
    iap_revenue = dau * daily_iap
    ad_revenue  = dau * daily_ad
    dates       = pd.date_range(start=start_date, periods=n_days, freq="D")

    return PlatformResults(
        platform=inputs.platform,
        dates=dates,
        dau=dau,
        organic_dau=organic_dau,
        new_cohort_dau=new_cohort_dau,
        payer_dau=payer_dau,
        iap_revenue=iap_revenue,
        ad_revenue=ad_revenue,
        new_installs=daily_installs,
    )


# ---------------------------------------------------------------------------
# Main engine
# ---------------------------------------------------------------------------

class SimulationEngine:
    N_DAYS = 365

    def run(
        self,
        ios_inputs: PlatformInputs,
        android_inputs: PlatformInputs,
        forecast_start: date,
        scenario_name: str = "unnamed",
        n_months: int = 12,
    ) -> SimulationResults:
        n_days = _months_to_days(forecast_start, n_months)
        ios_results = _run_platform(ios_inputs, forecast_start, n_days)
        ios_results.platform = "ios"
        android_results = _run_platform(android_inputs, forecast_start, n_days)
        android_results.platform = "android"
        return SimulationResults(ios=ios_results, android=android_results, scenario_name=scenario_name)


# ---------------------------------------------------------------------------
# Scenario persistence
# ---------------------------------------------------------------------------

def _inputs_to_dict(inputs: PlatformInputs) -> dict:
    return {
        "platform":           inputs.platform,
        "retention_curve":    inputs.retention_curve.tolist(),
        "conversion_curve":   inputs.conversion_curve.tolist(),
        "monthly_cpi":        inputs.monthly_cpi,
        "monthly_iap_arpdau": inputs.monthly_iap_arpdau,
        "monthly_ad_arpdau":  inputs.monthly_ad_arpdau,
        "monthly_ua_spend":   inputs.monthly_ua_spend,
        "anchor_dau":         inputs.anchor_dau,
        "avg_base_age":       inputs.avg_base_age,
    }


def _inputs_from_dict(d: dict) -> PlatformInputs:
    return PlatformInputs(
        platform=d["platform"],
        retention_curve=np.array(d["retention_curve"]),
        conversion_curve=np.array(d["conversion_curve"]),
        monthly_cpi=d["monthly_cpi"],
        monthly_iap_arpdau=d["monthly_iap_arpdau"],
        monthly_ad_arpdau=d["monthly_ad_arpdau"],
        monthly_ua_spend=d["monthly_ua_spend"],
        anchor_dau=d.get("anchor_dau"),
        avg_base_age=d.get("avg_base_age", 60),
    )


def save_scenario(
    name: str,
    forecast_start: date,
    ios_inputs: PlatformInputs,
    android_inputs: PlatformInputs,
    n_months: int = 12,
    curve_anchors: dict = None,
    actuals_range: dict = None,
    monthly_team_cost: dict = None,
) -> Path:
    SCENARIOS_DIR.mkdir(exist_ok=True)
    safe_name = name.replace(" ", "_").lower()
    path = SCENARIOS_DIR / f"{safe_name}.json"
    payload = {
        "name":               name,
        "forecast_start":     str(forecast_start),
        "n_months":           n_months,
        "curve_anchors":      curve_anchors,
        "actuals_range":      actuals_range,
        "monthly_team_cost":  monthly_team_cost,
        "ios":                _inputs_to_dict(ios_inputs),
        "android":            _inputs_to_dict(android_inputs),
    }
    path.write_text(json.dumps(payload, indent=2))
    return path


def load_scenario(name: str) -> tuple[str, date, int, PlatformInputs, PlatformInputs, dict, dict, dict]:
    safe_name = name.replace(" ", "_").lower()
    path = SCENARIOS_DIR / f"{safe_name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Scenario '{name}' not found at {path}")
    payload = json.loads(path.read_text())
    return (
        payload["name"],
        date.fromisoformat(payload["forecast_start"]),
        payload.get("n_months", 12),
        _inputs_from_dict(payload["ios"]),
        _inputs_from_dict(payload["android"]),
        payload.get("curve_anchors"),        # None for old scenarios
        payload.get("actuals_range"),        # None for old scenarios
        payload.get("monthly_team_cost"),    # None for old scenarios
    )


def list_scenarios() -> list[str]:
    SCENARIOS_DIR.mkdir(exist_ok=True)
    return [p.stem.replace("_", " ") for p in sorted(SCENARIOS_DIR.glob("*.json"))]


# ---------------------------------------------------------------------------
# Result persistence (simulation outputs)
# ---------------------------------------------------------------------------

def save_result(scenario_name: str, result: SimulationResults) -> Path:
    RESULTS_DIR.mkdir(exist_ok=True)
    safe_name = scenario_name.replace(" ", "_").lower()
    path = RESULTS_DIR / f"{safe_name}.json"
    df = result.to_dataframe(include_combined=True)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    path.write_text(json.dumps({"scenario": scenario_name, "data": df.to_dict("records")}))
    return path


def load_result(scenario_name: str) -> pd.DataFrame:
    safe_name = scenario_name.replace(" ", "_").lower()
    path = RESULTS_DIR / f"{safe_name}.json"
    if not path.exists():
        raise FileNotFoundError(f"No saved result for '{scenario_name}' — run the simulation first.")
    payload = json.loads(path.read_text())
    df = pd.DataFrame(payload["data"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def list_results() -> list[str]:
    RESULTS_DIR.mkdir(exist_ok=True)
    return [p.stem.replace("_", " ") for p in sorted(RESULTS_DIR.glob("*.json"))]
