"""Backtest harness for quant signals.

Self-contained: only imports pandas + numpy.
Copied into workspace as lib/backtest.py so Claude can import it directly.
"""
from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import yaml


@dataclass
class SignalConfig:
    """Signal configuration loaded from YAML."""
    signal_id: str
    bar_size: str = "1d"
    lookback: int = 50
    hold_periods: list[int] = field(default_factory=lambda: [1, 5, 10, 20])
    ticker: str = ""
    description: str = ""


@dataclass
class HoldPeriodResult:
    """Results for a single hold period."""
    hold_period: int
    signal_count: int
    hit_rate: float
    mean_return: float
    median_return: float
    sharpe: float
    max_drawdown: float
    t_stat: float
    # Out-of-sample
    oos_hit_rate: float | None = None
    oos_mean_return: float | None = None
    oos_sharpe: float | None = None
    oos_signal_count: int | None = None


@dataclass
class BacktestResult:
    """Full backtest result."""
    signal_id: str
    ticker: str
    bar_size: str
    total_bars: int
    signal_count: int
    signal_rate: float
    hold_period_results: list[HoldPeriodResult]
    beta: float | None = None
    alpha: float | None = None
    warnings: list[str] = field(default_factory=list)


def _annualization_factor(bar_size: str) -> float:
    """Return sqrt(periods_per_year) for Sharpe annualization."""
    factors = {
        "1d": np.sqrt(252),
        "5m": np.sqrt(252 * 78),
        "1m": np.sqrt(252 * 390),
    }
    return factors.get(bar_size, np.sqrt(252))


def load_signal(signal_dir: Path, signal_id: str) -> tuple[Callable, SignalConfig]:
    """Load a signal function and its config from the signals/ directory."""
    py_path = signal_dir / f"{signal_id}.py"
    yaml_path = signal_dir / f"{signal_id}.yaml"

    if not py_path.exists():
        raise FileNotFoundError(f"Signal file not found: {py_path}")
    if not yaml_path.exists():
        raise FileNotFoundError(f"Signal config not found: {yaml_path}")

    # Load config
    with open(yaml_path) as f:
        cfg_data = yaml.safe_load(f)
    config = SignalConfig(signal_id=signal_id, **{k: v for k, v in cfg_data.items() if k != "signal_id"})

    # Load signal function
    spec = importlib.util.spec_from_file_location(f"signal_{signal_id}", py_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    if not hasattr(mod, "signal"):
        raise AttributeError(f"Signal module {py_path} must define a 'signal(df)' function")

    return mod.signal, config


def _compute_forward_returns(df: pd.DataFrame, periods: int) -> pd.Series:
    """Compute forward returns over N periods."""
    return df["close"].pct_change(periods).shift(-periods)


def _compute_max_drawdown(returns: pd.Series) -> float:
    """Max drawdown of a return series (as a negative number)."""
    if returns.empty:
        return 0.0
    cumulative = (1 + returns).cumprod()
    peak = cumulative.expanding().max()
    drawdown = (cumulative - peak) / peak
    return float(drawdown.min())


def _compute_beta_alpha(
    signal_returns: pd.Series,
    benchmark_returns: pd.Series,
) -> tuple[float, float]:
    """Regress signal returns against benchmark for beta and alpha."""
    aligned = pd.DataFrame({"signal": signal_returns, "bench": benchmark_returns}).dropna()
    if len(aligned) < 10:
        return 0.0, 0.0
    x = aligned["bench"].values
    y = aligned["signal"].values
    x_mean = x.mean()
    y_mean = y.mean()
    beta = np.sum((x - x_mean) * (y - y_mean)) / (np.sum((x - x_mean) ** 2) + 1e-10)
    alpha = y_mean - beta * x_mean
    return float(beta), float(alpha)


def run_backtest(
    signal_fn: Callable,
    config: SignalConfig,
    data_dir: Path,
    ticker: str | None = None,
) -> BacktestResult:
    """Run backtest for a signal function against OHLCV data."""
    ticker = ticker or config.ticker
    if not ticker:
        raise ValueError("Ticker must be specified in config or as argument")

    bar_size = config.bar_size
    ann_factor = _annualization_factor(bar_size)

    # Load data
    data_path = data_dir / f"{ticker}_{bar_size}.csv"
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")
    df = pd.read_csv(data_path, index_col=0, parse_dates=True)
    total_bars = len(df)

    # Load benchmark
    spy_path = data_dir / f"SPY_{bar_size}.csv"
    spy_df = None
    if spy_path.exists():
        spy_df = pd.read_csv(spy_path, index_col=0, parse_dates=True)

    # Run signal
    signals = signal_fn(df)
    if not isinstance(signals, pd.Series):
        signals = pd.Series(signals, index=df.index)
    signals = signals.astype(bool)

    # Trim lookback
    signals.iloc[:config.lookback] = False

    signal_count = int(signals.sum())
    signal_rate = signal_count / max(total_bars - config.lookback, 1)

    warnings: list[str] = []
    if signal_count < 20:
        warnings.append(f"Low signal count ({signal_count}). Results may be unreliable.")

    # Compute results for each hold period
    hold_results: list[HoldPeriodResult] = []
    for hp in config.hold_periods:
        fwd_returns = _compute_forward_returns(df, hp)
        # Mask: valid signals with valid forward returns
        valid_mask = signals & fwd_returns.notna()
        signal_rets = fwd_returns[valid_mask]
        n = len(signal_rets)

        if n == 0:
            hold_results.append(HoldPeriodResult(
                hold_period=hp, signal_count=0, hit_rate=0.0,
                mean_return=0.0, median_return=0.0, sharpe=0.0,
                max_drawdown=0.0, t_stat=0.0,
            ))
            continue

        mean_ret = float(signal_rets.mean())
        std_ret = float(signal_rets.std()) if n > 1 else 1.0
        hit_rate = float((signal_rets > 0).mean())
        sharpe = (mean_ret / (std_ret + 1e-10)) * ann_factor / np.sqrt(hp) if std_ret > 0 else 0.0
        max_dd = _compute_max_drawdown(signal_rets)
        t_stat = mean_ret / (std_ret / np.sqrt(n) + 1e-10) if n > 1 else 0.0

        # OOS split if enough signals
        oos_hit = oos_mean = oos_sharpe = None
        oos_n = None
        if n >= 30:
            split = int(n * 0.7)
            oos_rets = signal_rets.iloc[split:]
            oos_n = len(oos_rets)
            if oos_n > 0:
                oos_hit = float((oos_rets > 0).mean())
                oos_mean = float(oos_rets.mean())
                oos_std = float(oos_rets.std()) if oos_n > 1 else 1.0
                oos_sharpe = (oos_mean / (oos_std + 1e-10)) * ann_factor / np.sqrt(hp)

        hold_results.append(HoldPeriodResult(
            hold_period=hp, signal_count=n, hit_rate=hit_rate,
            mean_return=mean_ret, median_return=float(signal_rets.median()),
            sharpe=float(sharpe), max_drawdown=float(max_dd), t_stat=float(t_stat),
            oos_hit_rate=oos_hit, oos_mean_return=oos_mean,
            oos_sharpe=float(oos_sharpe) if oos_sharpe is not None else None,
            oos_signal_count=oos_n,
        ))

    # Beta/alpha against SPY
    beta = alpha = None
    if spy_df is not None and signal_count > 0:
        # Use 1-period forward returns for beta calc
        fwd_1 = _compute_forward_returns(df, 1)
        spy_fwd_1 = _compute_forward_returns(spy_df, 1)
        valid = signals & fwd_1.notna()
        sig_rets_1 = fwd_1[valid]
        # Align by index
        common_idx = sig_rets_1.index.intersection(spy_fwd_1.index)
        if len(common_idx) >= 10:
            beta, alpha = _compute_beta_alpha(
                sig_rets_1.loc[common_idx],
                spy_fwd_1.loc[common_idx],
            )

    return BacktestResult(
        signal_id=config.signal_id,
        ticker=ticker,
        bar_size=bar_size,
        total_bars=total_bars,
        signal_count=signal_count,
        signal_rate=signal_rate,
        hold_period_results=hold_results,
        beta=beta,
        alpha=alpha,
        warnings=warnings,
    )


def format_report(result: BacktestResult) -> str:
    """Format backtest result as markdown."""
    lines = [
        f"# Backtest Report: {result.signal_id}",
        f"",
        f"**Ticker:** {result.ticker} | **Bar size:** {result.bar_size}",
        f"**Total bars:** {result.total_bars:,} | **Signal fires:** {result.signal_count} ({result.signal_rate:.1%})",
    ]

    if result.beta is not None:
        lines.append(f"**Beta:** {result.beta:.3f} | **Alpha (per bar):** {result.alpha:.5f}")

    if result.warnings:
        lines.append("")
        for w in result.warnings:
            lines.append(f"> WARNING: {w}")

    lines.append("")
    lines.append("## Results by Hold Period")
    lines.append("")
    lines.append("| Hold | N | Hit Rate | Mean Ret | Median Ret | Sharpe | MaxDD | t-stat |")
    lines.append("|------|---|----------|----------|------------|--------|-------|--------|")
    for hr in result.hold_period_results:
        lines.append(
            f"| {hr.hold_period}d | {hr.signal_count} | {hr.hit_rate:.1%} | "
            f"{hr.mean_return:.3%} | {hr.median_return:.3%} | {hr.sharpe:.2f} | "
            f"{hr.max_drawdown:.1%} | {hr.t_stat:.2f} |"
        )

    # OOS section
    has_oos = any(hr.oos_hit_rate is not None for hr in result.hold_period_results)
    if has_oos:
        lines.append("")
        lines.append("## Out-of-Sample Validation (30% holdout)")
        lines.append("")
        lines.append("| Hold | N | Hit Rate | Mean Ret | Sharpe |")
        lines.append("|------|---|----------|----------|--------|")
        for hr in result.hold_period_results:
            if hr.oos_hit_rate is not None:
                lines.append(
                    f"| {hr.hold_period}d | {hr.oos_signal_count} | {hr.oos_hit_rate:.1%} | "
                    f"{hr.oos_mean_return:.3%} | {hr.oos_sharpe:.2f} |"
                )

    lines.append("")
    return "\n".join(lines)
