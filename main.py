import json
import logging
import math
import os
import re
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Counter as CounterType, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import WorksheetNotFound
import requests
from fastapi import FastAPI
from google.oauth2.service_account import Credentials


APP_VERSION = "1.3.0-concentration-gate"


# -----------------------------
# Logging
# -----------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("alpaca-score-buyer")


# -----------------------------
# Config
# -----------------------------


def getenv_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def getenv_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number; got {raw!r}") from exc


def getenv_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer; got {raw!r}") from exc


@dataclass(frozen=True)
class Config:
    # Google Sheets
    google_service_account_json: str
    google_sheet_id: str
    google_worksheet_name: str
    screener_range: str
    symbol_col_index: int
    score_col_index: int
    close_col_index: int
    sma_200_col_index: int
    sma_50_col_index: int
    pos_52w_col_index: int
    dollar_vol_m_col_index: int
    start_row: int
    entry_log_worksheet_name: str

    # Alpaca
    alpaca_api_key: str
    alpaca_secret_key: str
    alpaca_paper: bool
    alpaca_data_feed: str
    trading_base_url: str
    data_base_url: str

    # Buyer behavior
    buy_score: float
    order_fraction: float
    min_notional: float
    max_orders_per_cycle: int
    cycle_sleep_seconds: float
    regt_fallback_fraction: float
    extended_hours: bool
    blocked_product_name_pattern: str
    skip_when_asset_name_unavailable: bool
    sma200_sizing_enabled: bool
    sma200_sizing_full_size_distance: float
    sma200_sizing_max_reduction_distance: float
    sma200_sizing_min_multiplier: float
    sma50_sizing_enabled: bool
    sma50_sizing_full_size_distance: float
    sma50_sizing_max_reduction_distance: float
    sma50_sizing_min_multiplier: float

    # Portfolio risk gate / ranking / re-entry behavior
    risk_gate_enabled: bool
    max_total_positions: int
    yellow_exposure_pct: float
    red_exposure_pct: float
    yellow_min_cash_pct: float
    red_min_cash_pct: float
    yellow_margin_use_pct: float
    red_margin_use_pct: float
    yellow_red_position_pct: float
    red_red_position_pct: float
    yellow_drawdown_pct: float
    red_drawdown_pct: float
    equity_high_watermark: float
    yellow_max_orders_per_cycle: int
    yellow_order_fraction_multiplier: float
    rank_candidates_enabled: bool
    reentry_guard_enabled: bool
    reentry_lookback_days: int
    reentry_min_discount_pct: float
    reentry_require_above_sma50: bool
    reentry_allow_in_yellow: bool
    reentry_allow_without_sell_price: bool

    # Sector / correlated-risk concentration protection
    concentration_gate_enabled: bool
    risk_map_worksheet_name: str
    unknown_classification_policy: str
    market_timezone: str
    max_new_per_sector_per_day: int
    max_new_per_risk_group_per_day: int
    max_open_positions_per_sector: int
    max_open_positions_per_risk_group: int
    max_sector_exposure_pct: float
    max_risk_group_exposure_pct: float
    max_daily_sector_notional_pct: float
    max_daily_risk_group_notional_pct: float
    stress_freeze_enabled: bool
    stress_min_open_positions: int
    stress_red_position_pct: float
    stress_aggregate_return_pct: float

    # Chasing behavior
    bid_to_market_steps: Tuple[float, ...]
    step_timeout_seconds: float
    total_chase_timeout_seconds: float
    order_poll_interval_seconds: float
    treat_partial_fill_as_success: bool
    order_failure_cooldown_seconds: float
    error_body_max_chars: int

    # HTTP/retry behavior
    request_timeout_seconds: float
    request_retries: int
    request_sleep_seconds: float
    rate_limit_sleep_seconds: float
    error_sleep_seconds: float



@dataclass(frozen=True)
class OrderAttemptResult:
    filled_or_partial: bool
    submitted: bool
    failure_status: Optional[int] = None
    failure_message: str = ""


@dataclass(frozen=True)
class BuyCandidate:
    row_num: int
    symbol: str
    close: float
    sma_200: float
    sma_50: float
    pos_52w: float
    dollar_vol_m: float
    score: float


@dataclass(frozen=True)
class PositionSnapshot:
    symbol: str
    qty: float
    market_value: float
    unrealized_pl: float
    unrealized_plpc: float


@dataclass(frozen=True)
class RecentSellFill:
    symbol: str
    price: float
    qty: float
    transaction_time: str


@dataclass(frozen=True)
class RiskSnapshot:
    mode: str
    reasons: Tuple[str, ...]
    equity: float
    cash: float
    cash_pct: float
    exposure_pct: float
    margin_use_pct: float
    drawdown_pct: float
    high_watermark: float
    position_count: int
    open_buy_order_count: int
    red_position_count: int
    green_position_count: int
    red_position_pct: float
    available_position_slots: int
    max_new_orders_this_cycle: int
    order_fraction_multiplier: float


@dataclass(frozen=True)
class SymbolClassification:
    symbol: str
    sector: str
    risk_group: str


@dataclass
class BucketStats:
    label: str
    position_count: int = 0
    market_value: float = 0.0
    unrealized_pl: float = 0.0
    cost_basis: float = 0.0
    red_position_count: int = 0

    @property
    def red_position_pct(self) -> float:
        return positive_ratio(self.red_position_count, self.position_count)

    @property
    def aggregate_return_pct(self) -> float:
        return positive_ratio(self.unrealized_pl, self.cost_basis)


@dataclass
class ConcentrationState:
    sector_stats: Dict[str, BucketStats]
    group_stats: Dict[str, BucketStats]
    daily_sector_entries: CounterType[str]
    daily_group_entries: CounterType[str]
    daily_sector_notional: CounterType[str]
    daily_group_notional: CounterType[str]
    unknown_position_symbols: Set[str]


@dataclass(frozen=True)
class ConcentrationCheck:
    allowed: bool
    reason: str
    sector: str
    risk_group: str
    sector_position_count_before: int
    group_position_count_before: int
    sector_exposure_pct_before: float
    group_exposure_pct_before: float
    sector_entries_today: int
    group_entries_today: int
    sector_daily_notional_pct_before: float
    group_daily_notional_pct_before: float
    sector_stressed: bool
    group_stressed: bool


class HttpStatusError(RuntimeError):
    def __init__(self, method: str, url: str, status_code: int, body: str) -> None:
        self.method = method
        self.url = url
        self.status_code = status_code
        self.body = body
        body_suffix = f" body={body}" if body else ""
        super().__init__(f"{method} {url} failed status={status_code}{body_suffix}")


def load_config() -> Config:
    google_service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    google_sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    google_worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Screener").strip()

    alpaca_api_key = (
        os.getenv("ALPACA_API_KEY")
        or os.getenv("ALPACA_API_KEY_ID")
        or os.getenv("APCA_API_KEY_ID")
        or ""
    ).strip()
    alpaca_secret_key = (
        os.getenv("ALPACA_SECRET_KEY")
        or os.getenv("ALPACA_API_SECRET")
        or os.getenv("ALPACA_API_SECRET_KEY")
        or os.getenv("APCA_API_SECRET_KEY")
        or ""
    ).strip()

    if not google_service_account_json:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is required")
    if not google_sheet_id:
        raise RuntimeError("GOOGLE_SHEET_ID is required")
    if not alpaca_api_key or not alpaca_secret_key:
        raise RuntimeError("ALPACA_API_KEY and ALPACA_SECRET_KEY are required")

    alpaca_paper = getenv_bool("ALPACA_PAPER", True)
    trading_base_url = (
        "https://paper-api.alpaca.markets" if alpaca_paper else "https://api.alpaca.markets"
    )

    steps_raw = os.getenv("BID_TO_MARKET_STEPS", "0.0,0.4,0.7,1.0")
    steps = tuple(float(x.strip()) for x in steps_raw.split(",") if x.strip())
    if not steps:
        steps = (0.0, 0.4, 0.7, 1.0)

    regt_fallback_fraction = getenv_float("REGT_FALLBACK_FRACTION", 0.95)
    if regt_fallback_fraction <= 0:
        raise RuntimeError("REGT_FALLBACK_FRACTION must be greater than 0")

    unknown_classification_policy = os.getenv("UNKNOWN_CLASSIFICATION_POLICY", "skip").strip().lower()
    if unknown_classification_policy not in {"skip", "bucket"}:
        raise RuntimeError("UNKNOWN_CLASSIFICATION_POLICY must be 'skip' or 'bucket'")

    return Config(
        google_service_account_json=google_service_account_json,
        google_sheet_id=google_sheet_id,
        google_worksheet_name=google_worksheet_name,
        screener_range=os.getenv("SCREENER_RANGE", "A:G").strip(),
        symbol_col_index=getenv_int("SYMBOL_COL_INDEX", 1),
        score_col_index=getenv_int("SCORE_COL_INDEX", 7),
        close_col_index=getenv_int("CLOSE_COL_INDEX", 2),
        sma_200_col_index=getenv_int("SMA_200_COL_INDEX", 3),
        sma_50_col_index=getenv_int("SMA_50_COL_INDEX", 4),
        pos_52w_col_index=getenv_int("POS_52W_COL_INDEX", 5),
        dollar_vol_m_col_index=getenv_int("DOLLAR_VOL_M_COL_INDEX", 6),
        start_row=getenv_int("START_ROW", 2),
        entry_log_worksheet_name=os.getenv("ENTRY_LOG_WORKSHEET_NAME", "Buyer Entry Log").strip(),
        alpaca_api_key=alpaca_api_key,
        alpaca_secret_key=alpaca_secret_key,
        alpaca_paper=alpaca_paper,
        alpaca_data_feed=os.getenv("ALPACA_DATA_FEED", "iex").strip().lower(),
        trading_base_url=os.getenv("ALPACA_TRADING_BASE_URL", trading_base_url).strip(),
        data_base_url=os.getenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets").strip(),
        buy_score=getenv_float("BUY_SCORE", 100.0),
        order_fraction=getenv_float("ORDER_FRACTION", 0.02),
        min_notional=getenv_float("MIN_NOTIONAL", 1.0),
        max_orders_per_cycle=getenv_int("MAX_ORDERS_PER_CYCLE", 0),
        cycle_sleep_seconds=getenv_float("CYCLE_SLEEP_SECONDS", 10.0),
        regt_fallback_fraction=regt_fallback_fraction,
        extended_hours=getenv_bool("EXTENDED_HOURS", False),
        blocked_product_name_pattern=os.getenv(
            "BLOCK_PRODUCT_NAME_PATTERN",
            r"\b(daily|inverse|2\s*x|3\s*x)\b",
        ).strip(),
        skip_when_asset_name_unavailable=getenv_bool("SKIP_WHEN_ASSET_NAME_UNAVAILABLE", True),
        sma200_sizing_enabled=getenv_bool("SMA200_SIZING_ENABLED", True),
        sma200_sizing_full_size_distance=getenv_float("SMA200_SIZING_FULL_SIZE_DISTANCE", 0.0),
        sma200_sizing_max_reduction_distance=getenv_float("SMA200_SIZING_MAX_REDUCTION_DISTANCE", 0.50),
        sma200_sizing_min_multiplier=getenv_float("SMA200_SIZING_MIN_MULTIPLIER", 0.25),
        sma50_sizing_enabled=getenv_bool("SMA50_SIZING_ENABLED", True),
        sma50_sizing_full_size_distance=getenv_float("SMA50_SIZING_FULL_SIZE_DISTANCE", 0.15),
        sma50_sizing_max_reduction_distance=getenv_float("SMA50_SIZING_MAX_REDUCTION_DISTANCE", 0.30),
        sma50_sizing_min_multiplier=getenv_float("SMA50_SIZING_MIN_MULTIPLIER", 0.50),
        risk_gate_enabled=getenv_bool("RISK_GATE_ENABLED", True),
        max_total_positions=getenv_int("MAX_TOTAL_POSITIONS", 60),
        yellow_exposure_pct=getenv_float("YELLOW_EXPOSURE_PCT", 0.75),
        red_exposure_pct=getenv_float("RED_EXPOSURE_PCT", 0.90),
        yellow_min_cash_pct=getenv_float("YELLOW_MIN_CASH_PCT", 0.07),
        red_min_cash_pct=getenv_float("RED_MIN_CASH_PCT", 0.02),
        yellow_margin_use_pct=getenv_float("YELLOW_MARGIN_USE_PCT", 0.75),
        red_margin_use_pct=getenv_float("RED_MARGIN_USE_PCT", 0.90),
        yellow_red_position_pct=getenv_float("YELLOW_RED_POSITION_PCT", 0.55),
        red_red_position_pct=getenv_float("RED_RED_POSITION_PCT", 0.70),
        yellow_drawdown_pct=getenv_float("YELLOW_DRAWDOWN_PCT", 0.05),
        red_drawdown_pct=getenv_float("RED_DRAWDOWN_PCT", 0.10),
        equity_high_watermark=getenv_float("EQUITY_HIGH_WATERMARK", 0.0),
        yellow_max_orders_per_cycle=getenv_int("YELLOW_MAX_ORDERS_PER_CYCLE", 2),
        yellow_order_fraction_multiplier=getenv_float("YELLOW_ORDER_FRACTION_MULTIPLIER", 0.50),
        rank_candidates_enabled=getenv_bool("RANK_CANDIDATES_ENABLED", True),
        reentry_guard_enabled=getenv_bool("REENTRY_GUARD_ENABLED", True),
        reentry_lookback_days=getenv_int("REENTRY_LOOKBACK_DAYS", 10),
        reentry_min_discount_pct=getenv_float("REENTRY_MIN_DISCOUNT_PCT", 0.02),
        reentry_require_above_sma50=getenv_bool("REENTRY_REQUIRE_ABOVE_SMA50", True),
        reentry_allow_in_yellow=getenv_bool("REENTRY_ALLOW_IN_YELLOW", False),
        reentry_allow_without_sell_price=getenv_bool("REENTRY_ALLOW_WITHOUT_SELL_PRICE", False),
        concentration_gate_enabled=getenv_bool("CONCENTRATION_GATE_ENABLED", True),
        risk_map_worksheet_name=os.getenv("RISK_MAP_WORKSHEET_NAME", "Symbol Risk Map").strip() or "Symbol Risk Map",
        unknown_classification_policy=unknown_classification_policy,
        market_timezone=os.getenv("MARKET_TIMEZONE", "America/New_York").strip() or "America/New_York",
        max_new_per_sector_per_day=max(0, getenv_int("MAX_NEW_PER_SECTOR_PER_DAY", 1)),
        max_new_per_risk_group_per_day=max(0, getenv_int("MAX_NEW_PER_RISK_GROUP_PER_DAY", 1)),
        max_open_positions_per_sector=max(0, getenv_int("MAX_OPEN_POSITIONS_PER_SECTOR", 5)),
        max_open_positions_per_risk_group=max(0, getenv_int("MAX_OPEN_POSITIONS_PER_RISK_GROUP", 3)),
        max_sector_exposure_pct=max(0.0, getenv_float("MAX_SECTOR_EXPOSURE_PCT", 0.15)),
        max_risk_group_exposure_pct=max(0.0, getenv_float("MAX_RISK_GROUP_EXPOSURE_PCT", 0.08)),
        max_daily_sector_notional_pct=max(0.0, getenv_float("MAX_DAILY_SECTOR_NOTIONAL_PCT", 0.03)),
        max_daily_risk_group_notional_pct=max(0.0, getenv_float("MAX_DAILY_RISK_GROUP_NOTIONAL_PCT", 0.02)),
        stress_freeze_enabled=getenv_bool("STRESS_FREEZE_ENABLED", True),
        stress_min_open_positions=max(1, getenv_int("STRESS_MIN_OPEN_POSITIONS", 3)),
        stress_red_position_pct=clamp(getenv_float("STRESS_RED_POSITION_PCT", 0.60), 0.0, 1.0),
        stress_aggregate_return_pct=-abs(getenv_float("STRESS_AGGREGATE_RETURN_PCT", -0.015)),
        bid_to_market_steps=steps,
        step_timeout_seconds=getenv_float("STEP_TIMEOUT_SECONDS", 5.0),
        total_chase_timeout_seconds=getenv_float("TOTAL_CHASE_TIMEOUT_SECONDS", 30.0),
        order_poll_interval_seconds=getenv_float("ORDER_POLL_INTERVAL_SECONDS", 2.0),
        treat_partial_fill_as_success=getenv_bool("TREAT_PARTIAL_FILL_AS_SUCCESS", True),
        order_failure_cooldown_seconds=getenv_float("ORDER_FAILURE_COOLDOWN_SECONDS", 1800.0),
        error_body_max_chars=getenv_int("ERROR_BODY_MAX_CHARS", 800),
        request_timeout_seconds=getenv_float("REQUEST_TIMEOUT_SECONDS", 10.0),
        request_retries=getenv_int("REQUEST_RETRIES", 3),
        request_sleep_seconds=getenv_float("REQUEST_SLEEP_SECONDS", 0.25),
        rate_limit_sleep_seconds=getenv_float("RATE_LIMIT_SLEEP_SECONDS", 10.0),
        error_sleep_seconds=getenv_float("ERROR_SLEEP_SECONDS", 15.0),
    )


# -----------------------------
# Shared runtime state
# -----------------------------

app = FastAPI(title="Alpaca Score Buyer")
_stop_event = threading.Event()
_worker_thread: Optional[threading.Thread] = None
_state_lock = threading.Lock()
_state: Dict[str, Any] = {
    "started_at": None,
    "last_cycle_started_at": None,
    "last_cycle_finished_at": None,
    "last_error": None,
    "last_candidates": 0,
    "last_orders_submitted": 0,
    "last_orders_filled_or_partial": 0,
    "last_skipped_existing": 0,
    "last_skipped_notional": 0,
    "last_skipped_recent_failures": 0,
    "last_skipped_product_name_block": 0,
    "last_skipped_asset_lookup": 0,
    "last_skipped_sma200_sized_below_min": 0,
    "last_skipped_entry_sized_below_min": 0,
    "last_skipped_reentry_guard": 0,
    "last_skipped_unknown_classification": 0,
    "last_skipped_sector_daily_limit": 0,
    "last_skipped_group_daily_limit": 0,
    "last_skipped_sector_position_limit": 0,
    "last_skipped_group_position_limit": 0,
    "last_skipped_sector_exposure": 0,
    "last_skipped_group_exposure": 0,
    "last_skipped_sector_daily_notional": 0,
    "last_skipped_group_daily_notional": 0,
    "last_skipped_sector_stress": 0,
    "last_skipped_group_stress": 0,
    "last_risk_mode": None,
    "last_risk_reasons": [],
    "last_position_count": 0,
    "last_open_buy_order_count": 0,
    "last_available_position_slots": 0,
    "last_max_new_orders_this_cycle": 0,
    "last_equity": 0.0,
    "last_cash_pct": 0.0,
    "last_exposure_pct": 0.0,
    "last_margin_use_pct": 0.0,
    "last_drawdown_pct": 0.0,
    "last_red_position_pct": 0.0,
    "last_risk_map_symbols": 0,
    "last_unknown_position_symbols": [],
    "last_concentration_gate_block_reason": None,
    "last_largest_sector": None,
    "last_largest_sector_exposure_pct": 0.0,
    "last_largest_risk_group": None,
    "last_largest_risk_group_exposure_pct": 0.0,
    "last_entry_log_rows": 0,
    "last_order_submit_failures": 0,
}
_order_failure_cooldowns: Dict[str, Tuple[float, str]] = {}
_equity_high_watermark: float = 0.0


# -----------------------------
# Utility helpers
# -----------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def set_state(**kwargs: Any) -> None:
    with _state_lock:
        _state.update(kwargs)


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if value == "":
                return default
        return float(value)
    except (TypeError, ValueError):
        return default


def clean_symbol(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def google_service_info(raw: str) -> Dict[str, Any]:
    """Accept either raw JSON or base64-ish escaped JSON from env vars."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Common Railway copy/paste issue: newlines escaped within the private key are okay after json.loads,
        # but some users paste a surrounding quoted JSON string. Try one extra decode.
        try:
            decoded = json.loads(json.loads(raw))
            if isinstance(decoded, dict):
                return decoded
        except Exception:
            pass
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON")


def request_headers(cfg: Config) -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": cfg.alpaca_api_key,
        "APCA-API-SECRET-KEY": cfg.alpaca_secret_key,
        "Content-Type": "application/json",
    }


def response_body_for_log(resp: requests.Response, cfg: Config) -> str:
    body = (resp.text or "").strip().replace("\n", " ")
    max_chars = max(0, cfg.error_body_max_chars)
    if max_chars <= 0:
        return ""
    if len(body) > max_chars:
        return body[:max_chars] + "...[truncated]"
    return body


def raise_for_status_with_body(resp: requests.Response, cfg: Config, method: str, url: str) -> None:
    if resp.status_code >= 400:
        raise HttpStatusError(method, url, resp.status_code, response_body_for_log(resp, cfg))


def sleep_if_configured(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


# -----------------------------
# HTTP helpers
# -----------------------------


def http_get(session: requests.Session, url: str, cfg: Config, *, params: Optional[Dict[str, Any]] = None) -> Any:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max(1, cfg.request_retries) + 1):
        try:
            resp = session.get(
                url,
                headers=request_headers(cfg),
                params=params,
                timeout=cfg.request_timeout_seconds,
            )
            if resp.status_code == 429:
                msg = f"rate limited status 429: {response_body_for_log(resp, cfg)}"
                if attempt < cfg.request_retries:
                    log.warning("GET rate limited attempt=%s url=%s err=%s; retrying in %.1fs", attempt, url, msg, cfg.rate_limit_sleep_seconds)
                    time.sleep(cfg.rate_limit_sleep_seconds)
                    continue
                raise RuntimeError(msg)
            if 500 <= resp.status_code < 600:
                msg = f"retryable status {resp.status_code}: {response_body_for_log(resp, cfg)}"
                if attempt < cfg.request_retries:
                    backoff = min(2 ** attempt, 30)
                    log.warning("GET failed attempt=%s url=%s err=%s; retrying in %.1fs", attempt, url, msg, backoff)
                    time.sleep(backoff)
                    continue
                raise RuntimeError(msg)
            raise_for_status_with_body(resp, cfg, "GET", url)
            sleep_if_configured(cfg.request_sleep_seconds)
            return resp.json() if resp.text else None
        except Exception as exc:
            last_exc = exc
            if attempt < cfg.request_retries:
                backoff = min(2 ** attempt, 30)
                log.warning("GET failed attempt=%s url=%s err=%s; retrying in %.1fs", attempt, url, exc, backoff)
                time.sleep(backoff)
                continue
    raise RuntimeError(f"GET failed after {cfg.request_retries} attempts: {url}: {last_exc}")


def http_post_once(session: requests.Session, url: str, cfg: Config, *, payload: Dict[str, Any]) -> Any:
    """Submit orders without automatic retry to avoid duplicate unknown-outcome POSTs."""
    resp = session.post(
        url,
        headers=request_headers(cfg),
        data=json.dumps(payload),
        timeout=cfg.request_timeout_seconds,
    )
    if resp.status_code == 429:
        raise HttpStatusError("POST", url, resp.status_code, response_body_for_log(resp, cfg))
    raise_for_status_with_body(resp, cfg, "POST", url)
    sleep_if_configured(cfg.request_sleep_seconds)
    return resp.json() if resp.text else None


def http_delete_once(session: requests.Session, url: str, cfg: Config) -> Optional[Any]:
    resp = session.delete(url, headers=request_headers(cfg), timeout=cfg.request_timeout_seconds)
    if resp.status_code in {404, 422}:
        # The order may already be filled/canceled; caller will re-check if needed.
        log.info("DELETE returned status=%s url=%s body=%s", resp.status_code, url, response_body_for_log(resp, cfg))
        return None
    raise_for_status_with_body(resp, cfg, "DELETE", url)
    sleep_if_configured(cfg.request_sleep_seconds)
    return resp.json() if resp.text else None


# -----------------------------
# Google Sheets
# -----------------------------


def create_gspread_client(cfg: Config) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info = google_service_info(cfg.google_service_account_json)
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_worksheet(gc: gspread.Client, cfg: Config) -> gspread.Worksheet:
    sheet = gc.open_by_key(cfg.google_sheet_id)
    return sheet.worksheet(cfg.google_worksheet_name)


def score_matches(value: Any, target: float) -> bool:
    """Return True when the row score meets or exceeds BUY_SCORE.

    Example: if BUY_SCORE=99.5, scores 99.5, 99.6, and 100 match.
    Blank/non-numeric scores do not match.
    """
    score = to_float(value, default=math.nan)
    if math.isnan(score):
        return False
    return score >= target


ENTRY_LOG_HEADERS = [
    "timestamp",
    "symbol",
    "asset_name",
    "score",
    "close",
    "sma_200",
    "sma_50",
    "pos_52w",
    "dollar_vol_m",
    "close_vs_sma200_pct",
    "close_vs_sma50_pct",
    "base_notional",
    "sma200_sizing_multiplier",
    "final_notional",
    "buying_power_field",
    "buying_power",
    "order_fraction",
    "submitted",
    "filled_or_partial",
    "failure_status",
    "failure_message",
    "regt_fallback_used",
    "regt_buying_power",
    "alpaca_paper",
    "sma50_sizing_multiplier",
    "entry_sizing_multiplier",
    "sma50_sizing_reason",
    "entry_sizing_reason",
    "rank_score",
    "rank_reason",
    "risk_mode",
    "risk_reasons",
    "position_count",
    "available_position_slots",
    "reentry_decision",
    "app_version",
    "sector",
    "risk_group",
    "sector_position_count_before",
    "group_position_count_before",
    "sector_exposure_pct_before",
    "group_exposure_pct_before",
    "sector_entries_today",
    "group_entries_today",
    "sector_daily_notional_pct_before",
    "group_daily_notional_pct_before",
    "sector_stressed",
    "group_stressed",
    "concentration_decision",
]

RISK_MAP_HEADERS = ["symbol", "sector", "risk_group"]


def open_or_create_worksheet(gc: gspread.Client, cfg: Config, title: str, headers: Sequence[str]) -> gspread.Worksheet:
    sheet = gc.open_by_key(cfg.google_sheet_id)
    try:
        ws = sheet.worksheet(title)
    except WorksheetNotFound:
        ws = sheet.add_worksheet(title=title, rows=1000, cols=max(1, len(headers)))
        log.info("Created worksheet %r for entry logging", title)

    try:
        if ws.col_count < len(headers):
            ws.resize(cols=len(headers))
        existing = ws.row_values(1)
        if existing[: len(headers)] != list(headers):
            ws.update("A1", [list(headers)])
            log.info("Initialized headers on worksheet %r", title)
    except Exception as exc:
        log.warning("Could not initialize headers on worksheet %r: %s", title, exc)

    return ws


def read_buy_candidates(ws: gspread.Worksheet, cfg: Config) -> List[BuyCandidate]:
    """Read screener rows and return ordered unique symbols where score column meets BUY_SCORE."""
    values = ws.get(cfg.screener_range) or []
    candidates: List[BuyCandidate] = []
    seen: Set[str] = set()

    symbol_idx = cfg.symbol_col_index - 1
    score_idx = cfg.score_col_index - 1
    close_idx = cfg.close_col_index - 1
    sma_200_idx = cfg.sma_200_col_index - 1
    sma_50_idx = cfg.sma_50_col_index - 1
    pos_52w_idx = cfg.pos_52w_col_index - 1
    dollar_vol_m_idx = cfg.dollar_vol_m_col_index - 1
    required_idx = max(symbol_idx, score_idx, close_idx, sma_200_idx, sma_50_idx, pos_52w_idx, dollar_vol_m_idx)

    for row_num, row in enumerate(values, start=1):
        if row_num < cfg.start_row:
            continue
        if len(row) <= required_idx:
            continue
        symbol = clean_symbol(row[symbol_idx])
        if not symbol or symbol == "SYMBOL":
            continue

        score = to_float(row[score_idx], default=math.nan)
        if math.isnan(score) or score < cfg.buy_score:
            continue
        if symbol in seen:
            continue

        seen.add(symbol)
        candidates.append(
            BuyCandidate(
                row_num=row_num,
                symbol=symbol,
                close=to_float(row[close_idx], default=0.0),
                sma_200=to_float(row[sma_200_idx], default=0.0),
                sma_50=to_float(row[sma_50_idx], default=0.0),
                pos_52w=to_float(row[pos_52w_idx], default=0.0),
                dollar_vol_m=to_float(row[dollar_vol_m_idx], default=0.0),
                score=score,
            )
        )

    return candidates


def safe_pct(numerator: float, denominator: float) -> Optional[float]:
    if denominator <= 0:
        return None
    return (numerator / denominator) - 1.0


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def extension_sizing_multiplier(
    *,
    distance: Optional[float],
    enabled: bool,
    full_size_distance: float,
    max_reduction_distance: float,
    min_multiplier: float,
    label: str,
) -> Tuple[float, Optional[float], str]:
    """Scale buy size down as price gets farther above a moving average.

    This intentionally never returns zero. The Buyer still avoids hard skips for
    over-extension; it only reduces notional size.
    """
    if not enabled:
        return 1.0, distance, f"{label}_disabled"
    if distance is None:
        return 1.0, distance, f"missing_or_invalid_{label}"

    full_size_distance = max(0.0, full_size_distance)
    max_reduction_distance = max(full_size_distance + 0.000001, max_reduction_distance)
    min_multiplier = clamp(min_multiplier, 0.01, 1.0)

    extension = max(0.0, distance)
    if extension <= full_size_distance:
        return 1.0, distance, f"{label}_full_size"

    progress = clamp((extension - full_size_distance) / (max_reduction_distance - full_size_distance), 0.0, 1.0)
    multiplier = 1.0 - ((1.0 - min_multiplier) * progress)
    return clamp(multiplier, min_multiplier, 1.0), distance, f"scaled_by_{label}_distance"


def sma200_sizing_multiplier(candidate: BuyCandidate, cfg: Config) -> Tuple[float, Optional[float], str]:
    """Scale buy size down as price gets farther above SMA 200.

    Defaults:
    - At 0% above SMA 200: 100% of normal order size.
    - At 50%+ above SMA 200: 25% of normal order size.
    - Between those distances: linear scale-down.
    """
    return extension_sizing_multiplier(
        distance=safe_pct(candidate.close, candidate.sma_200),
        enabled=cfg.sma200_sizing_enabled,
        full_size_distance=cfg.sma200_sizing_full_size_distance,
        max_reduction_distance=cfg.sma200_sizing_max_reduction_distance,
        min_multiplier=cfg.sma200_sizing_min_multiplier,
        label="sma200",
    )


def sma50_sizing_multiplier(candidate: BuyCandidate, cfg: Config) -> Tuple[float, Optional[float], str]:
    """Scale buy size down as price gets farther above SMA 50.

    Defaults:
    - At <= 15% above SMA 50: 100% of SMA200-adjusted order size.
    - At 30%+ above SMA 50: 50% of SMA200-adjusted order size.
    - Between those distances: linear scale-down.
    """
    return extension_sizing_multiplier(
        distance=safe_pct(candidate.close, candidate.sma_50),
        enabled=cfg.sma50_sizing_enabled,
        full_size_distance=cfg.sma50_sizing_full_size_distance,
        max_reduction_distance=cfg.sma50_sizing_max_reduction_distance,
        min_multiplier=cfg.sma50_sizing_min_multiplier,
        label="sma50",
    )


def append_entry_log_row(entry_log_ws: Optional[gspread.Worksheet], row: Sequence[Any]) -> bool:
    if entry_log_ws is None:
        return False
    try:
        entry_log_ws.append_row(list(row), value_input_option="USER_ENTERED")
        return True
    except Exception as exc:
        log.warning("Could not append entry log row: %s", exc)
        return False



# -----------------------------
# Sector / correlated-risk concentration helpers
# -----------------------------


def normalize_bucket_label(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def bucket_key(value: Any) -> str:
    return normalize_bucket_label(value).casefold()


def parse_iso_datetime(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def cell_is_true(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def market_timezone(cfg: Config) -> ZoneInfo:
    try:
        return ZoneInfo(cfg.market_timezone)
    except Exception as exc:
        log.warning("Invalid MARKET_TIMEZONE=%r; using America/New_York err=%s", cfg.market_timezone, exc)
        return ZoneInfo("America/New_York")


def read_symbol_risk_map(
    risk_map_ws: gspread.Worksheet,
) -> Dict[str, SymbolClassification]:
    values = risk_map_ws.get_all_values()
    if not values:
        return {}

    headers = [str(value).strip().lower() for value in values[0]]
    try:
        symbol_idx = headers.index("symbol")
        sector_idx = headers.index("sector")
        group_idx = headers.index("risk_group")
    except ValueError as exc:
        raise RuntimeError(
            "Symbol Risk Map must have headers: symbol, sector, risk_group"
        ) from exc

    result: Dict[str, SymbolClassification] = {}
    for row in values[1:]:
        symbol = clean_symbol(row[symbol_idx] if symbol_idx < len(row) else "")
        sector = normalize_bucket_label(row[sector_idx] if sector_idx < len(row) else "")
        risk_group = normalize_bucket_label(row[group_idx] if group_idx < len(row) else "")
        if not symbol or not sector or not risk_group:
            continue
        result[symbol] = SymbolClassification(symbol=symbol, sector=sector, risk_group=risk_group)
    return result


def classification_for_symbol(
    symbol: str,
    risk_map: Dict[str, SymbolClassification],
    cfg: Config,
) -> Optional[SymbolClassification]:
    classification = risk_map.get(clean_symbol(symbol))
    if classification is not None:
        return classification
    if cfg.unknown_classification_policy == "bucket":
        return SymbolClassification(
            symbol=clean_symbol(symbol),
            sector="Unclassified",
            risk_group="Unclassified",
        )
    return None


def read_today_successful_entries(
    entry_log_ws: Optional[gspread.Worksheet],
    risk_map: Dict[str, SymbolClassification],
    cfg: Config,
) -> Tuple[CounterType[str], CounterType[str], CounterType[str], CounterType[str]]:
    sector_entries: CounterType[str] = Counter()
    group_entries: CounterType[str] = Counter()
    sector_notional: CounterType[str] = Counter()
    group_notional: CounterType[str] = Counter()
    if entry_log_ws is None:
        return sector_entries, group_entries, sector_notional, group_notional

    try:
        values = entry_log_ws.get_all_values()
    except Exception as exc:
        log.warning("Could not read Buyer Entry Log for daily concentration counters: %s", exc)
        return sector_entries, group_entries, sector_notional, group_notional
    if len(values) < 2:
        return sector_entries, group_entries, sector_notional, group_notional

    headers = [str(value).strip() for value in values[0]]
    header_idx = {name: idx for idx, name in enumerate(headers)}
    required = {"timestamp", "symbol", "filled_or_partial", "final_notional"}
    if not required.issubset(header_idx):
        log.warning("Buyer Entry Log is missing fields needed for daily concentration counters")
        return sector_entries, group_entries, sector_notional, group_notional

    tz = market_timezone(cfg)
    today = datetime.now(tz).date()
    for row in values[1:]:
        def cell(name: str) -> str:
            idx = header_idx.get(name)
            return row[idx] if idx is not None and idx < len(row) else ""

        timestamp = parse_iso_datetime(cell("timestamp"))
        if timestamp is None:
            continue
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        if timestamp.astimezone(tz).date() != today:
            continue
        if not cell_is_true(cell("filled_or_partial")):
            continue

        symbol = clean_symbol(cell("symbol"))
        classification = risk_map.get(symbol)
        sector = normalize_bucket_label(cell("sector"))
        risk_group = normalize_bucket_label(cell("risk_group"))
        if not sector and classification:
            sector = classification.sector
        if not risk_group and classification:
            risk_group = classification.risk_group
        if not sector or not risk_group:
            continue

        notional = max(0.0, to_float(cell("final_notional"), default=0.0))
        sector_key = bucket_key(sector)
        group_key = bucket_key(risk_group)
        sector_entries[sector_key] += 1
        group_entries[group_key] += 1
        sector_notional[sector_key] += notional
        group_notional[group_key] += notional

    return sector_entries, group_entries, sector_notional, group_notional


def get_or_create_bucket(
    buckets: Dict[str, BucketStats],
    label: str,
) -> BucketStats:
    key = bucket_key(label)
    bucket = buckets.get(key)
    if bucket is None:
        bucket = BucketStats(label=normalize_bucket_label(label))
        buckets[key] = bucket
    return bucket


def build_concentration_state(
    positions: Sequence[PositionSnapshot],
    risk_map: Dict[str, SymbolClassification],
    entry_log_ws: Optional[gspread.Worksheet],
    cfg: Config,
) -> ConcentrationState:
    sector_entries, group_entries, sector_notional, group_notional = read_today_successful_entries(
        entry_log_ws, risk_map, cfg
    )
    state = ConcentrationState(
        sector_stats={},
        group_stats={},
        daily_sector_entries=sector_entries,
        daily_group_entries=group_entries,
        daily_sector_notional=sector_notional,
        daily_group_notional=group_notional,
        unknown_position_symbols=set(),
    )

    for position in positions:
        classification = classification_for_symbol(position.symbol, risk_map, cfg)
        if classification is None:
            state.unknown_position_symbols.add(position.symbol)
            continue

        market_value = max(0.0, position.market_value)
        cost_basis = market_value - position.unrealized_pl
        if cost_basis <= 0:
            cost_basis = market_value
        is_red = position.unrealized_pl < 0 or position.unrealized_plpc < 0

        for bucket in (
            get_or_create_bucket(state.sector_stats, classification.sector),
            get_or_create_bucket(state.group_stats, classification.risk_group),
        ):
            bucket.position_count += 1
            bucket.market_value += market_value
            bucket.unrealized_pl += position.unrealized_pl
            bucket.cost_basis += max(0.0, cost_basis)
            if is_red:
                bucket.red_position_count += 1

    return state


def bucket_is_stressed(bucket: BucketStats, cfg: Config) -> bool:
    if not cfg.stress_freeze_enabled:
        return False
    return (
        bucket.position_count >= cfg.stress_min_open_positions
        and bucket.red_position_pct >= cfg.stress_red_position_pct
        and bucket.aggregate_return_pct <= cfg.stress_aggregate_return_pct
    )


def evaluate_concentration(
    classification: SymbolClassification,
    concentration: ConcentrationState,
    equity: float,
    proposed_notional: float,
    cfg: Config,
) -> ConcentrationCheck:
    sector_key = bucket_key(classification.sector)
    group_key = bucket_key(classification.risk_group)
    sector_bucket = concentration.sector_stats.get(
        sector_key, BucketStats(label=classification.sector)
    )
    group_bucket = concentration.group_stats.get(
        group_key, BucketStats(label=classification.risk_group)
    )

    sector_exposure_before = positive_ratio(sector_bucket.market_value, equity)
    group_exposure_before = positive_ratio(group_bucket.market_value, equity)
    sector_daily_notional_before = positive_ratio(
        float(concentration.daily_sector_notional.get(sector_key, 0.0)), equity
    )
    group_daily_notional_before = positive_ratio(
        float(concentration.daily_group_notional.get(group_key, 0.0)), equity
    )
    sector_entries_today = int(concentration.daily_sector_entries.get(sector_key, 0))
    group_entries_today = int(concentration.daily_group_entries.get(group_key, 0))
    sector_stressed = bucket_is_stressed(sector_bucket, cfg)
    group_stressed = bucket_is_stressed(group_bucket, cfg)

    reason = "concentration_gate_disabled"
    allowed = True
    if cfg.concentration_gate_enabled:
        reason = "concentration_ok"
        if cfg.max_new_per_sector_per_day > 0 and sector_entries_today >= cfg.max_new_per_sector_per_day:
            allowed = False
            reason = "sector_daily_limit"
        elif cfg.max_new_per_risk_group_per_day > 0 and group_entries_today >= cfg.max_new_per_risk_group_per_day:
            allowed = False
            reason = "group_daily_limit"
        elif cfg.max_open_positions_per_sector > 0 and sector_bucket.position_count >= cfg.max_open_positions_per_sector:
            allowed = False
            reason = "sector_position_limit"
        elif cfg.max_open_positions_per_risk_group > 0 and group_bucket.position_count >= cfg.max_open_positions_per_risk_group:
            allowed = False
            reason = "group_position_limit"
        elif sector_stressed:
            allowed = False
            reason = "sector_stress"
        elif group_stressed:
            allowed = False
            reason = "group_stress"
        elif equity <= 0:
            allowed = False
            reason = "concentration_equity_invalid"
        else:
            projected_sector_exposure = positive_ratio(
                sector_bucket.market_value + max(0.0, proposed_notional), equity
            )
            projected_group_exposure = positive_ratio(
                group_bucket.market_value + max(0.0, proposed_notional), equity
            )
            projected_sector_daily_notional = positive_ratio(
                float(concentration.daily_sector_notional.get(sector_key, 0.0))
                + max(0.0, proposed_notional),
                equity,
            )
            projected_group_daily_notional = positive_ratio(
                float(concentration.daily_group_notional.get(group_key, 0.0))
                + max(0.0, proposed_notional),
                equity,
            )
            if cfg.max_sector_exposure_pct > 0 and projected_sector_exposure > cfg.max_sector_exposure_pct:
                allowed = False
                reason = "sector_exposure"
            elif cfg.max_risk_group_exposure_pct > 0 and projected_group_exposure > cfg.max_risk_group_exposure_pct:
                allowed = False
                reason = "group_exposure"
            elif (
                cfg.max_daily_sector_notional_pct > 0
                and projected_sector_daily_notional > cfg.max_daily_sector_notional_pct
            ):
                allowed = False
                reason = "sector_daily_notional"
            elif (
                cfg.max_daily_risk_group_notional_pct > 0
                and projected_group_daily_notional > cfg.max_daily_risk_group_notional_pct
            ):
                allowed = False
                reason = "group_daily_notional"

    return ConcentrationCheck(
        allowed=allowed,
        reason=reason,
        sector=classification.sector,
        risk_group=classification.risk_group,
        sector_position_count_before=sector_bucket.position_count,
        group_position_count_before=group_bucket.position_count,
        sector_exposure_pct_before=sector_exposure_before,
        group_exposure_pct_before=group_exposure_before,
        sector_entries_today=sector_entries_today,
        group_entries_today=group_entries_today,
        sector_daily_notional_pct_before=sector_daily_notional_before,
        group_daily_notional_pct_before=group_daily_notional_before,
        sector_stressed=sector_stressed,
        group_stressed=group_stressed,
    )


def apply_filled_entry_to_concentration(
    concentration: ConcentrationState,
    classification: SymbolClassification,
    notional: float,
) -> None:
    sector_key = bucket_key(classification.sector)
    group_key = bucket_key(classification.risk_group)
    for bucket in (
        get_or_create_bucket(concentration.sector_stats, classification.sector),
        get_or_create_bucket(concentration.group_stats, classification.risk_group),
    ):
        bucket.position_count += 1
        bucket.market_value += max(0.0, notional)
        bucket.cost_basis += max(0.0, notional)

    concentration.daily_sector_entries[sector_key] += 1
    concentration.daily_group_entries[group_key] += 1
    concentration.daily_sector_notional[sector_key] += max(0.0, notional)
    concentration.daily_group_notional[group_key] += max(0.0, notional)


def concentration_skip_counter_name(reason: str) -> Optional[str]:
    return {
        "sector_daily_limit": "skipped_sector_daily_limit",
        "group_daily_limit": "skipped_group_daily_limit",
        "sector_position_limit": "skipped_sector_position_limit",
        "group_position_limit": "skipped_group_position_limit",
        "sector_exposure": "skipped_sector_exposure",
        "group_exposure": "skipped_group_exposure",
        "sector_daily_notional": "skipped_sector_daily_notional",
        "group_daily_notional": "skipped_group_daily_notional",
        "sector_stress": "skipped_sector_stress",
        "group_stress": "skipped_group_stress",
    }.get(reason)


def set_concentration_state(
    concentration: ConcentrationState,
    risk_map_size: int,
    equity: float,
) -> None:
    largest_sector = max(
        concentration.sector_stats.values(), key=lambda item: item.market_value, default=None
    )
    largest_group = max(
        concentration.group_stats.values(), key=lambda item: item.market_value, default=None
    )
    set_state(
        last_risk_map_symbols=risk_map_size,
        last_unknown_position_symbols=sorted(concentration.unknown_position_symbols),
        last_largest_sector=largest_sector.label if largest_sector else None,
        last_largest_sector_exposure_pct=(
            round(positive_ratio(largest_sector.market_value, equity), 6) if largest_sector else 0.0
        ),
        last_largest_risk_group=largest_group.label if largest_group else None,
        last_largest_risk_group_exposure_pct=(
            round(positive_ratio(largest_group.market_value, equity), 6) if largest_group else 0.0
        ),
    )


# -----------------------------
# Alpaca trading helpers
# -----------------------------


def get_account(session: requests.Session, cfg: Config) -> Dict[str, Any]:
    return http_get(session, f"{cfg.trading_base_url}/v2/account", cfg)


def choose_buying_power(account: Dict[str, Any]) -> Tuple[str, float]:
    field = "buying_power"
    return field, to_float(account.get(field), default=0.0)


def regt_fallback_notional(account: Dict[str, Any], cfg: Config, primary_notional: float) -> Tuple[float, float]:
    regt_buying_power = to_float(account.get("regt_buying_power"), default=0.0)
    if regt_buying_power <= 0:
        return 0.0, regt_buying_power
    return min(primary_notional, round(regt_buying_power * cfg.regt_fallback_fraction, 2)), regt_buying_power


def should_retry_with_regt(attempt: OrderAttemptResult) -> bool:
    return (
        attempt.failure_status == 403
        and "insufficient regt buying power" in attempt.failure_message.lower()
    )


def list_positions(session: requests.Session, cfg: Config) -> List[PositionSnapshot]:
    try:
        positions = http_get(session, f"{cfg.trading_base_url}/v2/positions", cfg)
    except Exception as exc:
        # Alpaca returns an empty list when no positions in normal operation; this is just defensive.
        log.warning("Could not list positions: %s", exc)
        return []
    if not isinstance(positions, list):
        return []

    result: List[PositionSnapshot] = []
    for p in positions:
        if not isinstance(p, dict):
            continue
        symbol = clean_symbol(p.get("symbol"))
        if not symbol:
            continue
        result.append(
            PositionSnapshot(
                symbol=symbol,
                qty=to_float(p.get("qty"), default=0.0),
                market_value=to_float(p.get("market_value"), default=0.0),
                unrealized_pl=to_float(p.get("unrealized_pl"), default=0.0),
                unrealized_plpc=to_float(p.get("unrealized_plpc"), default=0.0),
            )
        )
    return result


def list_position_symbols(session: requests.Session, cfg: Config) -> Set[str]:
    positions = list_positions(session, cfg)
    if not positions:
        return set()
    return {p.symbol for p in positions}


def list_open_buy_order_symbols(session: requests.Session, cfg: Config) -> Set[str]:
    params = {"status": "open", "limit": 500, "direction": "desc"}
    orders = http_get(session, f"{cfg.trading_base_url}/v2/orders", cfg, params=params)
    if not isinstance(orders, list):
        return set()
    result = set()
    for order in orders:
        if str(order.get("side", "")).lower() == "buy":
            sym = clean_symbol(order.get("symbol"))
            if sym:
                result.add(sym)
    return result


def positive_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def candidate_quality_score(candidate: BuyCandidate) -> Tuple[float, str]:
    """Secondary ranking for candidates that already passed BUY_SCORE.

    The Sheet score remains the primary ranking input. This tie-breaker favors:
    - stronger 52-week position,
    - better liquidity,
    - price above major trend lines,
    - less over-extension above SMA50/SMA200.
    """
    pos_score = clamp(candidate.pos_52w, 0.0, 1.0) * 100.0
    liquidity_score = min(25.0, math.log10(max(candidate.dollar_vol_m, 0.0) + 1.0) * 8.0)

    close_vs_sma50 = safe_pct(candidate.close, candidate.sma_50)
    close_vs_sma200 = safe_pct(candidate.close, candidate.sma_200)

    trend_bonus = 0.0
    if close_vs_sma50 is not None and close_vs_sma50 >= 0:
        trend_bonus += 10.0
    if close_vs_sma200 is not None and close_vs_sma200 >= 0:
        trend_bonus += 5.0

    extension_penalty = 0.0
    if close_vs_sma50 is not None:
        extension_penalty += max(0.0, close_vs_sma50 - 0.15) * 120.0
    if close_vs_sma200 is not None:
        extension_penalty += max(0.0, close_vs_sma200 - 0.50) * 60.0

    quality = pos_score + liquidity_score + trend_bonus - extension_penalty
    sma50_text = f"{close_vs_sma50:.4f}" if close_vs_sma50 is not None else "n/a"
    sma200_text = f"{close_vs_sma200:.4f}" if close_vs_sma200 is not None else "n/a"
    reason = (
        f"pos52w={candidate.pos_52w:.4f}|dollar_vol_m={candidate.dollar_vol_m:.2f}|"
        f"close_vs_sma50={sma50_text}|close_vs_sma200={sma200_text}"
    )
    return quality, reason


def ranked_buy_candidates(candidates: Sequence[BuyCandidate], cfg: Config) -> List[BuyCandidate]:
    if not cfg.rank_candidates_enabled:
        return list(candidates)

    return sorted(
        candidates,
        key=lambda c: (
            c.score,
            candidate_quality_score(c)[0],
            -c.row_num,
        ),
        reverse=True,
    )


def update_equity_high_watermark(equity: float, cfg: Config) -> float:
    global _equity_high_watermark
    configured = max(0.0, cfg.equity_high_watermark)
    if _equity_high_watermark <= 0:
        _equity_high_watermark = max(configured, equity)
    else:
        _equity_high_watermark = max(_equity_high_watermark, configured, equity)
    return _equity_high_watermark


def build_risk_snapshot(
    account: Dict[str, Any],
    positions: Sequence[PositionSnapshot],
    open_buy_orders: Set[str],
    cfg: Config,
) -> RiskSnapshot:
    equity = to_float(account.get("equity"), default=0.0)
    cash = to_float(account.get("cash"), default=0.0)
    long_market_value = to_float(account.get("long_market_value"), default=0.0)
    if long_market_value <= 0 and positions:
        long_market_value = sum(max(0.0, p.market_value) for p in positions)

    initial_margin = to_float(account.get("initial_margin"), default=0.0)
    high_watermark = update_equity_high_watermark(equity, cfg)

    cash_pct = positive_ratio(cash, equity)
    exposure_pct = positive_ratio(long_market_value, equity)
    margin_use_pct = positive_ratio(initial_margin, equity)
    if margin_use_pct <= 0 and cash < 0:
        margin_use_pct = exposure_pct
    drawdown_pct = positive_ratio(equity, high_watermark) - 1.0 if high_watermark > 0 else 0.0

    position_count = len(positions)
    open_buy_order_count = len(open_buy_orders)
    red_position_count = sum(1 for p in positions if p.unrealized_plpc < 0 or p.unrealized_pl < 0)
    green_position_count = sum(1 for p in positions if p.unrealized_plpc > 0 or p.unrealized_pl > 0)
    red_position_pct = positive_ratio(red_position_count, position_count)

    if cfg.max_total_positions > 0:
        available_position_slots = max(0, cfg.max_total_positions - position_count - open_buy_order_count)
    else:
        available_position_slots = 1_000_000

    red_reasons: List[str] = []
    yellow_reasons: List[str] = []

    if not cfg.risk_gate_enabled:
        mode = "GREEN"
        reasons: Tuple[str, ...] = ("risk_gate_disabled",)
    else:
        if equity <= 0:
            red_reasons.append("equity<=0")
        if to_float(account.get("buying_power"), default=0.0) <= 0:
            red_reasons.append("buying_power<=0")
        if cfg.max_total_positions > 0 and available_position_slots <= 0:
            red_reasons.append(f"position_cap_reached:{position_count}+{open_buy_order_count}>={cfg.max_total_positions}")
        if exposure_pct >= cfg.red_exposure_pct:
            red_reasons.append(f"exposure_pct={exposure_pct:.2%}>={cfg.red_exposure_pct:.2%}")
        elif exposure_pct >= cfg.yellow_exposure_pct:
            yellow_reasons.append(f"exposure_pct={exposure_pct:.2%}>={cfg.yellow_exposure_pct:.2%}")
        if cash_pct <= cfg.red_min_cash_pct:
            red_reasons.append(f"cash_pct={cash_pct:.2%}<={cfg.red_min_cash_pct:.2%}")
        elif cash_pct <= cfg.yellow_min_cash_pct:
            yellow_reasons.append(f"cash_pct={cash_pct:.2%}<={cfg.yellow_min_cash_pct:.2%}")
        if margin_use_pct >= cfg.red_margin_use_pct:
            red_reasons.append(f"margin_use_pct={margin_use_pct:.2%}>={cfg.red_margin_use_pct:.2%}")
        elif margin_use_pct >= cfg.yellow_margin_use_pct:
            yellow_reasons.append(f"margin_use_pct={margin_use_pct:.2%}>={cfg.yellow_margin_use_pct:.2%}")
        if position_count > 0:
            if red_position_pct >= cfg.red_red_position_pct:
                red_reasons.append(f"red_position_pct={red_position_pct:.2%}>={cfg.red_red_position_pct:.2%}")
            elif red_position_pct >= cfg.yellow_red_position_pct:
                yellow_reasons.append(f"red_position_pct={red_position_pct:.2%}>={cfg.yellow_red_position_pct:.2%}")
        if drawdown_pct <= -abs(cfg.red_drawdown_pct):
            red_reasons.append(f"drawdown_pct={drawdown_pct:.2%}<=-{abs(cfg.red_drawdown_pct):.2%}")
        elif drawdown_pct <= -abs(cfg.yellow_drawdown_pct):
            yellow_reasons.append(f"drawdown_pct={drawdown_pct:.2%}<=-{abs(cfg.yellow_drawdown_pct):.2%}")

        if red_reasons:
            mode = "RED"
            reasons = tuple(red_reasons)
        elif yellow_reasons:
            mode = "YELLOW"
            reasons = tuple(yellow_reasons)
        else:
            mode = "GREEN"
            reasons = ("risk_ok",)

    if mode == "RED":
        max_new_orders_this_cycle = 0
        order_fraction_multiplier = 0.0
    elif mode == "YELLOW":
        max_new_orders_this_cycle = cfg.yellow_max_orders_per_cycle if cfg.yellow_max_orders_per_cycle > 0 else available_position_slots
        max_new_orders_this_cycle = min(max_new_orders_this_cycle, available_position_slots)
        order_fraction_multiplier = clamp(cfg.yellow_order_fraction_multiplier, 0.01, 1.0)
    else:
        max_new_orders_this_cycle = available_position_slots
        order_fraction_multiplier = 1.0

    if cfg.max_orders_per_cycle > 0:
        max_new_orders_this_cycle = min(max_new_orders_this_cycle, cfg.max_orders_per_cycle)

    return RiskSnapshot(
        mode=mode,
        reasons=reasons,
        equity=equity,
        cash=cash,
        cash_pct=cash_pct,
        exposure_pct=exposure_pct,
        margin_use_pct=margin_use_pct,
        drawdown_pct=drawdown_pct,
        high_watermark=high_watermark,
        position_count=position_count,
        open_buy_order_count=open_buy_order_count,
        red_position_count=red_position_count,
        green_position_count=green_position_count,
        red_position_pct=red_position_pct,
        available_position_slots=available_position_slots,
        max_new_orders_this_cycle=max_new_orders_this_cycle,
        order_fraction_multiplier=order_fraction_multiplier,
    )


def set_risk_state(risk: RiskSnapshot) -> None:
    set_state(
        last_risk_mode=risk.mode,
        last_risk_reasons=list(risk.reasons),
        last_position_count=risk.position_count,
        last_open_buy_order_count=risk.open_buy_order_count,
        last_available_position_slots=risk.available_position_slots,
        last_max_new_orders_this_cycle=risk.max_new_orders_this_cycle,
        last_equity=round(risk.equity, 4),
        last_cash_pct=round(risk.cash_pct, 6),
        last_exposure_pct=round(risk.exposure_pct, 6),
        last_margin_use_pct=round(risk.margin_use_pct, 6),
        last_drawdown_pct=round(risk.drawdown_pct, 6),
        last_red_position_pct=round(risk.red_position_pct, 6),
    )


def list_recent_sell_fills(session: requests.Session, cfg: Config) -> Dict[str, RecentSellFill]:
    if not cfg.reentry_guard_enabled or cfg.reentry_lookback_days <= 0:
        return {}

    after_dt = datetime.now(timezone.utc) - timedelta(days=cfg.reentry_lookback_days)
    params = {
        "after": after_dt.isoformat(),
        "direction": "desc",
        "page_size": 100,
    }
    try:
        activities = http_get(session, f"{cfg.trading_base_url}/v2/account/activities/FILL", cfg, params=params)
    except Exception as exc:
        log.warning("Could not list recent sell fills for re-entry guard; allowing normal candidate flow err=%s", exc)
        return {}

    result: Dict[str, RecentSellFill] = {}
    if not isinstance(activities, list):
        return result

    for activity in activities:
        if not isinstance(activity, dict):
            continue
        side = str(activity.get("side") or "").strip().lower()
        if side != "sell":
            continue
        symbol = clean_symbol(activity.get("symbol"))
        if not symbol or symbol in result:
            continue
        price = to_float(activity.get("price") or activity.get("filled_avg_price"), default=0.0)
        qty = to_float(activity.get("qty") or activity.get("quantity"), default=0.0)
        tx_time = str(activity.get("transaction_time") or activity.get("date") or "")
        result[symbol] = RecentSellFill(symbol=symbol, price=price, qty=qty, transaction_time=tx_time)
    return result


def reentry_guard_decision(
    candidate: BuyCandidate,
    recent_sell_fills: Dict[str, RecentSellFill],
    risk: RiskSnapshot,
    cfg: Config,
) -> Tuple[bool, str]:
    if not cfg.reentry_guard_enabled:
        return True, "reentry_guard_disabled"
    fill = recent_sell_fills.get(candidate.symbol)
    if not fill:
        return True, "no_recent_sell"

    if risk.mode == "YELLOW" and not cfg.reentry_allow_in_yellow:
        return False, f"recent_sell_{cfg.reentry_lookback_days}d_blocked_in_yellow"
    if risk.mode == "RED":
        return False, f"recent_sell_{cfg.reentry_lookback_days}d_blocked_in_red"

    if fill.price <= 0 or candidate.close <= 0:
        if cfg.reentry_allow_without_sell_price:
            return True, f"recent_sell_no_price_allowed tx={fill.transaction_time}"
        return False, f"recent_sell_no_usable_price tx={fill.transaction_time}"

    discount = (fill.price - candidate.close) / fill.price
    discount_ok = discount >= cfg.reentry_min_discount_pct
    trend_ok = True
    if cfg.reentry_require_above_sma50 and candidate.sma_50 > 0:
        trend_ok = candidate.close >= candidate.sma_50

    if discount_ok and trend_ok:
        return True, (
            f"recent_sell_reentry_allowed discount={discount:.2%} sell_price={fill.price:.4f} "
            f"close={candidate.close:.4f} trend_ok={trend_ok}"
        )

    return False, (
        f"recent_sell_reentry_blocked discount={discount:.2%} min_discount={cfg.reentry_min_discount_pct:.2%} "
        f"sell_price={fill.price:.4f} close={candidate.close:.4f} trend_ok={trend_ok} tx={fill.transaction_time}"
    )


def cooldown_reason(symbol: str) -> Optional[str]:
    entry = _order_failure_cooldowns.get(symbol)
    if not entry:
        return None
    blocked_until, reason = entry
    if time.time() >= blocked_until:
        _order_failure_cooldowns.pop(symbol, None)
        return None
    return reason


def remember_order_failure(symbol: str, cfg: Config, reason: str) -> None:
    if cfg.order_failure_cooldown_seconds <= 0:
        return
    _order_failure_cooldowns[symbol] = (time.time() + cfg.order_failure_cooldown_seconds, reason)


def get_asset(session: requests.Session, cfg: Config, symbol: str) -> Dict[str, Any]:
    asset = http_get(session, f"{cfg.trading_base_url}/v2/assets/{symbol}", cfg)
    return asset if isinstance(asset, dict) else {}


def asset_name(asset: Dict[str, Any]) -> str:
    return str(asset.get("name") or "").strip()


def product_name_block_reason(name: str, cfg: Config) -> Optional[str]:
    """Return a reason when the asset name looks like a leveraged/inverse trading product."""
    pattern = cfg.blocked_product_name_pattern
    if not pattern or not name:
        return None

    try:
        match = re.search(pattern, name, flags=re.IGNORECASE)
    except re.error as exc:
        raise RuntimeError(f"BLOCK_PRODUCT_NAME_PATTERN is not valid regex: {pattern!r}") from exc

    if not match:
        return None

    return f"product name matched blocked pattern {pattern!r}: {name!r}"


def get_snapshot(session: requests.Session, cfg: Config, symbol: str) -> Dict[str, Any]:
    params = {"feed": cfg.alpaca_data_feed} if cfg.alpaca_data_feed else None
    return http_get(session, f"{cfg.data_base_url}/v2/stocks/{symbol}/snapshot", cfg, params=params)


def nested_get(dct: Dict[str, Any], *keys: str) -> Any:
    cur: Any = dct
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def get_bid_ask_last(session: requests.Session, cfg: Config, symbol: str) -> Tuple[float, float, float]:
    try:
        snap = get_snapshot(session, cfg, symbol)
    except Exception as exc:
        log.warning("Failed to get snapshot for %s: %s", symbol, exc)
        return 0.0, 0.0, 0.0

    latest_quote = snap.get("latestQuote") or snap.get("latest_quote") or {}
    latest_trade = snap.get("latestTrade") or snap.get("latest_trade") or {}

    bid = to_float(latest_quote.get("bp") or latest_quote.get("bid_price"), 0.0)
    ask = to_float(latest_quote.get("ap") or latest_quote.get("ask_price"), 0.0)
    last = to_float(latest_trade.get("p") or latest_trade.get("price"), 0.0)
    return bid, ask, last


def round_limit_price(price: float) -> float:
    if price <= 0:
        return 0.0
    # Common equity tick handling: >= $1 uses cents; sub-dollar symbols often allow 4 decimals.
    return round(price, 2 if price >= 1 else 4)


def compute_limit_price(bid: float, ask: float, last: float, step_fraction: float) -> float:
    step_fraction = max(0.0, min(1.0, float(step_fraction)))

    if ask > 0:
        market_price = ask
    elif last > bid:
        market_price = last
    else:
        market_price = bid

    if bid <= 0 and market_price <= 0:
        return 0.0
    if bid <= 0:
        return round_limit_price(market_price)
    if market_price <= bid:
        return round_limit_price(max(bid, market_price))

    return round_limit_price(bid + (market_price - bid) * step_fraction)


def get_order(session: requests.Session, cfg: Config, order_id: str) -> Dict[str, Any]:
    return http_get(session, f"{cfg.trading_base_url}/v2/orders/{order_id}", cfg)


def cancel_order(session: requests.Session, cfg: Config, order_id: str) -> None:
    http_delete_once(session, f"{cfg.trading_base_url}/v2/orders/{order_id}", cfg)


def submit_limit_buy(session: requests.Session, cfg: Config, symbol: str, notional: float, limit_price: float) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "symbol": symbol,
        "notional": f"{round(notional, 2):.2f}",
        "side": "buy",
        "type": "limit",
        "time_in_force": "day",
        "limit_price": str(limit_price),
    }
    if cfg.extended_hours:
        payload["extended_hours"] = True

    log.info("Submitting BUY limit order symbol=%s notional=%.2f limit_price=%s extended_hours=%s", symbol, notional, limit_price, cfg.extended_hours)
    return http_post_once(session, f"{cfg.trading_base_url}/v2/orders", cfg, payload=payload)


def order_status(order: Dict[str, Any]) -> str:
    return str(order.get("status", "")).lower()


def maybe_cancel_live_order(session: requests.Session, cfg: Config, order_id: Optional[str], symbol: str, reason: str) -> None:
    if not order_id:
        return
    try:
        order = get_order(session, cfg, order_id)
        status = order_status(order)
        if status in {"new", "accepted", "pending_new", "partially_filled"}:
            cancel_order(session, cfg, order_id)
            log.info("Canceled order %s for %s status=%s reason=%s", order_id, symbol, status, reason)
    except Exception as exc:
        log.warning("Failed to cancel order %s for %s reason=%s err=%s", order_id, symbol, reason, exc)


def place_best_bid_chasing_order(session: requests.Session, cfg: Config, symbol: str, notional: float) -> OrderAttemptResult:
    """Submit/chase a buy order and report whether Alpaca accepted or filled it."""
    started = time.time()
    last_order_id: Optional[str] = None

    for idx, step_fraction in enumerate(cfg.bid_to_market_steps):
        elapsed = time.time() - started
        if elapsed >= cfg.total_chase_timeout_seconds:
            log.warning("Total chase timeout reached for %s after %.1fs", symbol, elapsed)
            break

        if last_order_id:
            try:
                order = get_order(session, cfg, last_order_id)
                status = order_status(order)
                if status == "filled":
                    log.info("Previous order %s for %s filled while chasing", last_order_id, symbol)
                    return OrderAttemptResult(filled_or_partial=True, submitted=True)
                if status == "partially_filled" and cfg.treat_partial_fill_as_success:
                    maybe_cancel_live_order(session, cfg, last_order_id, symbol, "partial fill accepted")
                    log.info("Previous order %s for %s partially filled; accepting partial and stopping chase", last_order_id, symbol)
                    return OrderAttemptResult(filled_or_partial=True, submitted=True)
                if status not in {"canceled", "rejected", "expired", "done_for_day"}:
                    cancel_order(session, cfg, last_order_id)
                    log.info("Canceled previous order %s for %s to reprice", last_order_id, symbol)
            except Exception as exc:
                log.warning("Error checking/canceling previous order %s for %s: %s", last_order_id, symbol, exc)

        bid, ask, last = get_bid_ask_last(session, cfg, symbol)
        limit_price = compute_limit_price(bid, ask, last, step_fraction)
        if limit_price <= 0:
            log.warning("Skipping %s step=%d: no usable bid/ask/last bid=%.4f ask=%.4f last=%.4f", symbol, idx, bid, ask, last)
            return OrderAttemptResult(filled_or_partial=False, submitted=False)

        log.info(
            "Step %d for %s: bid=%.4f ask=%.4f last=%.4f step_fraction=%.2f -> limit_price=%s",
            idx,
            symbol,
            bid,
            ask,
            last,
            step_fraction,
            limit_price,
        )

        try:
            order = submit_limit_buy(session, cfg, symbol, notional, limit_price)
        except HttpStatusError as exc:
            log.error(
                "Order submit failed for %s at %s on step %d status=%s err=%s",
                symbol,
                limit_price,
                idx,
                exc.status_code,
                exc,
            )
            return OrderAttemptResult(
                filled_or_partial=False,
                submitted=True,
                failure_status=exc.status_code,
                failure_message=str(exc),
            )
        except Exception as exc:
            log.exception("Order submit failed for %s at %s on step %d: %s", symbol, limit_price, idx, exc)
            return OrderAttemptResult(filled_or_partial=False, submitted=True, failure_message=str(exc))

        if not isinstance(order, dict):
            log.warning("Order response for %s on step %d was not an object; aborting chase response=%r", symbol, idx, order)
            return OrderAttemptResult(
                filled_or_partial=False,
                submitted=True,
                failure_message="order response was empty or not an object",
            )

        last_order_id = order.get("id") or order.get("client_order_id")
        if not last_order_id:
            log.warning("Could not determine order id for %s on step %d; aborting chase", symbol, idx)
            return OrderAttemptResult(
                filled_or_partial=False,
                submitted=True,
                failure_message="order response did not include id or client_order_id",
            )

        step_deadline = time.time() + min(cfg.step_timeout_seconds, max(0.0, cfg.total_chase_timeout_seconds - (time.time() - started)))
        while time.time() < step_deadline:
            try:
                order = get_order(session, cfg, last_order_id)
                status = order_status(order)
                if status == "filled":
                    avg = order.get("filled_avg_price") or limit_price
                    log.info("Order %s for %s filled avg_price=%s", last_order_id, symbol, avg)
                    return OrderAttemptResult(filled_or_partial=True, submitted=True)
                if status == "partially_filled" and cfg.treat_partial_fill_as_success:
                    maybe_cancel_live_order(session, cfg, last_order_id, symbol, "partial fill accepted")
                    log.info("Order %s for %s partially filled; accepting partial and stopping chase", last_order_id, symbol)
                    return OrderAttemptResult(filled_or_partial=True, submitted=True)
                if status in {"canceled", "rejected", "expired", "done_for_day"}:
                    log.info("Order %s for %s ended status=%s while waiting", last_order_id, symbol, status)
                    break
            except Exception as exc:
                log.warning("Error polling order %s for %s: %s", last_order_id, symbol, exc)
            time.sleep(cfg.order_poll_interval_seconds)

    maybe_cancel_live_order(session, cfg, last_order_id, symbol, "chase exit")
    log.warning("Giving up chasing BUY for %s after %.1fs", symbol, time.time() - started)
    return OrderAttemptResult(filled_or_partial=False, submitted=last_order_id is not None)


# -----------------------------
# Main buyer loop
# -----------------------------


def process_cycle(
    session: requests.Session,
    ws: gspread.Worksheet,
    entry_log_ws: Optional[gspread.Worksheet],
    risk_map_ws: gspread.Worksheet,
    cfg: Config,
) -> None:
    set_state(last_cycle_started_at=now_iso(), last_error=None)

    candidates = read_buy_candidates(ws, cfg)
    candidates = ranked_buy_candidates(candidates, cfg)
    log.info(
        "Found %d unique buy candidates with score>=%s rank_candidates_enabled=%s",
        len(candidates),
        cfg.buy_score,
        cfg.rank_candidates_enabled,
    )

    account = get_account(session, cfg)
    position_snapshots = list_positions(session, cfg)
    positions = {p.symbol for p in position_snapshots}
    open_buy_orders = list_open_buy_order_symbols(session, cfg)
    risk = build_risk_snapshot(account, position_snapshots, open_buy_orders, cfg)
    set_risk_state(risk)

    risk_map = read_symbol_risk_map(risk_map_ws)
    concentration = build_concentration_state(position_snapshots, risk_map, entry_log_ws, cfg)
    set_concentration_state(concentration, len(risk_map), risk.equity)
    set_state(last_concentration_gate_block_reason=None)

    log.info(
        "Risk gate mode=%s reasons=%s equity=%.2f cash_pct=%.2f%% exposure_pct=%.2f%% margin_use_pct=%.2f%% drawdown_pct=%.2f%% positions=%d open_buy_orders=%d red/green=%d/%d available_slots=%d max_new_orders_this_cycle=%d order_fraction_multiplier=%.2f",
        risk.mode,
        ";".join(risk.reasons),
        risk.equity,
        risk.cash_pct * 100,
        risk.exposure_pct * 100,
        risk.margin_use_pct * 100,
        risk.drawdown_pct * 100,
        risk.position_count,
        risk.open_buy_order_count,
        risk.red_position_count,
        risk.green_position_count,
        risk.available_position_slots,
        risk.max_new_orders_this_cycle,
        risk.order_fraction_multiplier,
    )
    log.info(
        "Concentration gate enabled=%s risk_map_symbols=%d unknown_policy=%s unknown_positions=%s",
        cfg.concentration_gate_enabled,
        len(risk_map),
        cfg.unknown_classification_policy,
        sorted(concentration.unknown_position_symbols),
    )
    if cfg.concentration_gate_enabled and not risk_map and cfg.unknown_classification_policy == "skip":
        log.warning(
            "Concentration gate is fail-closed and Symbol Risk Map has no complete rows; all candidates will be skipped"
        )

    orders_submitted = 0
    filled_or_partial = 0
    skipped_existing = 0
    skipped_notional = 0
    skipped_recent_failures = 0
    skipped_product_name_block = 0
    skipped_asset_lookup = 0
    skipped_entry_sized_below_min = 0
    skipped_reentry_guard = 0
    skipped_unknown_classification = 0
    concentration_skips: CounterType[str] = Counter()
    entry_log_rows = 0
    order_submit_failures = 0

    def finish_cycle() -> None:
        set_concentration_state(concentration, len(risk_map), risk.equity)
        set_state(
            last_cycle_finished_at=now_iso(),
            last_candidates=len(candidates),
            last_orders_submitted=orders_submitted,
            last_orders_filled_or_partial=filled_or_partial,
            last_skipped_existing=skipped_existing,
            last_skipped_notional=skipped_notional,
            last_skipped_recent_failures=skipped_recent_failures,
            last_skipped_product_name_block=skipped_product_name_block,
            last_skipped_asset_lookup=skipped_asset_lookup,
            last_skipped_sma200_sized_below_min=skipped_entry_sized_below_min,
            last_skipped_entry_sized_below_min=skipped_entry_sized_below_min,
            last_skipped_reentry_guard=skipped_reentry_guard,
            last_skipped_unknown_classification=skipped_unknown_classification,
            last_skipped_sector_daily_limit=concentration_skips["skipped_sector_daily_limit"],
            last_skipped_group_daily_limit=concentration_skips["skipped_group_daily_limit"],
            last_skipped_sector_position_limit=concentration_skips["skipped_sector_position_limit"],
            last_skipped_group_position_limit=concentration_skips["skipped_group_position_limit"],
            last_skipped_sector_exposure=concentration_skips["skipped_sector_exposure"],
            last_skipped_group_exposure=concentration_skips["skipped_group_exposure"],
            last_skipped_sector_daily_notional=concentration_skips["skipped_sector_daily_notional"],
            last_skipped_group_daily_notional=concentration_skips["skipped_group_daily_notional"],
            last_skipped_sector_stress=concentration_skips["skipped_sector_stress"],
            last_skipped_group_stress=concentration_skips["skipped_group_stress"],
            last_entry_log_rows=entry_log_rows,
            last_order_submit_failures=order_submit_failures,
        )

    if risk.max_new_orders_this_cycle <= 0:
        log.warning(
            "Risk gate blocks new buys this cycle mode=%s reasons=%s",
            risk.mode,
            ";".join(risk.reasons),
        )
        finish_cycle()
        return

    if cfg.concentration_gate_enabled and cfg.unknown_classification_policy == "skip":
        concentration_block_reason = ""
        if not risk_map:
            concentration_block_reason = "risk_map_empty"
        elif concentration.unknown_position_symbols:
            concentration_block_reason = (
                "unmapped_open_positions:"
                + ",".join(sorted(concentration.unknown_position_symbols))
            )
        if concentration_block_reason:
            set_state(last_concentration_gate_block_reason=concentration_block_reason)
            log.warning(
                "Concentration gate blocks all new buys until classification is complete reason=%s",
                concentration_block_reason,
            )
            finish_cycle()
            return

    recent_sell_fills = list_recent_sell_fills(session, cfg)
    if recent_sell_fills:
        log.info(
            "Loaded %d recent sell fills for re-entry guard lookback_days=%d",
            len(recent_sell_fills),
            cfg.reentry_lookback_days,
        )

    for candidate in candidates:
        symbol = candidate.symbol
        if _stop_event.is_set():
            break

        if orders_submitted >= risk.max_new_orders_this_cycle:
            log.info(
                "Risk-adjusted max new orders reached orders_submitted=%d limit=%d mode=%s; ending cycle",
                orders_submitted,
                risk.max_new_orders_this_cycle,
                risk.mode,
            )
            break

        rank_score, rank_reason = candidate_quality_score(candidate)

        recent_failure = cooldown_reason(symbol)
        if recent_failure:
            log.info("Skipping %s: recent order failure cooldown reason=%s", symbol, recent_failure)
            skipped_recent_failures += 1
            continue

        if symbol in positions:
            log.info("Skipping %s: already held in Alpaca account", symbol)
            skipped_existing += 1
            continue
        if symbol in open_buy_orders:
            log.info("Skipping %s: open buy order already exists", symbol)
            skipped_existing += 1
            continue

        classification = classification_for_symbol(symbol, risk_map, cfg)
        if classification is None:
            log.info(
                "Skipping %s: no complete symbol/sector/risk_group row in %r and UNKNOWN_CLASSIFICATION_POLICY=%s",
                symbol,
                cfg.risk_map_worksheet_name,
                cfg.unknown_classification_policy,
            )
            skipped_unknown_classification += 1
            continue

        concentration_check = evaluate_concentration(
            classification, concentration, risk.equity, 0.0, cfg
        )
        if not concentration_check.allowed:
            counter_name = concentration_skip_counter_name(concentration_check.reason)
            if counter_name:
                concentration_skips[counter_name] += 1
            log.info(
                "Skipping %s: concentration reason=%s sector=%s group=%s sector_positions=%d group_positions=%d sector_entries_today=%d group_entries_today=%d sector_exposure=%.2f%% group_exposure=%.2f%% sector_stressed=%s group_stressed=%s",
                symbol,
                concentration_check.reason,
                classification.sector,
                classification.risk_group,
                concentration_check.sector_position_count_before,
                concentration_check.group_position_count_before,
                concentration_check.sector_entries_today,
                concentration_check.group_entries_today,
                concentration_check.sector_exposure_pct_before * 100,
                concentration_check.group_exposure_pct_before * 100,
                concentration_check.sector_stressed,
                concentration_check.group_stressed,
            )
            continue

        reentry_allowed, reentry_decision = reentry_guard_decision(
            candidate, recent_sell_fills, risk, cfg
        )
        if not reentry_allowed:
            log.info("Skipping %s: re-entry guard %s", symbol, reentry_decision)
            skipped_reentry_guard += 1
            continue

        try:
            asset = get_asset(session, cfg, symbol)
            name = asset_name(asset)
        except Exception as exc:
            if cfg.skip_when_asset_name_unavailable:
                log.warning("Skipping %s: could not verify asset/product name err=%s", symbol, exc)
                skipped_asset_lookup += 1
                continue
            log.warning(
                "Could not verify asset/product name for %s; allowing buy because SKIP_WHEN_ASSET_NAME_UNAVAILABLE=false err=%s",
                symbol,
                exc,
            )
            name = ""

        if not name and cfg.skip_when_asset_name_unavailable:
            log.warning("Skipping %s: Alpaca asset lookup returned no product name", symbol)
            skipped_asset_lookup += 1
            continue

        block_reason = product_name_block_reason(name, cfg)
        if block_reason:
            log.info("Skipping %s: %s", symbol, block_reason)
            skipped_product_name_block += 1
            continue

        account = get_account(session, cfg)
        bp_field, buying_power = choose_buying_power(account)
        if buying_power <= 0:
            log.warning("No positive buying_power found; stopping this cycle")
            break

        effective_order_fraction = cfg.order_fraction * risk.order_fraction_multiplier
        base_notional = round(buying_power * effective_order_fraction, 2)
        if base_notional < cfg.min_notional:
            log.warning(
                "Skipping %s: computed base_notional %.2f is below MIN_NOTIONAL %.2f; stopping this cycle",
                symbol,
                base_notional,
                cfg.min_notional,
            )
            skipped_notional += 1
            break

        sma200_multiplier, close_vs_sma200_pct, sma200_sizing_reason = sma200_sizing_multiplier(
            candidate, cfg
        )
        sma50_multiplier, close_vs_sma50_pct, sma50_sizing_reason = sma50_sizing_multiplier(
            candidate, cfg
        )
        entry_sizing_multiplier = clamp(sma200_multiplier * sma50_multiplier, 0.01, 1.0)
        entry_sizing_reason = f"{sma200_sizing_reason}|{sma50_sizing_reason}"
        notional = round(base_notional * entry_sizing_multiplier, 2)

        if notional < cfg.min_notional:
            log.warning(
                "Skipping %s: entry-adjusted notional %.2f is below MIN_NOTIONAL %.2f base_notional=%.2f entry_multiplier=%.4f sma200_multiplier=%.4f sma50_multiplier=%.4f close=%.4f sma_200=%.4f sma_50=%.4f",
                symbol,
                notional,
                cfg.min_notional,
                base_notional,
                entry_sizing_multiplier,
                sma200_multiplier,
                sma50_multiplier,
                candidate.close,
                candidate.sma_200,
                candidate.sma_50,
            )
            skipped_entry_sized_below_min += 1
            continue

        current_equity = to_float(account.get("equity"), default=risk.equity)
        concentration_check = evaluate_concentration(
            classification, concentration, current_equity, notional, cfg
        )
        if not concentration_check.allowed:
            counter_name = concentration_skip_counter_name(concentration_check.reason)
            if counter_name:
                concentration_skips[counter_name] += 1
            log.info(
                "Skipping %s after sizing: concentration reason=%s sector=%s group=%s proposed_notional=%.2f sector_exposure_before=%.2f%% group_exposure_before=%.2f%% sector_daily_notional_before=%.2f%% group_daily_notional_before=%.2f%%",
                symbol,
                concentration_check.reason,
                classification.sector,
                classification.risk_group,
                notional,
                concentration_check.sector_exposure_pct_before * 100,
                concentration_check.group_exposure_pct_before * 100,
                concentration_check.sector_daily_notional_pct_before * 100,
                concentration_check.group_daily_notional_pct_before * 100,
            )
            continue

        log.info(
            "Buying candidate %s: score=%.4f rank_score=%.4f risk_mode=%s sector=%s risk_group=%s base_notional=%.2f adjusted_notional=%.2f entry_multiplier=%.4f sma200_multiplier=%.4f sma50_multiplier=%.4f close_vs_sma200=%s close_vs_sma50=%s sizing_reason=%s effective_fraction=%.2f%% buying_power_field=%s buying_power=%.2f close=%.4f sma_200=%.4f sma_50=%.4f reentry=%s concentration=%s",
            symbol,
            candidate.score,
            rank_score,
            risk.mode,
            classification.sector,
            classification.risk_group,
            base_notional,
            notional,
            entry_sizing_multiplier,
            sma200_multiplier,
            sma50_multiplier,
            f"{close_vs_sma200_pct:.2%}" if close_vs_sma200_pct is not None else "n/a",
            f"{close_vs_sma50_pct:.2%}" if close_vs_sma50_pct is not None else "n/a",
            entry_sizing_reason,
            effective_order_fraction * 100,
            bp_field,
            buying_power,
            candidate.close,
            candidate.sma_200,
            candidate.sma_50,
            reentry_decision,
            concentration_check.reason,
        )

        attempt = place_best_bid_chasing_order(session, cfg, symbol, notional)
        if attempt.submitted:
            orders_submitted += 1

        regt_fallback_used = False
        regt_buying_power = to_float(account.get("regt_buying_power"), default=0.0)

        if not attempt.filled_or_partial and should_retry_with_regt(attempt):
            fallback_notional, regt_buying_power = regt_fallback_notional(account, cfg, notional)
            if fallback_notional >= cfg.min_notional and fallback_notional < notional:
                regt_fallback_used = True
                log.info(
                    "Retrying %s with Reg-T fallback notional=%.2f original_adjusted_notional=%.2f base_notional=%.2f regt_buying_power=%.2f fallback_fraction=%.2f",
                    symbol,
                    fallback_notional,
                    notional,
                    base_notional,
                    regt_buying_power,
                    cfg.regt_fallback_fraction,
                )
                notional = fallback_notional
                attempt = place_best_bid_chasing_order(session, cfg, symbol, fallback_notional)
                if attempt.submitted:
                    orders_submitted += 1
            else:
                log.warning(
                    "No usable Reg-T fallback for %s fallback_notional=%.2f original_adjusted_notional=%.2f regt_buying_power=%.2f min_notional=%.2f",
                    symbol,
                    fallback_notional,
                    notional,
                    regt_buying_power,
                    cfg.min_notional,
                )

        final_concentration_check = evaluate_concentration(
            classification, concentration, current_equity, notional, cfg
        )
        if append_entry_log_row(
            entry_log_ws,
            [
                now_iso(),
                symbol,
                name,
                round(candidate.score, 6),
                round(candidate.close, 6),
                round(candidate.sma_200, 6),
                round(candidate.sma_50, 6),
                round(candidate.pos_52w, 6),
                round(candidate.dollar_vol_m, 6),
                round(close_vs_sma200_pct, 6) if close_vs_sma200_pct is not None else "",
                round(close_vs_sma50_pct, 6) if close_vs_sma50_pct is not None else "",
                base_notional,
                round(sma200_multiplier, 6),
                notional,
                bp_field,
                buying_power,
                effective_order_fraction,
                attempt.submitted,
                attempt.filled_or_partial,
                attempt.failure_status if attempt.failure_status is not None else "",
                attempt.failure_message[:500] if attempt.failure_message else "",
                regt_fallback_used,
                regt_buying_power,
                cfg.alpaca_paper,
                round(sma50_multiplier, 6),
                round(entry_sizing_multiplier, 6),
                sma50_sizing_reason,
                entry_sizing_reason,
                round(rank_score, 6),
                rank_reason,
                risk.mode,
                ";".join(risk.reasons),
                risk.position_count,
                risk.available_position_slots,
                reentry_decision,
                APP_VERSION,
                classification.sector,
                classification.risk_group,
                final_concentration_check.sector_position_count_before,
                final_concentration_check.group_position_count_before,
                round(final_concentration_check.sector_exposure_pct_before, 6),
                round(final_concentration_check.group_exposure_pct_before, 6),
                final_concentration_check.sector_entries_today,
                final_concentration_check.group_entries_today,
                round(final_concentration_check.sector_daily_notional_pct_before, 6),
                round(final_concentration_check.group_daily_notional_pct_before, 6),
                final_concentration_check.sector_stressed,
                final_concentration_check.group_stressed,
                final_concentration_check.reason,
            ],
        ):
            entry_log_rows += 1

        if attempt.filled_or_partial:
            filled_or_partial += 1
            positions.add(symbol)
            apply_filled_entry_to_concentration(
                concentration, classification, notional
            )
        else:
            if attempt.failure_message:
                order_submit_failures += 1
                remember_order_failure(symbol, cfg, attempt.failure_message)
                log.warning(
                    "Order failure cooldown set for %s seconds=%.0f status=%s reason=%s",
                    symbol,
                    cfg.order_failure_cooldown_seconds,
                    attempt.failure_status,
                    attempt.failure_message,
                )
            try:
                open_buy_orders = list_open_buy_order_symbols(session, cfg)
            except Exception as exc:
                log.warning(
                    "Could not refresh open buy orders after failed attempt for %s: %s",
                    symbol,
                    exc,
                )

    finish_cycle()
    log.info(
        "Cycle complete risk_mode=%s candidates=%d orders_submitted=%d filled_or_partial=%d skipped_existing=%d skipped_notional=%d skipped_recent_failures=%d skipped_reentry_guard=%d skipped_unknown_classification=%d concentration_skips=%s skipped_product_name_block=%d skipped_asset_lookup=%d skipped_entry_sized_below_min=%d entry_log_rows=%d order_submit_failures=%d",
        risk.mode,
        len(candidates),
        orders_submitted,
        filled_or_partial,
        skipped_existing,
        skipped_notional,
        skipped_recent_failures,
        skipped_reentry_guard,
        skipped_unknown_classification,
        dict(concentration_skips),
        skipped_product_name_block,
        skipped_asset_lookup,
        skipped_entry_sized_below_min,
        entry_log_rows,
        order_submit_failures,
    )


def buyer_loop() -> None:
    cfg = load_config()
    set_state(started_at=now_iso())
    log.info(
        "Buyer service started version=%s sheet_id=%s worksheet=%s range=%s buy_score=%s order_fraction=%.4f paper=%s primary_buying_power_field=buying_power regt_fallback_fraction=%.2f steps=%s cycle_sleep=%.1f extended_hours=%s order_failure_cooldown=%.0f blocked_product_name_pattern=%r skip_when_asset_name_unavailable=%s sma200_sizing_enabled=%s sma200_min_multiplier=%.2f sma200_max_reduction_distance=%.2f sma50_sizing_enabled=%s sma50_full_size_distance=%.2f sma50_max_reduction_distance=%.2f sma50_min_multiplier=%.2f risk_gate_enabled=%s max_total_positions=%d red_exposure_pct=%.2f red_min_cash_pct=%.2f red_margin_use_pct=%.2f yellow_max_orders_per_cycle=%d yellow_order_fraction_multiplier=%.2f rank_candidates_enabled=%s reentry_guard_enabled=%s reentry_lookback_days=%d reentry_min_discount_pct=%.2f concentration_gate_enabled=%s risk_map_worksheet=%r unknown_policy=%s max_new_sector_day=%d max_new_group_day=%d max_open_sector=%d max_open_group=%d max_sector_exposure=%.2f max_group_exposure=%.2f stress_freeze_enabled=%s entry_log_worksheet=%r",
        APP_VERSION,
        cfg.google_sheet_id,
        cfg.google_worksheet_name,
        cfg.screener_range,
        cfg.buy_score,
        cfg.order_fraction,
        cfg.alpaca_paper,
        cfg.regt_fallback_fraction,
        cfg.bid_to_market_steps,
        cfg.cycle_sleep_seconds,
        cfg.extended_hours,
        cfg.order_failure_cooldown_seconds,
        cfg.blocked_product_name_pattern,
        cfg.skip_when_asset_name_unavailable,
        cfg.sma200_sizing_enabled,
        cfg.sma200_sizing_min_multiplier,
        cfg.sma200_sizing_max_reduction_distance,
        cfg.sma50_sizing_enabled,
        cfg.sma50_sizing_full_size_distance,
        cfg.sma50_sizing_max_reduction_distance,
        cfg.sma50_sizing_min_multiplier,
        cfg.risk_gate_enabled,
        cfg.max_total_positions,
        cfg.red_exposure_pct,
        cfg.red_min_cash_pct,
        cfg.red_margin_use_pct,
        cfg.yellow_max_orders_per_cycle,
        cfg.yellow_order_fraction_multiplier,
        cfg.rank_candidates_enabled,
        cfg.reentry_guard_enabled,
        cfg.reentry_lookback_days,
        cfg.reentry_min_discount_pct,
        cfg.concentration_gate_enabled,
        cfg.risk_map_worksheet_name,
        cfg.unknown_classification_policy,
        cfg.max_new_per_sector_per_day,
        cfg.max_new_per_risk_group_per_day,
        cfg.max_open_positions_per_sector,
        cfg.max_open_positions_per_risk_group,
        cfg.max_sector_exposure_pct,
        cfg.max_risk_group_exposure_pct,
        cfg.stress_freeze_enabled,
        cfg.entry_log_worksheet_name,
    )

    gc: Optional[gspread.Client] = None
    ws: Optional[gspread.Worksheet] = None
    entry_log_ws: Optional[gspread.Worksheet] = None
    risk_map_ws: Optional[gspread.Worksheet] = None
    session = requests.Session()

    while not _stop_event.is_set():
        try:
            if gc is None:
                gc = create_gspread_client(cfg)
            if ws is None:
                ws = open_worksheet(gc, cfg)
            if entry_log_ws is None:
                entry_log_ws = open_or_create_worksheet(
                    gc, cfg, cfg.entry_log_worksheet_name, ENTRY_LOG_HEADERS
                )
            if risk_map_ws is None:
                risk_map_ws = open_or_create_worksheet(
                    gc, cfg, cfg.risk_map_worksheet_name, RISK_MAP_HEADERS
                )
            process_cycle(session, ws, entry_log_ws, risk_map_ws, cfg)
        except Exception as exc:
            log.exception("Buyer loop error: %s", exc)
            set_state(last_error=str(exc))
            gc = None
            ws = None
            entry_log_ws = None
            risk_map_ws = None
            time.sleep(cfg.error_sleep_seconds)

        time.sleep(cfg.cycle_sleep_seconds)


@app.on_event("startup")
def on_startup() -> None:
    global _worker_thread
    _worker_thread = threading.Thread(target=buyer_loop, name="buyer-loop", daemon=True)
    _worker_thread.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    _stop_event.set()
    if _worker_thread and _worker_thread.is_alive():
        _worker_thread.join(timeout=5)


@app.get("/")
def root() -> Dict[str, str]:
    return {"status": "ok", "service": "alpaca-score-buyer", "app_version": APP_VERSION}


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    with _state_lock:
        result = dict(_state)
    result["app_version"] = APP_VERSION
    return result