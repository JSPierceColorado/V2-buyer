import json
import logging
import math
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import gspread
import requests
from fastapi import FastAPI
from google.oauth2.service_account import Credentials


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
    start_row: int

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

    return Config(
        google_service_account_json=google_service_account_json,
        google_sheet_id=google_sheet_id,
        google_worksheet_name=google_worksheet_name,
        screener_range=os.getenv("SCREENER_RANGE", "A:G").strip(),
        symbol_col_index=getenv_int("SYMBOL_COL_INDEX", 1),
        score_col_index=getenv_int("SCORE_COL_INDEX", 7),
        start_row=getenv_int("START_ROW", 2),
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
    "last_order_submit_failures": 0,
}
_order_failure_cooldowns: Dict[str, Tuple[float, str]] = {}


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


def read_buy_candidates(ws: gspread.Worksheet, cfg: Config) -> List[str]:
    """Read screener rows and return ordered unique symbols where score column equals BUY_SCORE."""
    values = ws.get(cfg.screener_range) or []
    candidates: List[str] = []
    seen: Set[str] = set()

    symbol_idx = cfg.symbol_col_index - 1
    score_idx = cfg.score_col_index - 1

    for row_num, row in enumerate(values, start=1):
        if row_num < cfg.start_row:
            continue
        if len(row) <= max(symbol_idx, score_idx):
            continue
        symbol = clean_symbol(row[symbol_idx])
        if not symbol or symbol == "SYMBOL":
            continue
        if not score_matches(row[score_idx], cfg.buy_score):
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        candidates.append(symbol)

    return candidates


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


def list_position_symbols(session: requests.Session, cfg: Config) -> Set[str]:
    try:
        positions = http_get(session, f"{cfg.trading_base_url}/v2/positions", cfg)
    except requests.HTTPError as exc:
        # Alpaca returns an empty list when no positions in normal operation; this is just defensive.
        log.warning("Could not list positions: %s", exc)
        return set()
    if not isinstance(positions, list):
        return set()
    return {clean_symbol(p.get("symbol")) for p in positions if clean_symbol(p.get("symbol"))}


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


def process_cycle(session: requests.Session, ws: gspread.Worksheet, cfg: Config) -> None:
    set_state(last_cycle_started_at=now_iso(), last_error=None)

    candidates = read_buy_candidates(ws, cfg)
    log.info("Found %d unique buy candidates with score=%s", len(candidates), cfg.buy_score)

    positions = list_position_symbols(session, cfg)
    open_buy_orders = list_open_buy_order_symbols(session, cfg)
    log.info("Existing positions=%d open_buy_orders=%d", len(positions), len(open_buy_orders))

    orders_submitted = 0
    filled_or_partial = 0
    skipped_existing = 0
    skipped_notional = 0
    skipped_recent_failures = 0
    order_submit_failures = 0

    for symbol in candidates:
        if _stop_event.is_set():
            break

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

        account = get_account(session, cfg)
        bp_field, buying_power = choose_buying_power(account)
        if buying_power <= 0:
            log.warning("No positive buying_power found; stopping this cycle")
            break

        notional = round(buying_power * cfg.order_fraction, 2)
        if notional < cfg.min_notional:
            log.warning(
                "Skipping %s: computed notional %.2f is below MIN_NOTIONAL %.2f; stopping this cycle",
                symbol,
                notional,
                cfg.min_notional,
            )
            skipped_notional += 1
            break

        log.info(
            "Buying candidate %s: score=%s notional=%.2f fraction=%.2f%% buying_power_field=%s buying_power=%.2f",
            symbol,
            cfg.buy_score,
            notional,
            cfg.order_fraction * 100,
            bp_field,
            buying_power,
        )

        attempt = place_best_bid_chasing_order(session, cfg, symbol, notional)
        if attempt.submitted:
            orders_submitted += 1

        if not attempt.filled_or_partial and should_retry_with_regt(attempt):
            fallback_notional, regt_buying_power = regt_fallback_notional(account, cfg, notional)
            if fallback_notional >= cfg.min_notional and fallback_notional < notional:
                log.info(
                    "Retrying %s with Reg-T fallback notional=%.2f original_notional=%.2f regt_buying_power=%.2f fallback_fraction=%.2f",
                    symbol,
                    fallback_notional,
                    notional,
                    regt_buying_power,
                    cfg.regt_fallback_fraction,
                )
                attempt = place_best_bid_chasing_order(session, cfg, symbol, fallback_notional)
                if attempt.submitted:
                    orders_submitted += 1
            else:
                log.warning(
                    "No usable Reg-T fallback for %s fallback_notional=%.2f original_notional=%.2f regt_buying_power=%.2f min_notional=%.2f",
                    symbol,
                    fallback_notional,
                    notional,
                    regt_buying_power,
                    cfg.min_notional,
                )

        if attempt.filled_or_partial:
            filled_or_partial += 1
            # Keep in-memory duplicate protection current inside this same cycle.
            positions.add(symbol)
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
            # Refresh open buy orders in case an unknown outcome left one open.
            try:
                open_buy_orders = list_open_buy_order_symbols(session, cfg)
            except Exception as exc:
                log.warning("Could not refresh open buy orders after failed attempt for %s: %s", symbol, exc)

        if cfg.max_orders_per_cycle > 0 and orders_submitted >= cfg.max_orders_per_cycle:
            log.info("MAX_ORDERS_PER_CYCLE=%d reached; ending cycle", cfg.max_orders_per_cycle)
            break

    set_state(
        last_cycle_finished_at=now_iso(),
        last_candidates=len(candidates),
        last_orders_submitted=orders_submitted,
        last_orders_filled_or_partial=filled_or_partial,
        last_skipped_existing=skipped_existing,
        last_skipped_notional=skipped_notional,
        last_skipped_recent_failures=skipped_recent_failures,
        last_order_submit_failures=order_submit_failures,
    )
    log.info(
        "Cycle complete candidates=%d orders_submitted=%d filled_or_partial=%d skipped_existing=%d skipped_notional=%d skipped_recent_failures=%d order_submit_failures=%d",
        len(candidates),
        orders_submitted,
        filled_or_partial,
        skipped_existing,
        skipped_notional,
        skipped_recent_failures,
        order_submit_failures,
    )


def buyer_loop() -> None:
    cfg = load_config()
    set_state(started_at=now_iso())
    log.info(
        "Buyer service started sheet_id=%s worksheet=%s range=%s buy_score=%s order_fraction=%.4f paper=%s primary_buying_power_field=buying_power regt_fallback_fraction=%.2f steps=%s cycle_sleep=%.1f extended_hours=%s order_failure_cooldown=%.0f",
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
    )

    gc: Optional[gspread.Client] = None
    ws: Optional[gspread.Worksheet] = None
    session = requests.Session()

    while not _stop_event.is_set():
        try:
            if gc is None:
                gc = create_gspread_client(cfg)
            if ws is None:
                ws = open_worksheet(gc, cfg)
            process_cycle(session, ws, cfg)
        except Exception as exc:
            log.exception("Buyer loop error: %s", exc)
            set_state(last_error=str(exc))
            # Force re-open Google resources next time; helps if token/sheet handle gets stale.
            gc = None
            ws = None
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
    return {"status": "ok", "service": "alpaca-score-buyer"}


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    with _state_lock:
        return dict(_state)
