import time
import argparse
import pandas as pd
import numpy as np
import yfinance as yf
import pytz
import requests
from datetime import timedelta

def ntfy_send(topic: str, title: str, msg: str):
    url = f"https://ntfy.sh/{topic}"
    data = f"{title}\n{msg}".encode("utf-8")
    try:
        r = requests.post(url, data=data, timeout=5)
        if r.status_code >= 300:
            print(f"[WARN] ntfy 回應碼：{r.status_code}")
    except Exception as e:
        print(f"[WARN] ntfy 失敗：{e}")

def compute_vwap(df: pd.DataFrame) -> pd.Series:
    tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
    cum_vol = df["Volume"].cumsum().replace(0, np.nan)
    return (tp * df["Volume"]).cumsum() / cum_vol

def fetch_multi_1m(symbols):
    df = yf.download(
        tickers=" ".join(symbols),
        period="1d",
        interval="1m",
        prepost=False,
        progress=False,
        group_by="ticker",
        threads=True,
    )
    out = {}
    if isinstance(df.columns, pd.MultiIndex):
        for sym in symbols:
            try:
                dfi = df[sym].copy()
            except KeyError:
                continue
            dfi = dfi.rename(columns=str.title).dropna(how="all")
            if not dfi.empty:
                out[sym] = dfi
    else:
        dfi = df.rename(columns=str.title).dropna(how="all")
        if not dfi.empty and len(symbols) == 1:
            out[symbols[0]] = dfi
    return out

def opening_window(df: pd.DataFrame, open_mins: int):
    first_ts = df.index[0]
    win_end = first_ts + timedelta(minutes=open_mins)
    mask = (df.index >= first_ts) & (df.index < win_end)
    return df.loc[mask]

def fmt_alert(sym, side, ts, price, orh, orl, vwap):
    return f"{sym} {side.upper()} @ {price:.2f} | {ts} | ORH={orh:.2f} ORL={orl:.2f} VWAP={vwap:.2f}"

def main():
    triggered_long=set();triggered_short=set()
    triggered_long=set();triggered_short=set()
    triggered_long=set();triggered_short=set()
    ap = argparse.ArgumentParser(description="ORB 多標的掃描（ntfy 通知版；yfinance 延遲資料）")
    ap.add_argument("--symbols", type=str, help="逗號分隔代號：2330.TW,2317.TW,0050.TW")
    ap.add_argument("--symbols-file", type=str, help="每行一檔的清單檔")
    ap.add_argument("--open-mins", type=int, default=15)
    ap.add_argument("--poll-secs", type=int, default=30)
    ap.add_argument("--timezone", type=str, default="Asia/Taipei")
    ap.add_argument("--vol-factor", type=float, default=1.5)
    ap.add_argument("--notify", type=str, default="ntfy", choices=["ntfy","print"])
    ap.add_argument("--ntfy-topic", type=str, default=None, help="ntfy topic，例如 Chailease 對應 https://ntfy.sh/Chailease")
    ap.add_argument("--once-per-bar", action="store_true", default=True)
    args = ap.parse_args()

    symbols = []
    if args.symbols:
        symbols += [s.strip() for s in args.symbols.split(",") if s.strip()]
    if args.symbols_file:
        with open(args.symbols_file, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s:
                    symbols.append(s)
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        raise SystemExit("請用 --symbols 或 --symbols-file 指定標的清單")

    if args.notify == "ntfy" and not args.ntfy_topic:
        raise SystemExit("請提供 --ntfy-topic，例如 Chailease（https://ntfy.sh/Chailease）")

    tz = pytz.timezone(args.timezone)
    last_alert_bar_long = {s: None for s in symbols}
    last_alert_bar_short = {s: None for s in symbols}

    state = {s: {"orh": None, "orl": None, "win_end": None} for s in symbols}

    print(f"Start ORB multi (ntfy) | N={len(symbols)} | ORB={args.open_mins}m | tz={args.timezone} | volF={args.vol_factor}")
    print("Symbols:", ", ".join(symbols))

    while True:
        try:
            data_map = fetch_multi_1m(symbols)
            if not data_map:
                time.sleep(args.poll_secs)
                continue

            for sym, df in data_map.items():
                # tz
                if df.index.tz is None:
                    df.index = df.index.tz_localize(tz)
                else:
                    df.index = df.index.tz_convert(tz)

                df["VWAP"] = compute_vwap(df)
                df["VolMA10"] = df["Volume"].rolling(10, min_periods=1).mean()

                st = state[sym]
                if st["win_end"] is None:
                    win = opening_window(df, args.open_mins)
                    if len(win) >= max(1, args.open_mins // 2):
                        st["win_end"] = win.index[-1]
                        st["orh"] = float(win["High"].max())
                        st["orl"] = float(win["Low"].min())
                    else:
                        continue

                after = df[df.index >= st["win_end"]]
                if after.empty:
                    continue

                last = after.iloc[-1]
                price = float(last["Close"])
                vwap = float(last["VWAP"])
                vol = float(last["Volume"] or 0.0)
                volma = float(last["VolMA10"] or 0.0)
                orh, orl = st["orh"], st["orl"]

                bar_id = str(after.index[-1].floor("T"))

                long_break = (price > orh) and (volma > 0 and vol >= volma * args.vol_factor) and (price >= vwap)
                short_break = (price < orl) and (volma > 0 and vol >= volma * args.vol_factor) and (price <= vwap)

                if long_break and (not args.once_per_bar or last_alert_bar_long[sym] != bar_id):
                    msg = fmt_alert(sym, "long", after.index[-1], price, orh, orl, vwap)
                    if args.notify == "ntfy":
                        ntfy_send(args.ntfy_topic, "ORB 多頭突破", msg)
                    else:
                        print("[ORB 多頭突破]", msg)
                    last_alert_bar_long[sym] = bar_id

                if short_break and (not args.once_per_bar or last_alert_bar_short[sym] != bar_id):
                    msg = fmt_alert(sym, "short", after.index[-1], price, orh, orl, vwap)
                    if args.notify == "ntfy":
                        ntfy_send(args.ntfy_topic, "ORB 空頭跌破", msg)
                    else:
                        print("[ORB 空頭跌破]", msg)
                    last_alert_bar_short[sym] = bar_id

            time.sleep(args.poll_secs)

        except KeyboardInterrupt:
            print("Stopped by user.")
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(args.poll_secs)

if __name__ == "__main__":
    main()