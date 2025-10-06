#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
replay_min.py
模擬開盤重播 1 分鐘 K CSV，符合 ORB（開盤區間突破）條件時發送 ntfy 通知。
"""

import time
import argparse
import requests
import pandas as pd
import numpy as np
from datetime import timedelta


def vwap(df):
    """計算 VWAP（Volume Weighted Average Price）"""
    tp = (df["High"] + df["Low"] + df["Close"]) / 3.0
    cv = df["Volume"].cumsum().replace(0, np.nan)
    return (tp * df["Volume"]).cumsum() / cv


def ntfy(topic, title, msg):
    """發送 ntfy 推播"""
    try:
        requests.post(
            f"https://ntfy.sh/{topic}",
            data=f"{title}\n{msg}".encode("utf-8"),
            timeout=5,
        )
        print(f"🔔 {title} | {msg}")
    except Exception as e:
        print(f"[WARN] ntfy 失敗: {e}")


def main():
    ap = argparse.ArgumentParser(
        description="重播 1m CSV 模擬開盤，符合 ORB 條件即推 ntfy 通知"
    )
    ap.add_argument("--csv", required=True, help="CSV 檔路徑 (含 Datetime,OHLCV 欄位)")
    ap.add_argument("--symbol", default="SYMBOL", help="股票代號顯示名稱")
    ap.add_argument("--open-mins", type=int, default=15, help="開盤區間分鐘數")
    ap.add_argument("--vol-factor", type=float, default=1.0, help="量能門檻 (成交量 >= MA10*factor)")
    ap.add_argument("--speed", type=float, default=0.5, help="每分鐘K播放間隔秒數")
    ap.add_argument("--ntfy-topic", required=True, help="ntfy 主題，例如 Chailease")
    args = ap.parse_args()

    df = pd.read_csv(args.csv, parse_dates=["Datetime"]).rename(columns=str.title)
    df = df.set_index("Datetime")
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col not in df.columns:
            raise SystemExit(f"CSV 缺少欄位: {col}")

    df["VWAP"] = vwap(df)
    df["VolMA10"] = df["Volume"].rolling(10, min_periods=1).mean()

    first = df.index[0]
    win_end = first + timedelta(minutes=args.open_mins)
    win = df[(df.index >= first) & (df.index < win_end)]
    if win.empty:
        raise SystemExit("⚠️ 開盤區間資料為空，請檢查 CSV 時間與 --open-mins")
    orh, orl = float(win["High"].max()), float(win["Low"].min())

    print(
        f"▶ Replaying {args.symbol} | ORB={args.open_mins}m | speed={args.speed}s/min | ORH={orh:.2f} ORL={orl:.2f}"
    )

    lastL, lastS = None, None
    cur = first

    while cur <= df.index[-1]:
        sub = df[df.index <= cur]
        if not sub.empty:
            row = sub.iloc[-1]
            price = float(row["Close"])
            vol = float(row["Volume"])
            volma = float(row["VolMA10"])
            vwapv = float(row["VWAP"])
            bar_id = str(row.name.floor("min"))

            long_ok = (
                (price > orh)
                and (volma > 0)
                and (vol >= volma * args.vol_factor)
                and (price >= vwapv)
            )
            short_ok = (
                (price < orl)
                and (volma > 0)
                and (vol >= volma * args.vol_factor)
                and (price <= vwapv)
            )

            if long_ok and bar_id != lastL:
                ntfy(
                    args.ntfy_topic,
                    "ORB 多頭突破",
                    f"{args.symbol} LONG @ {price:.2f} | {bar_id} | ORH={orh:.2f} ORL={orl:.2f} VWAP={vwapv:.2f}",
                )
                lastL = bar_id

            if short_ok and bar_id != lastS:
                ntfy(
                    args.ntfy_topic,
                    "ORB 空頭跌破",
                    f"{args.symbol} SHORT @ {price:.2f} | {bar_id} | ORH={orh:.2f} ORL={orl:.2f} VWAP={vwapv:.2f}",
                )
                lastS = bar_id

        cur += timedelta(minutes=1)
        time.sleep(max(0.05, args.speed))


if __name__ == "__main__":
    main()
