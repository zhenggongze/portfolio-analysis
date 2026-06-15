#!/usr/bin/env python3
"""
自动化指数估值数据采集与报告推送系统
数据源：蛋卷基金API（主） + ETF.run（辅助） + 静态兜底数据
"""

import requests
import json
import logging
import os
import sys
import time
import random
import argparse
import pandas as pd
from datetime import datetime, timezone, timedelta

# ============================================================
# 配置
# ============================================================

PUSHDEER_KEY = "PDU41552TCTtotgq3EC5AvTOaXpiZG0eMTR6VAl8v"
PUSHDEER_URL = "https://api2.pushdeer.com/message/push"

BEIJING_TZ = timezone(timedelta(hours=8))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# 蛋卷基金代码映射
DANJUAN_CODE_MAP = {
    "000300": "SH000300",
    "000905": "SH000905",
    "399006": "SZ399006",
    "399989": "SZ399989",
    "399997": "SZ399997",
    "000993": "SH000993",
    "159995": "OF159995",
    "515880": "OF515880",
    "NDX": "NDX",
    "H30533": "CSIH30533",
}

# ETF.run URL 映射（仅A股指数）
ETF_RUN_URLS = {
    "000300": "https://www.etf.run/index/SH/000300",
    "000905": "https://www.etf.run/index/SH/000905",
    "399006": "https://www.etf.run/index/SZ/399006",
    "399989": "https://www.etf.run/index/SZ/399989",
    "399997": "https://www.etf.run/index/SZ/399997",
    "000993": "https://www.etf.run/index/SH/000993",
    "159995": "https://www.etf.run/etf/SZ/159995",
    "515880": "https://www.etf.run/etf/SH/515880",
}

# 指数配置列表
INDEX_CONFIG = [
    {"code": "000300", "name": "沪深300", "category": "A股"},
    {"code": "000905", "name": "中证500", "category": "A股"},
    {"code": "399006", "name": "创业板", "category": "A股"},
    {"code": "399989", "name": "中证医疗", "category": "A股"},
    {"code": "399997", "name": "中证白酒", "category": "A股"},
    {"code": "000993", "name": "中证全指信息技术", "category": "A股"},
    {"code": "159995", "name": "芯片ETF", "category": "A股"},
    {"code": "515880", "name": "通信ETF", "category": "A股"},
    {"code": "NDX", "name": "纳斯达克100", "category": "其他"},
    {"code": "H30533", "name": "中概互联50", "category": "其他"},
]

# 静态兜底数据
FALLBACK_DATA = {
    "000300": {"pe": 12.50, "pe_pct": 45.20, "pb": 1.35, "pb_pct": 18.50,
               "low_pe": 8.03, "low_pe_date": "2014-05-19", "low_pe_diff": 35.8,
               "low_pb": 1.04, "low_pb_date": "2014-05-19", "low_pb_diff": 23.0},
    "000905": {"pe": 23.80, "pe_pct": 35.60, "pb": 1.85, "pb_pct": 22.30,
               "low_pe": 16.19, "low_pe_date": "2018-10-18", "low_pe_diff": 32.0,
               "low_pb": 1.31, "low_pb_date": "2018-10-18", "low_pb_diff": 29.2},
    "399006": {"pe": 35.20, "pe_pct": 28.40, "pb": 4.50, "pb_pct": 25.10,
               "low_pe": 27.25, "low_pe_date": "2018-10-18", "low_pe_diff": 22.6,
               "low_pb": 2.78, "low_pb_date": "2018-10-18", "low_pb_diff": 38.2},
    "399989": {"pe": 42.50, "pe_pct": 55.30, "pb": 5.80, "pb_pct": 38.60,
               "low_pe": 30.12, "low_pe_date": "2022-09-26", "low_pe_diff": 29.1,
               "low_pb": 3.65, "low_pb_date": "2022-09-26", "low_pb_diff": 37.1},
    "399997": {"pe": 28.60, "pe_pct": 62.10, "pb": 7.20, "pb_pct": 75.40,
               "low_pe": 18.35, "low_pe_date": "2018-10-29", "low_pe_diff": 35.8,
               "low_pb": 3.82, "low_pb_date": "2018-10-29", "low_pb_diff": 46.9},
    "000993": {"pe": 45.30, "pe_pct": 48.70, "pb": 3.90, "pb_pct": 42.10,
               "low_pe": 28.40, "low_pe_date": "2018-10-18", "low_pe_diff": 37.3,
               "low_pb": 2.30, "low_pb_date": "2018-10-18", "low_pb_diff": 41.0},
    "159995": {"pe": 75.50, "pe_pct": 65.30, "pb": 5.20, "pb_pct": 60.50,
               "low_pe": 38.20, "low_pe_date": "2022-04-26", "low_pe_diff": 49.4,
               "low_pb": 3.20, "low_pb_date": "2022-04-26", "low_pb_diff": 38.5},
    "515880": {"pe": 42.60, "pe_pct": 58.80, "pb": 3.80, "pb_pct": 55.30,
               "low_pe": 25.10, "low_pe_date": "2018-10-18", "low_pe_diff": 41.1,
               "low_pb": 2.30, "low_pb_date": "2018-10-18", "low_pb_diff": 39.5},
    "NDX": {"pe": 34.39, "pe_pct": 68.36, "pb": 9.92, "pb_pct": 94.34,
            "low_pe": 15.28, "low_pe_date": "2008-11-20", "low_pe_diff": 55.6,
            "low_pb": 2.15, "low_pb_date": "2008-11-20", "low_pb_diff": 78.3},
    "H30533": {"pe": 16.79, "pe_pct": 2.72, "pb": 2.28, "pb_pct": 4.86,
               "low_pe": 10.50, "low_pe_date": "2022-10-31", "low_pe_diff": 37.5,
               "low_pb": 1.42, "low_pb_date": "2022-10-31", "low_pb_diff": 37.7},
}

# 近10年时间跨度（毫秒）- 蛋卷API的ts是毫秒级时间戳
MS_PER_DAY = 86400 * 1000
TEN_YEARS_MS = 365 * 10 * MS_PER_DAY


# ============================================================
# 日志设置
# ============================================================

def setup_logging(date_str):
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_file = os.path.join(LOGS_DIR, f"valuation_{date_str}.log")

    logger = logging.getLogger("valuation")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ============================================================
# Node1-3：数据获取
# ============================================================

def fetch_danjuan_pe_pb(danjuan_code, logger):
    """从蛋卷基金API获取PE/PB历史数据"""
    result = {"pe_history": [], "pb_history": [], "error": None}

    for data_type, field_name in [("pe", "index_eva_pe_growths"), ("pb", "index_eva_pb_growths")]:
        url = f"https://danjuanfunds.com/djapi/index_eva/{data_type}_history/{danjuan_code}?day=all"
        try:
            logger.debug(f"请求蛋卷API: {url}")
            resp = requests.get(url, timeout=15,
                                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data", {}).get(field_name, [])
            history = []
            for item in items:
                ts = item.get("ts", 0)
                val = item.get(data_type, 0)
                if ts and val:
                    history.append({"ts": ts, "value": val})
            history.sort(key=lambda x: x["ts"])
            key = "pe_history" if data_type == "pe" else "pb_history"
            result[key] = history
            logger.info(f"蛋卷 {danjuan_code} {data_type.upper()} 获取成功, 共 {len(history)} 条记录")
        except Exception as e:
            logger.warning(f"蛋卷 {danjuan_code} {data_type.upper()} 获取失败: {e}")
            result["error"] = str(e)

    return result


def calc_percentile(history, current_value):
    """计算当前值在历史数据中的百分位：低于当前值的个数 / 总个数 × 100%"""
    if not history or current_value is None:
        return None
    lower_count = sum(1 for item in history if item["value"] < current_value)
    total = len(history)
    pct = (lower_count / total) * 100
    return round(pct, 2)


def filter_recent_years(history, years_ms):
    """过滤出近N年的数据 - ts为毫秒级时间戳"""
    if not history:
        return []
    latest_ts = max(item["ts"] for item in history)
    cutoff_ts = latest_ts - years_ms
    return [item for item in history if item["ts"] >= cutoff_ts]


def find_min_value_with_date(history):
    """从全量历史数据中找到最低值及其对应日期 - ts为毫秒级时间戳"""
    if not history:
        return None, None
    min_item = min(history, key=lambda x: x["value"])
    min_value = round(min_item["value"], 2)
    min_date = datetime.fromtimestamp(min_item["ts"] / 1000).strftime("%Y-%m-%d")
    return min_value, min_date


def fetch_etf_run_data(index_code, logger):
    """从ETF.run获取等权PE/PB辅助数据"""
    url = ETF_RUN_URLS.get(index_code)
    if not url:
        return None

    try:
        logger.debug(f"请求ETF.run: {url}")
        resp = requests.get(url, timeout=15,
                            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                                     "Accept-Encoding": "gzip, deflate, br"})

        if resp.status_code >= 500:
            logger.warning(f"ETF.run {index_code} 服务端错误 (HTTP {resp.status_code})，网站可能暂时不可用")
            return None
        if resp.status_code >= 400:
            logger.warning(f"ETF.run {index_code} 请求失败 (HTTP {resp.status_code})")
            return None

        content = resp.content

        if content[:2] == b'\xce\xb2' or len(content) < 100:
            try:
                import brotli
                content = brotli.decompress(content)
                logger.debug(f"ETF.run {index_code} brotli解压成功")
            except ImportError:
                logger.debug("brotli 库未安装，无法解压")
                return None
            except Exception as e:
                logger.debug(f"ETF.run {index_code} brotli解压失败: {e}")
                return None

        html = content.decode("utf-8", errors="ignore")

        # 查找 compressedIndexDaily JSON
        marker = "compressedIndexDaily"
        start = html.find(marker)
        if start == -1:
            logger.warning(f"ETF.run {index_code} 未找到 compressedIndexDaily")
            return None

        # 提取JSON字符串
        json_start = html.find('{', start)
        if json_start == -1:
            return None

        brace_count = 0
        json_end = json_start
        for i in range(json_start, len(html)):
            if html[i] == '{':
                brace_count += 1
            elif html[i] == '}':
                brace_count -= 1
                if brace_count == 0:
                    json_end = i + 1
                    break

        json_str = html[json_start:json_end]
        data = json.loads(json_str)

        field_names = data.get("fieldNames", [])
        values = data.get("values", [])

        if not field_names or not values or not values[0]:
            return None

        latest = values[0]
        result = {}
        for i, name in enumerate(field_names):
            if i < len(latest):
                result[name] = latest[i]

        output = {}
        if "equalWeightedPeTtm" in result:
            output["ew_pe"] = result["equalWeightedPeTtm"]
        if "equalWeightedPbTtm" in result:
            output["ew_pb"] = result["equalWeightedPbTtm"]
        if "year10PePercentile" in result:
            raw = result["year10PePercentile"]
            output["ew_pe_pct"] = round(raw * 100, 2) if raw is not None else None
        if "year10PbPercentile" in result:
            raw = result["year10PbPercentile"]
            output["ew_pb_pct"] = round(raw * 100, 2) if raw is not None else None

        if output:
            logger.info(f"ETF.run {index_code} 等权数据获取成功")
        return output if output else None

    except Exception as e:
        logger.warning(f"ETF.run {index_code} 获取失败: {e}")
        return None


# ============================================================
# Node3.5：雪球数据源（蛋卷不覆盖的指数用）
# ============================================================

def fetch_xueqiu_pe_pb(index_code, logger):
    """从雪球获取指数/ETF的PE/PB历史数据"""
    result = {"pe_history": [], "pb_history": [], "error": None}

    # ETF/股票代码判断交易所前缀
    if index_code.startswith(("5", "6")):
        xq_prefix = "SH"
    else:
        xq_prefix = "SZ"

    try:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        s.get("https://xueqiu.com/", timeout=10)

        for data_type, indicator, field_name in [("pe", "pe_ttm", "pe_history"), ("pb", "pb", "pb_history")]:
            url = f"https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol={xq_prefix}{index_code}&begin=0&period=week&type=before&count=-500&indicator={indicator}"
            try:
                resp = s.get(url, timeout=15)
                data = resp.json()
                items = data.get("data", {}).get("item", [])
                history = []
                for item in items:
                    if isinstance(item, list) and len(item) >= 10:
                        ts = item[0]
                        val = item[-1]
                        if ts and val and val != 0 and float(val) > 0:
                            history.append({"ts": ts, "value": float(val)})
                history.sort(key=lambda x: x["ts"])
                result[field_name] = history
                logger.info(f"雪球 {index_code} {data_type.upper()} K线: {len(history)} 条")
            except Exception as e:
                logger.warning(f"雪球 {index_code} {data_type.upper()} K线失败: {e}")

        # K线没数据时，尝试报价API获取当前PE/PB
        if not result["pe_history"]:
            try:
                url = f"https://stock.xueqiu.com/v5/stock/quote.json?symbol={xq_prefix}{index_code}&extend=detail"
                resp = s.get(url, timeout=10)
                data = resp.json()
                quote = data.get("data", {}).get("quote", {})
                pe = quote.get("pe_ttm") or quote.get("ttm_pe")
                pb = quote.get("pb") or quote.get("pb_lf")
                ts = int(time.time() * 1000)
                if pe:
                    result["pe_history"] = [{"ts": ts, "value": float(pe)}]
                    logger.info(f"雪球 {index_code} PE(报价): {pe}")
                if pb:
                    result["pb_history"] = [{"ts": ts, "value": float(pb)}]
                    logger.info(f"雪球 {index_code} PB(报价): {pb}")
            except Exception as e:
                logger.warning(f"雪球 {index_code} 报价API失败: {e}")

    except Exception as e:
        logger.warning(f"雪球 {index_code} 获取失败: {e}")
        result["error"] = str(e)

    return result


def fetch_akshare_pe_pb(index_code, logger):
    """从akshare获取指数PE/PB历史数据"""
    result = {"pe_history": [], "pb_history": [], "error": None}

    try:
        import akshare as ak

        for indicator, field_name in [("市盈率", "pe_history"), ("市净率", "pb_history")]:
            df = ak.index_value_hist_funddb(
                symbol=index_code,
                indicator=indicator,
                period="daily",
                start_date="20150101",
                end_date="20301231"
            )
            history = []
            col = indicator
            for _, row in df.iterrows():
                date_val = row.get("日期")
                val = row.get(col)
                if date_val and val and float(val) > 0:
                    dt = datetime.strptime(str(date_val)[:10], "%Y-%m-%d")
                    ts = int(dt.timestamp() * 1000)
                    history.append({"ts": ts, "value": float(val)})
            history.sort(key=lambda x: x["ts"])
            result[field_name] = history
            logger.info(f"akshare {index_code} {indicator}: {len(history)} 条")

    except ImportError:
        logger.warning("akshare 未安装")
        result["error"] = "akshare未安装"
    except Exception as e:
        logger.warning(f"akshare {index_code} 异常: {e}")
        result["error"] = str(e)

    return result


# ============================================================
# Node4：估值评级
# ============================================================

def calc_rating(pe_pct):
    """根据PE百分位计算估值评级"""
    if pe_pct is None:
        return {"level": "未知", "emoji": "⚪", "color": "#9E9E9E", "label": "未知"}
    if pe_pct > 90:
        return {"level": "极度高估", "emoji": "🔴", "color": "#E53935", "label": "极度高估"}
    elif pe_pct >= 70:
        return {"level": "高估", "emoji": "🟠", "color": "#FB8C00", "label": "高估"}
    elif pe_pct >= 30:
        return {"level": "合理", "emoji": "🟡", "color": "#FDD835", "label": "合理"}
    elif pe_pct >= 10:
        return {"level": "低估", "emoji": "🟢", "color": "#43A047", "label": "低估"}
    else:
        return {"level": "极度低估", "emoji": "🔵", "color": "#1E88E5", "label": "极度低估"}


# ============================================================
# Node5：报告生成
# ============================================================

def generate_simple_report(results, date_str, detail_url=None):
    """生成简版Markdown报告"""
    now_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

    lines = []
    lines.append(f"📊 指数估值日报 ({date_str})")
    lines.append(f"更新时间：{now_str}")
    lines.append(f"数据来源：蛋卷基金（加权PE/PB）+ ETF.run（等权PE/PB）")
    lines.append("")

    sorted_results = sorted(results, key=lambda x: x.get("pe_pct") if x.get("pe_pct") is not None else 999)

    for r in sorted_results:
        rating = r.get("rating", {})
        source_tag = "" if r.get("source") in ("danjuan", "xueqiu", "eastmoney") else " ⚠️兜底数据"
        lines.append(
            f"{rating['emoji']} {r['name']}（{r['code']}）{source_tag}"
        )
        lines.append(f"  PE：{r['pe']}（分位 {r['pe_pct']}%）")
        lines.append(f"  PB：{r['pb']}（分位 {r['pb_pct']}%）")
        if r.get("low_pe") is not None:
            diff_str = f"（需跌{r['low_pe_diff']}%）" if r.get("low_pe_diff") is not None else ""
            lines.append(f"  历史最低PE：{r['low_pe']}（{r['low_pe_date']}）{diff_str}")
        if r.get("low_pb") is not None:
            diff_str = f"（需跌{r['low_pb_diff']}%）" if r.get("low_pb_diff") is not None else ""
            lines.append(f"  历史最低PB：{r['low_pb']}（{r['low_pb_date']}）{diff_str}")
        lines.append(f"  估值评级：{rating['level']}")
        lines.append("")

    lines.append("---")
    lines.append("评级说明：")
    lines.append("🔴 >90% 极度高估 | 🟠 70%-90% 高估 | 🟡 30%-70% 合理 | 🟢 10%-30% 低估 | 🔵 <10% 极度低估")
    lines.append("")
    lines.append("⚠️ 数据说明：")
    lines.append("- PE/PB为蛋卷基金加权数据，基于近10年周频数据计算百分位")
    lines.append("- ETF.run提供等权PE/PB作为辅助参考，不参与主评级")
    lines.append("- 历史最低PE/PB基于蛋卷基金全量历史数据")
    lines.append("- T+1数据，仅供参考，不构成投资建议")

    if detail_url:
        lines.append("")
        lines.append(f"📎 详细版报告：{detail_url}")

    return "\n".join(lines)


def generate_html_report(results, date_str):
    """生成详细版HTML报告"""
    os.makedirs(REPORTS_DIR, exist_ok=True)

    # 按PE百分位从小到大排序
    sorted_results = sorted(results, key=lambda x: x.get("pe_pct") if x.get("pe_pct") is not None else 999)

    now_str = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

    cards_html = ""
    for r in sorted_results:
        rating = r.get("rating", {})
        ew = r.get("etf_run") or {}
        low_pe_html = ""
        if r.get("low_pe") is not None:
            diff_str = f"需跌{r['low_pe_diff']}%" if r.get("low_pe_diff") is not None else ""
            low_pe_html = f"""
            <div class="extra-data">
                <span class="extra-label">历史最低PE</span>
                <span class="extra-value">{r['low_pe']}</span>
                <span class="extra-pct">{r['low_pe_date']} {diff_str}</span>
            </div>"""
        low_pb_html = ""
        if r.get("low_pb") is not None:
            diff_str = f"需跌{r['low_pb_diff']}%" if r.get("low_pb_diff") is not None else ""
            low_pb_html = f"""
            <div class="extra-data">
                <span class="extra-label">历史最低PB</span>
                <span class="extra-value">{r['low_pb']}</span>
                <span class="extra-pct">{r['low_pb_date']} {diff_str}</span>
            </div>"""

        ew_html = ""
        if ew:
            ew_parts = []
            if ew.get("ew_pe") is not None:
                ew_parts.append(f"等权PE {ew['ew_pe']}")
            if ew.get("ew_pe_pct") is not None:
                ew_parts.append(f"分位 {ew['ew_pe_pct']}%")
            if ew.get("ew_pb") is not None:
                ew_parts.append(f"等权PB {ew['ew_pb']}")
            if ew.get("ew_pb_pct") is not None:
                ew_parts.append(f"分位 {ew['ew_pb_pct']}%")
            if ew_parts:
                ew_html = f"""
            <div class="extra-data etf-run">
                <span class="extra-label">ETF.run辅助数据</span>
                <span class="extra-value">{' | '.join(ew_parts)}</span>
            </div>"""

        cards_html += f"""
        <div class="card" style="border-left: 4px solid {rating['color']};">
            <div class="card-header">
                <span class="rating-badge" style="background: {rating['color']};">{rating['emoji']} {rating['level']}</span>
                <span class="index-code">{r['code']}</span>
            </div>
            <div class="card-title">{r['name']}</div>
            <div class="data-grid">
                <div class="data-item">
                    <span class="data-label">PE（加权）</span>
                    <span class="data-value">{r['pe']}</span>
                </div>
                <div class="data-item">
                    <span class="data-label">PE 百分位</span>
                    <span class="data-value highlight">{r['pe_pct']}%</span>
                </div>
                <div class="data-item">
                    <span class="data-label">PB（加权）</span>
                    <span class="data-value">{r['pb']}</span>
                </div>
                <div class="data-item">
                    <span class="data-label">PB 百分位</span>
                    <span class="data-value">{r['pb_pct']}%</span>
                </div>
            </div>
            {low_pe_html}
            {low_pb_html}
            {ew_html}
        </div>"""

    legend_items = ""
    legend_config = [
        ("🔴", "极度高估", ">90%", "#E53935"),
        ("🟠", "高估", "70%-90%", "#FB8C00"),
        ("🟡", "合理", "30%-70%", "#FDD835"),
        ("🟢", "低估", "10%-30%", "#43A047"),
        ("🔵", "极度低估", "<10%", "#1E88E5"),
    ]
    for emoji, label, pct_range, color in legend_config:
        legend_items += f"""
            <div class="legend-item">
                <span class="legend-dot" style="background: {color};"></span>
                <span>{emoji} {label}（{pct_range}）</span>
            </div>"""

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>指数估值报告 - {date_str}</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif;
    background: #f5f6fa;
    color: #2c3e50;
    line-height: 1.6;
}}
.container {{ max-width: 900px; margin: 0 auto; padding: 20px; }}
.header {{
    text-align: center;
    padding: 30px 20px;
    background: linear-gradient(135deg, #1a237e 0%, #283593 100%);
    color: white;
    border-radius: 12px;
    margin-bottom: 24px;
}}
.header h1 {{ font-size: 1.6rem; margin-bottom: 8px; }}
.header .subtitle {{ font-size: 0.85rem; opacity: 0.85; }}
.legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    justify-content: center;
    margin-bottom: 24px;
    padding: 16px;
    background: white;
    border-radius: 10px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}}
.legend-item {{
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 0.82rem;
}}
.legend-dot {{
    width: 10px;
    height: 10px;
    border-radius: 50%;
    display: inline-block;
}}
.cards {{ display: flex; flex-direction: column; gap: 16px; }}
.card {{
    background: white;
    border-radius: 10px;
    padding: 18px 20px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    transition: transform 0.15s;
}}
.card:hover {{ transform: translateY(-1px); box-shadow: 0 3px 12px rgba(0,0,0,0.1); }}
.card-header {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 6px;
}}
.rating-badge {{
    color: white;
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 0.78rem;
    font-weight: 600;
}}
.index-code {{
    font-size: 0.78rem;
    color: #90a4ae;
    font-family: "SF Mono", "Fira Code", monospace;
}}
.card-title {{
    font-size: 1.1rem;
    font-weight: 700;
    margin-bottom: 12px;
    color: #1a237e;
}}
.data-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
    gap: 12px;
}}
.data-item {{
    display: flex;
    flex-direction: column;
    gap: 2px;
}}
.data-label {{
    font-size: 0.72rem;
    color: #90a4ae;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}
.data-value {{
    font-size: 1.05rem;
    font-weight: 600;
    color: #37474f;
}}
.data-value.highlight {{ color: #1a237e; font-size: 1.15rem; }}
.extra-data {{
    margin-top: 12px;
    padding-top: 10px;
    border-top: 1px dashed #e0e0e0;
    display: flex;
    gap: 12px;
    align-items: center;
    font-size: 0.82rem;
}}
.extra-label {{
    color: #90a4ae;
    font-size: 0.72rem;
    white-space: nowrap;
}}
.extra-value {{ color: #5c6bc0; font-weight: 600; }}
.extra-pct {{ color: #37474f; }}
.etf-run {{ background: #f9fafc; padding: 8px 10px; border-radius: 6px; }}
.footer {{
    text-align: center;
    padding: 24px 20px;
    color: #90a4ae;
    font-size: 0.75rem;
    margin-top: 20px;
}}
.footer p {{ margin-bottom: 4px; }}
.sort-info {{
    text-align: center;
    margin-bottom: 16px;
    font-size: 0.8rem;
    color: #90a4ae;
}}

@media (max-width: 600px) {{
    .container {{ padding: 12px; }}
    .header {{ padding: 20px 16px; border-radius: 8px; }}
    .header h1 {{ font-size: 1.3rem; }}
    .card {{ padding: 14px; }}
    .data-grid {{ grid-template-columns: 1fr 1fr; gap: 8px; }}
}}
</style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>📊 指数估值日报</h1>
        <div class="subtitle">{date_str} · 更新于 {now_str} · 数据来源：蛋卷基金 + ETF.run</div>
    </div>

    <div class="legend">
        <span style="font-size:0.8rem;color:#90a4ae;">评级图例：</span>
        {legend_items}
    </div>

    <div class="sort-info">按 PE 百分位从低到高排序（低估 → 高估）</div>

    <div class="cards">
        {cards_html}
    </div>

    <div class="footer">
        <p>数据说明：PE/PB为蛋卷基金加权数据，基于近10年周频数据计算百分位</p>
        <p>ETF.run提供等权PE/PB作为辅助参考，不参与主评级计算</p>
        <p>历史最低PE/PB基于蛋卷基金全量历史数据</p>
        <p>T+1数据，仅供参考，不构成投资建议</p>
    </div>
</div>
</body>
</html>"""

    filename = f"valuation_detail_{date_str}.html"
    filepath = os.path.join(REPORTS_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)

    return filepath


# ============================================================
# Node6：PushDeer推送
# ============================================================

def send_pushdeer(message, logger):
    """通过PushDeer推送消息 — 3次重试+唯一标识防去重"""
    unique_id = datetime.now(BEIJING_TZ).strftime("%H%M%S%f")[:10]
    tagged = f"{message}\n\n[{unique_id}]"

    for attempt in range(1, 4):
        payload = {
            "pushkey": PUSHDEER_KEY,
            "text": tagged,
            "type": "text",
        }
        try:
            logger.info(f"PushDeer 推送中... (第{attempt}次)")
            resp = requests.post(PUSHDEER_URL, data=payload, timeout=15)
            time.sleep(1)
            result = resp.json()
            code = result.get("code")
            if code == 0:
                logger.info(f"PushDeer 推送成功 (第{attempt}次)")
                return True, result
            else:
                logger.warning(f"PushDeer 返回非0: {result}")
        except Exception as e:
            logger.warning(f"PushDeer 推送异常 (第{attempt}次): {e}")

        if attempt < 3:
            wait = [5, 15, 30][attempt - 1]
            logger.info(f"等待{wait}秒后重试...")
            time.sleep(wait)

    logger.error("PushDeer 3次推送均失败")
    return False, "3次重试均失败"


# ============================================================
# 主流程
# ============================================================

def process_index(config, logger):
    """处理单个指数的数据获取和评级"""
    code = config["code"]
    name = config["name"]
    danjuan_code = DANJUAN_CODE_MAP.get(code, code)

    logger.info(f"--- 处理指数: {name}（{code}）---")

    # 随机延迟
    delay = random.uniform(0.5, 2.0)
    time.sleep(delay)

    # 获取蛋卷基金数据
    danjuan_data = fetch_danjuan_pe_pb(danjuan_code, logger)

    result = {
        "code": code,
        "name": name,
        "category": config.get("category", ""),
        "pe": None,
        "pe_pct": None,
        "pb": None,
        "pb_pct": None,
        "rating": None,
        "source": "fallback",
        "etf_run": None,
        "low_pe": None,
        "low_pe_date": None,
        "low_pe_diff": None,
        "low_pb": None,
        "low_pb_date": None,
        "low_pb_diff": None,
    }

    # 解析PE数据
    pe_history = danjuan_data.get("pe_history", [])
    pb_history = danjuan_data.get("pb_history", [])

    # 蛋卷无数据时，依次尝试baostock和雪球
    alt_source = None
    if not pe_history or not pb_history:
        alt_sources = [
            ("akshare", fetch_akshare_pe_pb),
            ("xueqiu", fetch_xueqiu_pe_pb),
        ]
        for src_name, src_func in alt_sources:
            logger.info(f"{name} 蛋卷无数据，尝试{src_name}数据源...")
            alt_data = src_func(code, logger)
            if alt_data.get("pe_history") and alt_data.get("pb_history"):
                pe_history = alt_data["pe_history"]
                pb_history = alt_data["pb_history"]
                alt_source = src_name
                logger.info(f"{name} {src_name}数据获取成功")
                break

    if pe_history and pb_history:
        # 过滤近10年数据
        pe_10y = filter_recent_years(pe_history, TEN_YEARS_MS)
        pb_10y = filter_recent_years(pb_history, TEN_YEARS_MS)

        if pe_10y and pb_10y:
            latest_pe = pe_10y[-1]["value"]
            latest_pb = pb_10y[-1]["value"]
            pe_pct = calc_percentile(pe_10y, latest_pe)
            pb_pct = calc_percentile(pb_10y, latest_pb)

            result["pe"] = round(latest_pe, 2)
            result["pe_pct"] = pe_pct
            result["pb"] = round(latest_pb, 2)
            result["pb_pct"] = pb_pct
            result["source"] = alt_source or "danjuan"

            # 全历史最低PE/PB值、日期及距当前涨跌幅（所有指数）
            low_pe, low_pe_date = find_min_value_with_date(pe_history)
            low_pb, low_pb_date = find_min_value_with_date(pb_history)
            result["low_pe"] = low_pe
            result["low_pe_date"] = low_pe_date
            result["low_pb"] = low_pb
            result["low_pb_date"] = low_pb_date
            if low_pe and result["pe"]:
                pe_diff = round((result["pe"] - low_pe) / result["pe"] * 100, 1)
                result["low_pe_diff"] = pe_diff
            if low_pb and result["pb"]:
                pb_diff = round((result["pb"] - low_pb) / result["pb"] * 100, 1)
                result["low_pb_diff"] = pb_diff
            if low_pe:
                logger.info(f"{name} 历史最低PE: {low_pe}（{low_pe_date}）, 距最低需跌{pe_diff}%")
            if low_pb:
                logger.info(f"{name} 历史最低PB: {low_pb}（{low_pb_date}）, 距最低需跌{pb_diff}%")
        else:
            logger.warning(f"{name} 近10年数据不足，使用兜底数据")
    else:
        logger.warning(f"{name} 蛋卷API数据获取失败，使用兜底数据")

    # 兜底数据
    if result["pe"] is None:
        fb = FALLBACK_DATA.get(code, {})
        result["pe"] = fb.get("pe")
        result["pe_pct"] = fb.get("pe_pct")
        result["pb"] = fb.get("pb")
        result["pb_pct"] = fb.get("pb_pct")
        result["low_pe"] = fb.get("low_pe")
        result["low_pe_date"] = fb.get("low_pe_date")
        result["low_pe_diff"] = fb.get("low_pe_diff")
        result["low_pb"] = fb.get("low_pb")
        result["low_pb_date"] = fb.get("low_pb_date")
        result["low_pb_diff"] = fb.get("low_pb_diff")
        result["source"] = "fallback"

    # A股指数获取ETF.run辅助数据
    if code in ETF_RUN_URLS:
        delay2 = random.uniform(0.5, 1.5)
        time.sleep(delay2)
        etf_data = fetch_etf_run_data(code, logger)
        if etf_data:
            result["etf_run"] = etf_data

    # 计算估值评级
    result["rating"] = calc_rating(result["pe_pct"])

    logger.info(
        f"{name} 结果: PE={result['pe']} (分位{result['pe_pct']}%), "
        f"PB={result['pb']} (分位{result['pb_pct']}%), "
        f"评级={result['rating']['level']}, 来源={result['source']}"
    )

    return result


def run_workflow(push=False, detail_url=None, logger=None):
    """主工作流"""
    if logger is None:
        logger = logging.getLogger("valuation")

    beijing_now = datetime.now(BEIJING_TZ)
    date_str = beijing_now.strftime("%Y%m%d")

    logger.info("=" * 50)
    logger.info(f"指数估值工作流启动 - {date_str}")
    logger.info("=" * 50)

    results = []
    errors = []

    for config in INDEX_CONFIG:
        try:
            result = process_index(config, logger)
            results.append(result)
        except Exception as e:
            logger.error(f"处理 {config['name']} 异常: {e}", exc_info=True)
            errors.append({"index": config["name"], "error": str(e)})
            # 使用兜底数据
            fb = FALLBACK_DATA.get(config["code"], {})
            fallback_result = {
                "code": config["code"],
                "name": config["name"],
                "category": config.get("category", ""),
                "pe": fb.get("pe"),
                "pe_pct": fb.get("pe_pct"),
                "pb": fb.get("pb"),
                "pb_pct": fb.get("pb_pct"),
                "rating": calc_rating(fb.get("pe_pct")),
                "source": "fallback",
                "etf_run": None,
                "low_pe": fb.get("low_pe"),
                "low_pe_date": fb.get("low_pe_date"),
                "low_pe_diff": fb.get("low_pe_diff"),
                "low_pb": fb.get("low_pb"),
                "low_pb_date": fb.get("low_pb_date"),
                "low_pb_diff": fb.get("low_pb_diff"),
            }
            results.append(fallback_result)

    # 生成简版报告
    simple_report = generate_simple_report(results, date_str, detail_url)
    logger.info("简版报告生成完成")

    # 生成HTML详细报告
    html_path = generate_html_report(results, date_str)
    html_rel_path = os.path.relpath(html_path, BASE_DIR)
    logger.info(f"HTML详细报告已生成: {html_path}")

    # 推送PushDeer
    push_status = None
    if push:
        push_status = send_pushdeer(simple_report, logger)
        logger.info(f"PushDeer推送结果: {push_status}")

    # 统计数据来源
    from_danjuan = sum(1 for r in results if r.get("source") == "danjuan")
    from_akshare = sum(1 for r in results if r.get("source") == "akshare")
    from_xueqiu = sum(1 for r in results if r.get("source") == "xueqiu")
    from_realtime = from_danjuan + from_akshare + from_xueqiu
    from_fallback = sum(1 for r in results if r.get("source") == "fallback")

    # 写入状态文件
    status = {
        "task": "指数估值日报",
        "date": date_str,
        "timestamp": beijing_now.isoformat(),
        "success": len(errors) == 0,
        "total_indices": len(INDEX_CONFIG),
        "fetched_count": len(results),
        "danjuan_count": from_danjuan,
        "akshare_count": from_akshare,
        "xueqiu_count": from_xueqiu,
        "fallback_count": from_fallback,
        "error_count": len(errors),
        "errors": errors if errors else None,
        "summary": f"共处理 {len(results)} 个指数(蛋卷{from_danjuan}个, akshare{from_akshare}个, 雪球{from_xueqiu}个, 兜底{from_fallback}个), 错误 {len(errors)} 个",
        "pushdeer": {
            "enabled": push,
            "success": push_status[0] if push_status else None,
            "response": str(push_status[1]) if push_status else None,
        } if push else {"enabled": False},
        "html_file": html_rel_path,
    }

    status_file = os.path.join(LOGS_DIR, f"valuation_{date_str}_status.json")
    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    logger.info(f"状态文件已保存: {status_file}")

    logger.info("=" * 50)
    logger.info(f"工作流完成 - {'成功' if len(errors) == 0 else '部分失败'}")
    logger.info("=" * 50)

    return {
        "success": len(errors) == 0,
        "simple_report": simple_report,
        "html_file_path": html_rel_path,
        "date": date_str,
    }


# ============================================================
# 命令行入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="指数估值数据采集与报告推送系统")
    parser.add_argument("--push", action="store_true", default=False, help="推送简版报告到PushDeer")
    parser.add_argument("--no-push", action="store_true", default=True, help="不推送（默认）")
    parser.add_argument("--detail-url", type=str, default=None, help="详细版报告的URL，将附加到简版报告中")

    args = parser.parse_args()

    do_push = args.push and not args.no_push
    if args.push:
        do_push = True

    date_str = datetime.now(BEIJING_TZ).strftime("%Y%m%d")
    logger = setup_logging(date_str)

    result = run_workflow(push=do_push, detail_url=args.detail_url, logger=logger)

    print("\n" + "=" * 50)
    print("工作流执行完成")
    print(f"  成功: {result['success']}")
    print(f"  日期: {result['date']}")
    print(f"  HTML报告: {result['html_file_path']}")
    if do_push:
        print(f"  PushDeer: 已推送")
    print("=" * 50)

    return 0 if result["success"] else 1


if __name__ == "__main__":
    sys.exit(main())
