#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parallel MySQL Slow Log Aggregator (robust v2)
- Memory-map + boundary scan; shards aligned to record starts
- Multiprocessing (--jobs N) to parse shards in parallel
- Exact p95 using numpy (memory is assumed OK) — keeps per-fingerprint durations
- Chinese headers by default (--lang zh), English via --lang en
- Robust to truncated logs: optional marking and stats
- "Loose start" mode: treat "# Query_time:" as a valid record start when "# Time:" is missing
- Prints detailed timing and counters with --stats (default on)
"""
import os, re, sys, argparse, mmap, math, hashlib, time
from multiprocessing import Pool, cpu_count
from collections import defaultdict
from datetime import datetime, timedelta, timezone

try:
    import numpy as np
    import pandas as pd
except Exception as e:
    print("This script requires numpy and pandas. pip install numpy pandas", file=sys.stderr)
    raise

# Elasticsearch integration (optional)
try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
    ES_AVAILABLE = True
except ImportError:
    ES_AVAILABLE = False

# ---------- Normalization helpers (Enhanced) ----------
# 增强的MySQL Hint处理 - 支持更多格式
re_mysql_hint_versioned = re.compile(r"/\*![0-9]{5}.*?\*/", flags=re.DOTALL)  # /*!40001 ... */
re_mysql_hint_simple = re.compile(r"/\*!(?![0-9]{5}).*?\*/", flags=re.DOTALL)   # /*!STRAIGHT_JOIN */ 等
re_mysql_hint_executor = re.compile(r"/\*\+.*?\*/", flags=re.DOTALL)             # /*+ ... */ Oracle风格
re_inline_comment = re.compile(r"(--[^\n]*$)", flags=re.MULTILINE)
re_block_comment = re.compile(r"/\*(?![!+]).*?\*/", flags=re.DOTALL)            # 普通注释，不包括hint
re_string = re.compile(r"('([^'\\]|\\.)*'|\"([^\"\\]|\\.)*\")", flags=re.DOTALL)
re_numeric = re.compile(r"\b\d+(\.\d+)?\b")
re_in_list = re.compile(r"\bIN\s*\((?:[^()]*|\([^()]*\))*\)", flags=re.IGNORECASE)
re_whitespace = re.compile(r"\s+")

def normalize_sql(sql: str) -> str:
    """
    增强版SQL规范化，更彻底地处理MySQL Hints和各种注释
    """
    s = sql.strip()
    
    # 按顺序处理各种MySQL Hints（从具体到一般）
    s = re_mysql_hint_versioned.sub(" ", s)    # /*!40001 SQL_NO_CACHE */
    s = re_mysql_hint_simple.sub(" ", s)       # /*!STRAIGHT_JOIN */
    s = re_mysql_hint_executor.sub(" ", s)     # /*+ USE_INDEX(t1 idx1) */
    
    # 处理其他注释
    s = re_block_comment.sub(" ", s)           # /* 普通注释 */
    s = re_inline_comment.sub(" ", s)          # -- 行注释
    
    # 参数化处理
    s = re_in_list.sub(" IN (?) ", s)
    s = re_string.sub("?", s)
    s = re_numeric.sub("?", s)
    
    # 清理空白和格式化
    s = re_whitespace.sub(" ", s)
    s = s.rstrip("; ").strip()
    s = s.lower()
    
    return s

def fingerprint(sql: str) -> str:
    return hashlib.md5(normalize_sql(sql).encode("utf-8")).hexdigest()

def extract_main_table(sql: str):
    m = re.search(r"\bfrom\s+([`\"\w\.\-]+)", sql, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"\bupdate\s+([`\"\w\.\-]+)", sql, flags=re.IGNORECASE)
    if not m:
        m = re.search(r"\binto\s+([`\"\w\.\-]+)", sql, flags=re.IGNORECASE)
    return m.group(1).strip("`\"") if m else None

# ---------- Time filtering helpers (Expert Redesign) ----------
def parse_mysql_time(time_str: str) -> datetime:
    """
    Parse various MySQL slow log time formats to datetime object.
    Optimized for performance and robustness.
    """
    if not time_str:
        return None
    
    time_str = time_str.strip()
    
    # Fast path: ISO format (most common in modern MySQL)
    try:
        if 'T' in time_str and ('+' in time_str or 'Z' in time_str):
            return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except:
        pass
    
    # Unix timestamp (fastest to parse)
    try:
        if time_str.isdigit() and len(time_str) == 10:
            return datetime.fromtimestamp(int(time_str), tz=timezone.utc)
    except:
        pass
    
    # MySQL short format with optimized parsing
    try:
        if '-' in time_str:
            return datetime.strptime(time_str[:19], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        elif len(time_str.split()[0]) == 6:
            dt = datetime.strptime(time_str[:15], '%y%m%d %H:%M:%S')
            if dt.year < 1931:
                dt = dt.replace(year=dt.year + 100)
            return dt.replace(tzinfo=timezone.utc)
    except:
        pass
    
    return None

def calculate_time_range(days: int) -> tuple:
    """Calculate time range for filtering with timezone awareness."""
    now = datetime.now(timezone.utc)
    
    if days == 0:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    else:
        start = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
    
    return start, end

def smart_time_range_check(file_path: str, time_range: tuple, max_sample_size: int = 10 * 1024 * 1024) -> dict:
    """
    Smart sampling to determine if file contains data in time range.
    Uses head/middle/tail sampling instead of full scan.
    Returns: {
        'has_data_in_range': bool,
        'estimated_coverage': float,  # 0.0-1.0
        'sample_times': [datetime, ...],
        'file_time_range': (start_dt, end_dt)
    }
    """
    if not time_range:
        return {'has_data_in_range': True, 'estimated_coverage': 1.0}
    
    try:
        file_size = os.path.getsize(file_path)
        # For small files, do a more thorough check
        sample_size = min(max_sample_size, file_size // 3)
        
        sample_times = []
        file_start_time = None
        file_end_time = None
        
        with open(file_path, 'rb') as f:
            # Sample from head, middle, tail
            positions = [0, max(0, file_size // 2 - sample_size // 2), max(0, file_size - sample_size)]
            
            for pos in positions:
                f.seek(pos)
                chunk = f.read(sample_size).decode('utf-8', errors='ignore')
                
                # Find time patterns
                for line in chunk.split('\n')[:200]:  # Limit lines per chunk
                    if line.startswith('# Time:'):
                        time_str = line.split('# Time:', 1)[1].strip()
                        dt = parse_mysql_time(time_str)
                        if dt:
                            sample_times.append(dt)
                            if file_start_time is None or dt < file_start_time:
                                file_start_time = dt
                            if file_end_time is None or dt > file_end_time:
                                file_end_time = dt
                    elif line.startswith('SET timestamp='):
                        m = re.search(r'SET timestamp=(\d+);', line)
                        if m:
                            dt = parse_mysql_time(m.group(1))
                            if dt:
                                sample_times.append(dt)
                                if file_start_time is None or dt < file_start_time:
                                    file_start_time = dt
                                if file_end_time is None or dt > file_end_time:
                                    file_end_time = dt
        
        if not sample_times:
            return {'has_data_in_range': True, 'estimated_coverage': 0.0, 'reason': 'no_timestamps_found'}
        
        # Check if any sample time is in range
        target_start, target_end = time_range
        has_data = any(target_start <= dt <= target_end for dt in sample_times)
        
        # 更智能的覆盖率估算
        coverage = 0.0
        coverage_type = "unknown"
        
        if file_start_time and file_end_time and has_data:
            # 计算文件时间跨度和目标时间跨度
            file_duration = (file_end_time - file_start_time).total_seconds()
            target_duration = (target_end - target_start).total_seconds()
            
            # 计算重叠部分
            overlap_start = max(file_start_time, target_start)
            overlap_end = min(file_end_time, target_end)
            
            if overlap_start <= overlap_end:
                overlap_duration = (overlap_end - overlap_start).total_seconds()
                
                # 情况1：文件完全在目标范围内
                if file_start_time >= target_start and file_end_time <= target_end:
                    coverage = 1.0  # 文件数据完全覆盖，只是时间范围小
                    coverage_type = "full_file_in_range"
                
                # 情况2：目标范围完全在文件内
                elif target_start >= file_start_time and target_end <= file_end_time:
                    coverage = 1.0  # 目标时间完全被文件覆盖
                    coverage_type = "full_range_covered"
                
                # 情况3：部分重叠，按实际数据密度估算
                else:
                    # 基于重叠时间占文件时间的比例来估算
                    if file_duration > 0:
                        file_overlap_ratio = overlap_duration / file_duration
                        # 如果重叠部分占文件时间的大部分，认为覆盖率较高
                        if file_overlap_ratio > 0.8:
                            coverage = 0.9  # 大部分文件数据在范围内
                            coverage_type = "mostly_covered"
                        elif file_overlap_ratio > 0.5:
                            coverage = 0.7  # 一半以上文件数据在范围内
                            coverage_type = "partially_covered"
                        else:
                            coverage = file_overlap_ratio * 0.5  # 保守估算
                            coverage_type = "limited_overlap"
            else:
                coverage = 0.0
                coverage_type = "no_overlap"
        
        return {
            'has_data_in_range': has_data,
            'estimated_coverage': coverage,
            'coverage_type': coverage_type,
            'sample_times': sample_times,
            'file_time_range': (file_start_time, file_end_time),
            'sample_count': len(sample_times)
        }
        
    except Exception as e:
        # Graceful fallback
        return {'has_data_in_range': True, 'estimated_coverage': 1.0, 'error': str(e)}

# ---------- Boundary scan ----------
def _find_all(mm, needle: bytes):
    pos = 0
    out = []
    L = len(needle)
    while True:
        i = mm.find(needle, pos)
        if i == -1:
            break
        out.append(i + (1 if needle.startswith(b"\n") else 0))  # point at '#'
        pos = i + L
    return out

def compute_boundaries(path: str, max_parts: int, loose_start: bool):
    """
    Lightweight boundary scan without time filtering.
    Time filtering is now handled by smart sampling and worker-level filtering.
    """
    with open(path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            starts = set()
            if loose_start:
                if mm[:7] == b"# Time:":
                    starts.add(0)
                if mm[:14] == b"# Query_time:":
                    starts.add(0)
                starts.update(_find_all(mm, b"\n# Time:"))
                starts.update(_find_all(mm, b"\n# Query_time:"))
            else:
                if mm[:7] == b"# Time:":
                    starts.add(0)
                starts.update(_find_all(mm, b"\n# Time:"))
            
            starts = sorted(starts)
            if not starts:
                return [(0, mm.size())], 0, mm.size()
            
            parts = min(max_parts, max(1, len(starts)))
            idxs = [int(round(i*len(starts)/parts)) for i in range(parts+1)]
            idxs = sorted(set(max(0, min(k, len(starts))) for k in idxs))
            
            shards = []
            for a,b in zip(idxs[:-1], idxs[1:]):
                if a==b: 
                    continue
                start = starts[a]
                end = starts[b] if b < len(starts) else mm.size()
                shards.append((start, end))
            
            if not shards:
                shards = [(0, mm.size())]
            return shards, len(starts), mm.size()
        finally:
            mm.close()

# ---------- Chunk parser ----------
def parse_chunk(args):
    """
    Enhanced chunk parser with early termination and memory awareness.
    Returns (agg_dict, stats_dict)
    """
    path, start, end, min_time, exclude_dumps, mark_truncated, loose_start, time_range = args
    with open(path, "rb") as fb:
        fb.seek(start)
        data = fb.read(end - start)
    text = data.decode("utf-8", errors="ignore").splitlines()

    stats = {
        "time_lines": 0,
        "qtime_lines": 0,
        "parsed_records": 0,
        "filtered_min_time": 0,
        "filtered_dumps": 0,
        "truncated_records": 0,
        "filtered_time_range": 0,
    }

    result = {}

    current = {
        "time": None, "user_host": None, "query_time": None, "lock_time": None,
        "rows_sent": None, "rows_examined": None, "bytes_sent": None, "bytes_received": None,
        "start": None, "end": None, "db": None, "set_timestamp": None, "thread_id": None, "errno": None,
    }
    sql_buf = []
    last_db = None
    started = False  # have we seen a record header?

    def add_entry(entry, truncated=False):
        nonlocal result, last_db
        sql = entry.get("sql","").strip()
        if not sql:
            return
        if exclude_dumps and "sql_no_cache" in sql.lower() and "/*!" in sql:
            stats["filtered_dumps"] += 1
            return
        qt = entry.get("query_time")
        if qt is None:
            return
        try:
            qt = float(qt)
        except:
            return
        if qt < min_time:
            stats["filtered_min_time"] += 1
            return
        
        # Apply time range filtering (optimized)
        if time_range:
            time_in_range = False
            # Check primary time source first (most reliable)
            time_val = entry.get("time")
            if time_val:
                dt = parse_mysql_time(str(time_val))
                if dt:
                    target_start, target_end = time_range
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    elif dt.tzinfo != timezone.utc:
                        dt = dt.astimezone(timezone.utc)
                    time_in_range = target_start <= dt <= target_end
            
            # Fallback to other time sources if needed
            if not time_in_range:
                for time_key in ("set_timestamp", "start", "end"):
                    time_val = entry.get(time_key)
                    if time_val:
                        dt = parse_mysql_time(str(time_val))
                        if dt:
                            target_start, target_end = time_range
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            elif dt.tzinfo != timezone.utc:
                                dt = dt.astimezone(timezone.utc)
                            if target_start <= dt <= target_end:
                                time_in_range = True
                                break
            
            if not time_in_range:
                stats["filtered_time_range"] += 1
                return

        if truncated and mark_truncated:
            sql = sql + " /* TRUNCATED */"

        fp = fingerprint(sql)
        g = result.get(fp)
        if g is None:
            g = {
                "samples": 0,
                "total_time_s": 0.0,
                "max_time_s": 0.0,
                "sum_lock_time_s": 0.0,
                "rows_examined_total": 0,
                "rows_sent_total": 0,
                "durations": [],
                "first_seen": None,
                "last_seen": None,
                "norm_sql": normalize_sql(sql),
                "example_query": sql[:1500],
                "db": entry.get("db"),
                "user_host": entry.get("user_host"),
                "main_table": extract_main_table(sql),
                "has_truncated": bool(truncated),
            }
            result[fp] = g
        else:
            if truncated:
                g["has_truncated"] = True

        g["samples"] += 1
        g["total_time_s"] += qt
        g["durations"].append(qt)
        if qt > g["max_time_s"]:
            g["max_time_s"] = qt
        lt = entry.get("lock_time")
        if lt is not None:
            try: g["sum_lock_time_s"] += float(lt)
            except: pass
        rex = entry.get("rows_examined")
        if rex is not None:
            try: g["rows_examined_total"] += int(rex)
            except: pass
        rs = entry.get("rows_sent")
        if rs is not None:
            try: g["rows_sent_total"] += int(rs)
            except: pass

        # update times by raw string compare
        for key in ("time","start","end","set_timestamp"):
            val = entry.get(key)
            if not val: continue
            if g["first_seen"] is None or str(val) < str(g["first_seen"]):
                g["first_seen"] = val
            if g["last_seen"] is None or str(val) > str(g["last_seen"]):
                g["last_seen"] = val

    def flush(truncated=False):
        nonlocal current, sql_buf, last_db, started
        sql = "\n".join(sql_buf).strip()
        if sql:
            row = current.copy()
            row["db"] = current["db"] or last_db
            row["sql"] = sql
            add_entry(row, truncated=truncated)
            stats["parsed_records"] += 1
        current = {k: None for k in current}
        sql_buf = []

    for l in text:
        if l.startswith("# Time:"):
            stats["time_lines"] += 1
            if sql_buf and started:
                flush(truncated=False)
            started = True
            current["time"] = l.split("# Time:",1)[1].strip()
            continue
        if l.startswith("# Query_time:"):
            stats["qtime_lines"] += 1
            # In loose mode, if we haven't seen a start yet, treat this as a header start
            if loose_start and not started:
                if sql_buf:
                    flush(truncated=False)
                started = True
            qt = re.search(r"Query_time:\s*([\d\.]+)", l)
            lt = re.search(r"Lock_time:\s*([\d\.]+)", l)
            rs = re.search(r"Rows_sent:\s*(\d+)", l)
            rexa = re.search(r"Rows_examined:\s*(\d+)", l)
            st = re.search(r"Start:\s*([^\s]+)", l)
            en = re.search(r"End:\s*([^\s]+)", l)
            if qt: current["query_time"] = float(qt.group(1))
            if lt: current["lock_time"] = float(lt.group(1))
            if rs: current["rows_sent"] = int(rs.group(1))
            if rexa: current["rows_examined"] = int(rexa.group(1))
            if st: current["start"] = st.group(1)
            if en: current["end"] = en.group(1)
            continue
        m_use = re.match(r"\s*use\s+([`\"\w\.\-]+);", l, flags=re.IGNORECASE)
        if m_use:
            dbname = m_use.group(1).strip("`\"")
            current["db"] = dbname
            last_db = dbname
            continue
        if l.startswith("SET timestamp="):
            m = re.search(r"SET timestamp=(\d+);", l)
            if m:
                current["set_timestamp"] = m.group(1)
            continue
        if l.startswith("# "):
            continue
        if not l.strip():
            continue
        sql_buf.append(l)

    # tail flush: if we were inside a record, mark as possibly truncated
    if sql_buf:
        stats["truncated_records"] += 1
        flush(truncated=True)

    return result, stats

# ---------- Merge worker results ----------
def merge_results(parts):
    agg = {}
    stats_total = {
        "time_lines": 0, "qtime_lines": 0, "parsed_records": 0,
        "filtered_min_time": 0, "filtered_dumps": 0, "truncated_records": 0, "filtered_time_range": 0,
    }
    for d, st in parts:
        for k in stats_total:
            stats_total[k] += st.get(k, 0)
        for fp, g in d.items():
            t = agg.get(fp)
            if t is None:
                agg[fp] = {
                    "fingerprint": fp,
                    "samples": g["samples"],
                    "total_time_s": g["total_time_s"],
                    "max_time_s": g["max_time_s"],
                    "sum_lock_time_s": g["sum_lock_time_s"],
                    "rows_examined_total": g["rows_examined_total"],
                    "rows_sent_total": g["rows_sent_total"],
                    "durations": list(g["durations"]),
                    "first_seen": g["first_seen"],
                    "last_seen": g["last_seen"],
                    "norm_sql": g["norm_sql"],
                    "example_query": g["example_query"],
                    "db": g["db"],
                    "user_host": g["user_host"],
                    "main_table": g["main_table"],
                    "has_truncated": g.get("has_truncated", False),
                }
            else:
                t["samples"] += g["samples"]
                t["total_time_s"] += g["total_time_s"]
                t["max_time_s"] = max(t["max_time_s"], g["max_time_s"])
                t["sum_lock_time_s"] += g["sum_lock_time_s"]
                t["rows_examined_total"] += g["rows_examined_total"]
                t["rows_sent_total"] += g["rows_sent_total"]
                t["durations"].extend(g["durations"])
                if g["first_seen"] and (t["first_seen"] is None or str(g["first_seen"]) < str(t["first_seen"])):
                    t["first_seen"] = g["first_seen"]
                if g["last_seen"] and (t["last_seen"] is None or str(g["last_seen"]) > str(t["last_seen"])):
                    t["last_seen"] = g["last_seen"]
                if g.get("has_truncated"):
                    t["has_truncated"] = True
                for k in ("db","user_host","main_table"):
                    if not t[k] and g[k]:
                        t[k] = g[k]
    return agg, stats_total

# ---------- Build DataFrame & outputs ----------
def build_dataframe(agg):
    rows = []
    for fp, g in agg.items():
        durations = np.array(g["durations"], dtype=float)
        avg_time = float(np.mean(durations)) if len(durations) else 0.0
        p95_time = float(np.percentile(durations, 95)) if len(durations) else np.nan
        avg_lock = g["sum_lock_time_s"]/g["samples"] if g["samples"] else np.nan
        rows_examined_avg = g["rows_examined_total"]/g["samples"] if g["samples"] else np.nan
        rows_sent_avg = g["rows_sent_total"]/g["samples"] if g["samples"] else np.nan
        rows.append({
            "fingerprint": fp,
            "samples": g["samples"],
            "total_time_s": g["total_time_s"],
            "avg_time_s": avg_time,
            "p95_time_s": p95_time,
            "max_time_s": g["max_time_s"],
            "avg_lock_time_s": avg_lock,
            "rows_examined_total": g["rows_examined_total"],
            "rows_examined_avg": rows_examined_avg,
            "rows_sent_total": g["rows_sent_total"],
            "rows_sent_avg": rows_sent_avg,
            "first_seen": g["first_seen"],
            "last_seen": g["last_seen"],
            "example_query": g["example_query"],
            "norm_sql": g["norm_sql"],
            "db": g["db"],
            "user_host": g["user_host"],
            "main_table": g["main_table"],
            "has_truncated": g.get("has_truncated", False),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        total_time = df["total_time_s"].sum()
        total_count = df["samples"].sum()
        df["time_share_pct"]  = (df["total_time_s"]/total_time*100.0).round(3)
        df["count_share_pct"] = (df["samples"]/total_count*100.0).round(3)
        df.sort_values(["total_time_s","samples"], ascending=[False, False], inplace=True)
    return df

def rename_columns(df: "pd.DataFrame", lang: str) -> "pd.DataFrame":
    if df is None or df.empty:
        return df
    if lang.lower() not in ("zh", "en"):
        lang = "zh"
    if lang.lower() == "en":
        return df
    col_map = {
        "fingerprint": "指纹",
        "samples": "样本数",
        "total_time_s": "总耗时(s)",
        "avg_time_s": "平均耗时(s)",
        "p95_time_s": "P95耗时(s)",
        "max_time_s": "最大耗时(s)",
        "time_share_pct": "总耗时占比(%)",
        "count_share_pct": "次数占比(%)",
        "avg_lock_time_s": "平均锁等待(s)",
        "rows_examined_total": "扫描行数-总计",
        "rows_examined_avg": "扫描行数-平均",
        "rows_sent_total": "返回行数-总计",
        "rows_sent_avg": "返回行数-平均",
        "db": "数据库",
        "main_table": "主表",
        "user_host": "用户@主机",
        "norm_sql": "规范化SQL",
        "example_query": "示例SQL",
        "first_seen": "首次出现时间",
        "last_seen": "最后出现时间",
        "has_truncated": "含截断样本",
    }
    out = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns}).copy()
    order = ["指纹","样本数","总耗时(s)","平均耗时(s)","P95耗时(s)","最大耗时(s)","总耗时占比(%)","次数占比(%)",
             "平均锁等待(s)","扫描行数-总计","扫描行数-平均","返回行数-总计","返回行数-平均",
             "数据库","主表","用户@主机","规范化SQL","示例SQL","首次出现时间","最后出现时间","含截断样本"]
    cols = [c for c in order if c in out.columns] + [c for c in out.columns if c not in order]
    return out[cols]

def write_markdown(df, out_md, top):
    if df is None or df.empty:
        with open(out_md, "w", encoding="utf-8") as f:
            f.write("# MySQL 慢日志汇总\n\n（无数据）\n")
        return
    topN = min(top, len(df))
    lines = []
    lines.append(f"# MySQL 慢日志汇总（Top {topN} 按总耗时）\n")
    lines.append(f"- 总样本数：**{int(df['samples'].sum())}**\n")
    lines.append(f"- 总耗时：**{df['total_time_s'].sum():.3f} s**\n")
    lines.append("| 排名 | 样本数 | 总耗时(s) | 平均耗时(s) | P95耗时(s) | 最大耗时(s) | 总耗时占比(%) | 主表 | 数据库 | 指纹 | 规范化SQL(前120字) |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|")
    for i, row in df.head(topN).reset_index(drop=True).iterrows():
        norm_short = (str(row.get("norm_sql",""))[:120]).replace("|","\\|")
        p95v = "" if (row.get("p95_time_s") is None or pd.isna(row.get("p95_time_s"))) else f"{float(row['p95_time_s']):.3f}"
        lines.append("| {} | {} | {:.3f} | {:.3f} | {} | {:.3f} | {} | {} | {} | `{}` | {} |".format(
            i+1, int(row["samples"]), float(row["total_time_s"]), float(row["avg_time_s"]),
            p95v, float(row["max_time_s"]), row.get("time_share_pct",""), row.get("main_table","") or "",
            row.get("db","") or "", row["fingerprint"], norm_short
        ))
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# ---------- Elasticsearch Integration ----------
def create_es_client(es_hosts, es_user=None, es_password=None, es_ca_certs=None, es_verify_certs=True):
    """
    创建Elasticsearch客户端连接
    """
    if not ES_AVAILABLE:
        raise ImportError("elasticsearch package not available. Install with: pip install elasticsearch")
    
    try:
        # 构建连接配置
        config = {
            'hosts': es_hosts if isinstance(es_hosts, list) else [es_hosts],
            'verify_certs': es_verify_certs,
            'request_timeout': 30,
            'retry_on_timeout': True,
            'max_retries': 3
        }
        
        # 添加认证（使用新的API）
        if es_user and es_password:
            config['basic_auth'] = (es_user, es_password)
        
        # 添加CA证书
        if es_ca_certs:
            config['ca_certs'] = es_ca_certs
        
        client = Elasticsearch(**config)
        
        # 测试连接
        if not client.ping():
            raise ConnectionError("无法连接到Elasticsearch集群")
        
        return client
    
    except Exception as e:
        print(f"⚠️ Elasticsearch连接失败: {e}")
        return None

def prepare_es_documents(df, index_pattern="mysql-slowlog-%{+yyyy.MM.dd}", hostname=None, log_file_path=None):
    """
    将DataFrame转换为Elasticsearch文档格式
    
    遵循ECS (Elastic Common Schema) 标准：
    - host.*: 主机元数据（名称、IP、操作系统）
    - log.file.*: 日志文件信息（路径、名称、目录）
    - agent.*: 采集器信息（名称、版本、类型）
    - service.*: 服务标识（MySQL数据库）
    - mysql.slowlog.*: MySQL慢日志专用字段
    
    优势：
    - 符合Elastic Stack生态标准
    - 无重复字段，节省存储空间
    - 便于跨数据源关联分析
    - 支持标准化可视化模板
    """
    if df is None or df.empty:
        return []
    
    documents = []
    current_time = datetime.now(timezone.utc)
    
    # 解析索引模式中的日期
    index_name = current_time.strftime(index_pattern.replace('%{+yyyy.MM.dd}', '%Y.%m.%d'))
    
    # 获取系统信息
    import socket
    import platform
    
    # 主机信息
    system_hostname = hostname or socket.gethostname()
    system_ip = socket.getfqdn()
    system_os = platform.system()
    system_arch = platform.machine()
    
    # 文件路径信息（跨平台兼容）
    if log_file_path:
        file_path = os.path.abspath(log_file_path).replace('\\', '/')  # 统一使用正斜杠
        file_name = os.path.basename(log_file_path)
        file_dir = os.path.dirname(file_path)
    else:
        file_path = 'unknown'
        file_name = 'unknown'
        file_dir = 'unknown'
    
    for _, row in df.iterrows():
        # 构建ES文档
        doc = {
            '@timestamp': current_time.isoformat(),
            'analysis_date': current_time.strftime('%Y-%m-%d'),
            
            # ========== 数据源标识（类似filebeat） ==========
            'host': {
                'name': system_hostname,
                'ip': system_ip,
                'os': {
                    'family': system_os,
                    'platform': system_os.lower(),
                    'architecture': system_arch
                }
            },
            'log': {
                'file': {
                    'path': file_path,
                    'name': file_name,
                    'directory': file_dir
                }
            },
            'agent': {
                'name': 'mysql_slowlog_analyzer',
                'version': 'expert',
                'type': 'mysql_analyzer'
            },
            
            # ========== 业务标识 ==========
            'service': {
                'name': 'mysql',
                'type': 'database'
            },
            'mysql': {
                'slowlog': {
                    'fingerprint': row.get('fingerprint', ''),
                    'samples': int(row.get('samples', 0)),
                    
                    # 性能指标
                    'query_time': {
                        'total_seconds': float(row.get('total_time_s', 0)),
                        'avg_seconds': float(row.get('avg_time_s', 0)),
                        'p95_seconds': float(row.get('p95_time_s', 0)) if pd.notna(row.get('p95_time_s')) else None,
                        'max_seconds': float(row.get('max_time_s', 0))
                    },
                    
                    # 占比信息
                    'share': {
                        'time_percent': float(row.get('time_share_pct', 0)),
                        'count_percent': float(row.get('count_share_pct', 0))
                    },
                    
                    # 锁和行数统计
                    'lock_time': {
                        'avg_seconds': float(row.get('avg_lock_time_s', 0)) if pd.notna(row.get('avg_lock_time_s')) else 0
                    },
                    'rows': {
                        'examined_total': int(row.get('rows_examined_total', 0)),
                        'examined_avg': float(row.get('rows_examined_avg', 0)) if pd.notna(row.get('rows_examined_avg')) else 0,
                        'sent_total': int(row.get('rows_sent_total', 0)),
                        'sent_avg': float(row.get('rows_sent_avg', 0)) if pd.notna(row.get('rows_sent_avg')) else 0
                    },
                    
                    # 维度信息
                    'database': row.get('db', '') or '',
                    'table': row.get('main_table', '') or '',
                    'user_host': row.get('user_host', '') or '',
                    
                    # SQL信息
                    'sql': {
                        'normalized': row.get('norm_sql', ''),
                        'example': row.get('example_query', '') or '',  # 完整显示，不限制长度
                        'has_truncated': bool(row.get('has_truncated', False))
                    },
                    
                    # 时间信息
                    'time_range': {
                        'first_seen': row.get('first_seen', ''),
                        'last_seen': row.get('last_seen', '')
                    }
                }
            },
            
            # ========== ECS标准结构，无重复字段 ==========
            # 所有数据通过标准ECS字段访问：
            # - 主机信息: host.*
            # - 日志文件: log.file.*
            # - MySQL数据: mysql.slowlog.*
            # - 服务信息: service.*
            # - 采集器: agent.*
        }
        
        documents.append({
            '_index': index_name,
            '_source': doc
        })
    
    return documents

def send_to_elasticsearch(client, documents, chunk_size=100):
    """
    批量发送文档到Elasticsearch
    """
    if not documents:
        return {'success': 0, 'failed': 0}
    
    try:
        # 使用bulk API批量索引
        success_count = 0
        failed_count = 0
        
        for i in range(0, len(documents), chunk_size):
            chunk = documents[i:i + chunk_size]
            try:
                response = bulk(client, chunk, refresh=True)
                success_count += len(chunk)
            except Exception as e:
                print(f"⚠️ 批量索引失败 (chunk {i//chunk_size + 1}): {e}")
                failed_count += len(chunk)
        
        return {'success': success_count, 'failed': failed_count}
    
    except Exception as e:
        print(f"⚠️ Elasticsearch索引失败: {e}")
        return {'success': 0, 'failed': len(documents)}

def main():
    # 跨平台多进程兼容性修复
    import multiprocessing
    import platform
    
    # 根据操作系统设置合适的多进程启动方法
    if platform.system() == 'Windows':
        # Windows下使用spawn方法避免KeyboardInterrupt问题
        if hasattr(multiprocessing, 'set_start_method'):
            try:
                multiprocessing.set_start_method('spawn', force=True)
            except RuntimeError:
                pass  # 已经设置过了
    else:
        # Linux/macOS下使用fork方法（性能更好）
        if hasattr(multiprocessing, 'set_start_method'):
            try:
                multiprocessing.set_start_method('fork', force=True)
            except (RuntimeError, OSError):
                # 如果fork不可用，回退到spawn
                try:
                    multiprocessing.set_start_method('spawn', force=True)
                except RuntimeError:
                    pass
    
    ap = argparse.ArgumentParser(description="Parallel MySQL slow log aggregator (robust v2)")
    ap.add_argument("logfile", help="path to MySQL slow log")
    ap.add_argument("--out-csv", default="slowlog_summary.csv")
    ap.add_argument("--out-md", default=None)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--lang", default="zh", choices=["zh","en"])
    ap.add_argument("--min-time", type=float, default=0.0)
    ap.add_argument("--exclude-dumps", action="store_true")
    ap.add_argument("--jobs", type=int, default=cpu_count(), help="workers (default = CPU cores)")
    ap.add_argument("--loose-start", action="store_true", help="treat '# Query_time:' as a valid start when '# Time:' missing")
    ap.add_argument("--mark-truncated", action="store_true", help="append /* TRUNCATED */ to example/norm SQL when tail-truncated")
    ap.add_argument("--stats", action="store_true", help="print processing stats and timings")
    
    # Time filtering options
    time_group = ap.add_mutually_exclusive_group()
    time_group.add_argument("--days", type=int, help="analyze last N days (0=today only, 1=last 1 day, 7=last 7 days, etc.)")
    time_group.add_argument("--today", action="store_true", help="analyze today only (same as --days 0)")
    time_group.add_argument("--all", action="store_true", help="analyze all records (default, no time filtering)")
    
    # Elasticsearch integration options
    es_group = ap.add_argument_group('Elasticsearch Integration')
    es_group.add_argument("--es-host", help="Elasticsearch host (e.g., http://localhost:9200)")
    es_group.add_argument("--es-index", default="mysql-slowlog-%{+yyyy.MM.dd}", 
                         help="Elasticsearch index pattern (default: mysql-slowlog-%%{+yyyy.MM.dd})")
    es_group.add_argument("--es-user", help="Elasticsearch username")
    es_group.add_argument("--es-password", help="Elasticsearch password")
    es_group.add_argument("--es-ca-certs", help="Path to CA certificate file")
    es_group.add_argument("--es-no-verify-certs", action="store_true", help="Disable SSL certificate verification")
    es_group.add_argument("--es-hostname", help="Hostname to include in ES documents (default: system hostname)")
    
    args = ap.parse_args()
    
    # Elasticsearch configuration validation
    if args.es_host:
        print("📡 检测到Elasticsearch配置...")
        
        # Check if elasticsearch package is available
        if not ES_AVAILABLE:
            print("❌ 错误: Elasticsearch功能需要安装elasticsearch包")
            print("   安装命令: pip install elasticsearch")
            print("   或者: pip install elasticsearch[async]")
            sys.exit(1)
        
        # Configuration recommendations
        print("🔧 Elasticsearch配置检查:")
        print(f"   连接地址: {args.es_host}")
        print(f"   索引模式: {args.es_index}")
        
        # Check authentication
        if args.es_user and args.es_password:
            print(f"   认证用户: {args.es_user}")
            print("   认证密码: *** (已配置)")
        else:
            print("   认证方式: 无认证")
            if "https://" in args.es_host.lower():
                print("   ⚠️  建议: HTTPS连接通常需要配置用户名密码")
                print("   使用: --es-user <用户名> --es-password <密码>")
        
        # Check SSL settings
        if args.es_ca_certs:
            print(f"   SSL证书: {args.es_ca_certs}")
        elif "https://" in args.es_host.lower():
            print("   SSL验证: 默认开启")
            if args.es_no_verify_certs:
                print("   ⚠️  警告: SSL证书验证已禁用")
            else:
                print("   💡 提示: 如遇SSL证书问题，可使用 --es-no-verify-certs")
        
        # Hostname for documents
        hostname = args.es_hostname or (os.uname().nodename if hasattr(os, 'uname') else 'unknown')
        print(f"   文档主机: {hostname}")
        
        print()
    
    # Handle time filtering arguments
    time_range = None
    if args.today or args.days == 0:
        time_range = calculate_time_range(0)
        print(f"时间过滤: 仅今天 ({time_range[0].strftime('%Y-%m-%d')})")
    elif args.days is not None and args.days > 0:
        time_range = calculate_time_range(args.days)
        print(f"时间过滤: 最近 {args.days} 天 ({time_range[0].strftime('%Y-%m-%d')} 至 {time_range[1].strftime('%Y-%m-%d')})")
    else:
        print("时间过滤: 全部记录 (无过滤)")

    t0 = time.perf_counter()
    
    # 智能时间范围预检查（专家级优化）
    if time_range:
        print("🔍 智能采样检查时间范围...")
        sample_result = smart_time_range_check(args.logfile, time_range)
        
        if not sample_result['has_data_in_range']:
            print(f"⚠️  基于采样分析，指定时间范围内未发现数据")
            if 'file_time_range' in sample_result and sample_result['file_time_range'][0]:
                file_start, file_end = sample_result['file_time_range']
                print(f"   文件时间范围: {file_start.strftime('%Y-%m-%d %H:%M')} 至 {file_end.strftime('%Y-%m-%d %H:%M')}")
            target_start, target_end = time_range
            print(f"   目标时间范围: {target_start.strftime('%Y-%m-%d %H:%M')} 至 {target_end.strftime('%Y-%m-%d %H:%M')}")
            print(f"   已采样 {sample_result.get('sample_count', 0)} 个时间戳")
            return
        
        coverage = sample_result['estimated_coverage']
        coverage_type = sample_result.get('coverage_type', 'unknown')
        
        # 根据覆盖率类型给出更准确的提示
        if coverage_type == "full_file_in_range":
            print(f"✅ 文件数据完全在目标时间范围内（覆盖率: 100%）")
        elif coverage_type == "full_range_covered":
            print(f"✅ 目标时间范围完全被文件覆盖（覆盖率: 100%）")
        elif coverage_type == "mostly_covered":
            print(f"✅ 在时间范围内发现充足数据（预估覆盖率: {coverage*100:.0f}%）")
        elif coverage_type == "partially_covered":
            print(f"✅ 在时间范围内发现部分数据（预估覆盖率: {coverage*100:.0f}%）")
        else:
            print(f"✅ 在时间范围内发现数据（预估覆盖率: {coverage*100:.1f}%）")
            if coverage < 0.3:
                print(f"💡 提示: 文件数据时间跨度较小，大部分目标时间范围可能无数据")
    
    # Lightweight boundary scan (no time filtering at this stage)
    shards, record_starts, file_size = compute_boundaries(args.logfile, args.jobs, args.loose_start)
    t_scan = time.perf_counter()

    job_args = [(args.logfile, s, e, args.min_time, args.exclude_dumps, args.mark_truncated, args.loose_start, time_range) for (s,e) in shards]
    with Pool(processes=min(args.jobs, len(job_args))) as pool:
        parts = pool.map(parse_chunk, job_args)
    t_parse = time.perf_counter()

    merged, stats_total = merge_results(parts)
    t_merge = time.perf_counter()

    # 检查过滤后是否有数据
    if not merged or len(merged) == 0:
        print("\n⚠️  在指定时间范围和条件下未找到慢查询")
        if time_range:
            start_date = time_range[0].strftime('%Y-%m-%d')
            end_date = time_range[1].strftime('%Y-%m-%d')
            print(f"   时间范围: {start_date} 至 {end_date}")
        print(f"   最小耗时阈值: {args.min_time}秒")
        if args.stats:
            print(f"\n[统计信息] ==========")
            print(f"文件大小         : {file_size} bytes")
            print(f"记录起始点       : {record_starts}")
            print(f"解析记录数       : {stats_total.get('parsed_records',0)}")
            print(f"过滤<最小耗时    : {stats_total.get('filtered_min_time',0)}")
            print(f"过滤dumps查询    : {stats_total.get('filtered_dumps',0)}")
            print(f"过滤时间范围     : {stats_total.get('filtered_time_range',0)}")
            print(f"最终剩余         : 0")
        return

    df = build_dataframe(merged)
    t_build = time.perf_counter()

    out_df = rename_columns(df, args.lang)
    out_df.to_csv(args.out_csv, index=False, encoding="utf-8")
    print("已保存 CSV:", args.out_csv)
    if args.out_md:
        write_markdown(df, args.out_md, args.top)
        print("已保存 Markdown:", args.out_md)
    
    # Elasticsearch integration
    es_result = None
    if args.es_host:
        print("📡 正在发送数据到Elasticsearch...")
        try:
            # 创建ES客户端
            es_client = create_es_client(
                es_hosts=args.es_host,
                es_user=args.es_user,
                es_password=args.es_password,
                es_ca_certs=args.es_ca_certs,
                es_verify_certs=not args.es_no_verify_certs
            )
            
            if es_client:
                # 准备文档
                hostname = args.es_hostname or (os.uname().nodename if hasattr(os, 'uname') else 'unknown')
                documents = prepare_es_documents(df, args.es_index, hostname, args.logfile)
                
                if documents:
                    # 发送到ES
                    es_result = send_to_elasticsearch(es_client, documents)
                    
                    if es_result['success'] > 0:
                        index_name = documents[0]['_index']
                        print(f"✅ 成功发送 {es_result['success']} 条记录到 Elasticsearch")
                        print(f"   索引: {index_name}")
                        print(f"   主机: {hostname}")
                        print(f"   文件: {args.logfile}")
                        print(f"   时间戳: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    if es_result['failed'] > 0:
                        print(f"⚠️ {es_result['failed']} 条记录发送失败")
                else:
                    print("⚠️ 无数据发送到Elasticsearch")
            
        except Exception as e:
            print(f"⚠️ Elasticsearch集成失败: {e}")
            print("   💡 请检查:")
            print("     - ES服务是否运行")
            print("     - 网络连接是否正常")
            print("     - 用户名密码是否正确")
            print("     - SSL证书配置是否正确")
    
    t_write = time.perf_counter()

    if args.stats:
        total_time = t_write - t0
        print("\n[统计信息] ==========")
        print(f"文件大小         : {file_size} bytes")
        print(f"记录起始点       : {record_starts}")
        print(f"分片数           : {len(shards)}; 使用工作进程: {min(args.jobs, len(job_args))}")
        print(f"Time 行数        : {stats_total.get('time_lines',0)}")
        print(f"Query_time 行数  : {stats_total.get('qtime_lines',0)}")
        print(f"解析记录数       : {stats_total.get('parsed_records',0)}")
        print(f"过滤<最小耗时    : {stats_total.get('filtered_min_time',0)}")
        print(f"过滤dumps查询    : {stats_total.get('filtered_dumps',0)}")
        print(f"过滤时间范围     : {stats_total.get('filtered_time_range',0)}")
        print(f"尾部截断         : {stats_total.get('truncated_records',0)}")
        if df is not None and not df.empty:
            print(f"指纹数量         : {len(df)}")
            print(f"样本总数         : {int(df['samples'].sum())}")
            print(f"总耗时 (秒)      : {float(df['total_time_s'].sum()):.3f}")
        
        # ES统计信息
        if es_result:
            print(f"ES发送成功       : {es_result['success']}")
            print(f"ES发送失败       : {es_result['failed']}")
        
        print("[耗时统计] ==========")
        print(f"边界扫描         : {(t_scan - t0)*1000:.1f} ms")
        print(f"并行解析         : {(t_parse - t_scan):.3f} s")
        print(f"结果合并         : {(t_merge - t_parse)*1000:.1f} ms")
        print(f"构建DataFrame    : {(t_build - t_merge)*1000:.1f} ms")
        print(f"写出文件         : {(t_write - t_build)*1000:.1f} ms")
        print(f"总计             : {total_time:.3f} s")

if __name__ == "__main__":
    # 这个保护对Windows多进程非常重要
    main()
