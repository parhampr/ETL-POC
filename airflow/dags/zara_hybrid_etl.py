import json, logging, os, re
from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "zara-etl",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

ARXIV_MAX_RESULTS = int(os.getenv("ARXIV_MAX_RESULTS", 10))
ARXIV_CATEGORIES = [c.strip() for c in os.getenv("ARXIV_CATEGORIES", "cs.AI,cs.CL").split(",")]
QUALITY_THRESHOLD = float(os.getenv("QUALITY_THRESHOLD", 0.7))
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "gpt-4o-mini")

BASE = "/opt/airflow/data"
PROC = f"{BASE}/processed"
OUT = f"{BASE}/output"
DOCETL_JSON = f"{PROC}/docetl_output.json"
DOCETL_INT = f"{PROC}/intermediate"
TPL_DIR = "/opt/airflow/docetl/config"
TPL_NAME = "article_pipeline.yaml.j2"

# ---------- utils ----------

def parse_docetl_costs(lines: List[str]) -> Dict[str, Any]:
    op_cost_re = re.compile(r"✓\s+([A-Za-z0-9_/\-\.]+)\s+\(Cost:\s*\$([0-9.]+)\)")
    summary_cost_re = re.compile(r"\bCost:\s*\$\s*([0-9.]+)\b")
    time_re = re.compile(r"\bTime:\s*([0-9.]+)s\b")
    cache_re = re.compile(r"\bCache:\s*(\S+)")
    output_re = re.compile(r"\bOutput:\s*(\S+)")

    ops = {}
    total_cost = None
    total_time = None
    cache_dir = None
    output_path = None

    for ln in lines:
        m = op_cost_re.search(ln)
        if m:
            ops[m.group(1).strip()] = float(m.group(2))
        m = summary_cost_re.search(ln)
        if m:
            total_cost = float(m.group(1))
        m = time_re.search(ln)
        if m:
            total_time = float(m.group(1))
        m = cache_re.search(ln)
        if m:
            cache_dir = m.group(1)
        m = output_re.search(ln)
        if m:
            output_path = m.group(1)

    return {
        "total_cost_usd": total_cost if total_cost is not None else sum(ops.values()),
        "total_time_s": total_time or 0.0,
        "operations": ops,
        "cache_dir": cache_dir,
        "output_path": output_path,
    }

def slugify(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower())
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "untitled"

def load_and_normalize_docetl_output(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        logger.warning("DocETL output not found: %s", path); return {"articles": [], "processing_stats": {}}
    except Exception as e:
        logger.error("Failed to read DocETL output: %s", e); return {"articles": [], "processing_stats": {}}
    if isinstance(raw, list):
        return {"articles": raw, "processing_stats": {}}
    if isinstance(raw, dict):
        return {"articles": raw.get("articles") or raw.get("data") or [], "processing_stats": raw.get("processing_stats", {})}
    return {"articles": [], "processing_stats": {}}

def write_article_markdown(article: Dict[str, Any], folder: str) -> str:
    import yaml
    os.makedirs(folder, exist_ok=True)

    fm = {
        "title": article.get("headline", "") or article.get("title", ""),
        "subtitle": article.get("subtitle", ""),
        "tags": article.get("topic_tags", []) or [],
        "authors": article.get("authors", []) or [],
        "arxiv_id": article.get("arxiv_id", ""),
        "word_count": article.get("word_count", 0),
        "meta_description": article.get("meta_description", ""),
        "created": datetime.utcnow().isoformat() + "Z",
    }

    front = "---\n" + yaml.safe_dump(
        fm, sort_keys=False, allow_unicode=True
    ) + "---\n\n"

    body = article.get("article_body", "") or ""
    if article.get("pull_quotes"):
        body += "\n\n> " + "\n> ".join(article["pull_quotes"])
    if article.get("key_takeaways"):
        body += "\n\n## What This Means\n" + "\n".join(f"- {t}" for t in article["key_takeaways"])

    md_path = os.path.join(folder, "article.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(front + body)

    with open(os.path.join(folder, "article.json"), "w", encoding="utf-8") as f:
        json.dump(article, f, indent=2, ensure_ascii=False)

    return md_path

# ---------- tasks ----------
def extract_arxiv_papers(**context) -> Dict[str, Any]:
    logger.info("Extracting %d papers from %s", ARXIV_MAX_RESULTS, ARXIV_CATEGORIES)
    try:
        from plugins.arxiv_hook import ArxivHook
    except ImportError:
        import sys; sys.path.append("/opt/airflow/plugins"); from arxiv_hook import ArxivHook  # type: ignore
    hook = ArxivHook()
    papers: List[Dict[str, Any]] = []
    for cat in ARXIV_CATEGORIES:
        try:
            papers.extend(hook.search_papers(
                query=f"cat:{cat}",
                max_results=max(1, ARXIV_MAX_RESULTS // max(1, len(ARXIV_CATEGORIES))),
                sort_by="submittedDate", sort_order="descending"
            ))
        except Exception as e:
            logger.error("arXiv fetch error %s: %s", cat, e)
    downloaded = []
    for p in papers[:ARXIV_MAX_RESULTS]:
        try:
            path = hook.download_paper(p)
            if path: p["pdf_path"] = path; downloaded.append(p)
        except Exception as e:
            logger.warning("download failed: %s", e)
    os.makedirs(PROC, exist_ok=True)
    with open(f"{PROC}/arxiv_paths.json", "w", encoding="utf-8") as f:
        json.dump([
            {
                "arxiv_id": p.get("id") or p.get("arxiv_id"),
                "title": p.get("title"),
                "authors": p.get("authors", []),
                "pdf_path": p["pdf_path"],
            }
            for p in downloaded
        ], f, indent=2, ensure_ascii=False)
    return {"total_papers": len(downloaded), "dataset_json": f"{PROC}/arxiv_paths.json"}

def prepare_docetl_config(**context) -> str:
    ti = context["task_instance"]
    payload = ti.xcom_pull(task_ids="data_ingestion.extract_arxiv_papers") or {}
    dataset_json = payload.get("dataset_json")
    if not dataset_json:
        raise ValueError("dataset_json missing")
    os.makedirs(PROC, exist_ok=True)
    os.makedirs(DOCETL_INT, exist_ok=True)
    env = Environment(
        loader=FileSystemLoader(TPL_DIR),
        autoescape=False,
        # use [[ ... ]] for OUR variables
        variable_start_string="[[",
        variable_end_string="]]",
    )
    tpl = env.get_template(TPL_NAME)
    rendered = tpl.render(
        default_model=DEFAULT_MODEL,
        dataset_json=dataset_json,
        output_json=DOCETL_JSON,
        intermediate_dir=DOCETL_INT,
    )
    conf_path = f"{PROC}/docetl_config.yaml"
    with open(conf_path, "w", encoding="utf-8") as f:
        f.write(rendered)
    return conf_path

def process_with_docetl(**context) -> Dict[str, Any]:
    import subprocess, sys
    log = logging.getLogger("airflow.task")
    conf = context["task_instance"].xcom_pull(task_ids="docetl_processing.prepare_docetl_config")
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    # Build optimized pipeline then run it
    build = [sys.executable, "-m", "docetl.cli", "build", conf]
    run = [sys.executable, "-m", "docetl.cli", "run", f"{os.path.splitext(conf)[0]}_opt.yaml"]
    for cmd in (build, run):
        log.info("Running: %s", " ".join(cmd))
        proc = subprocess.Popen(cmd, cwd="/opt/airflow", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
        for line in proc.stdout: log.info("[docetl] %s", line.rstrip())
        rc = proc.wait()
        if rc != 0: raise RuntimeError(f"DocETL command failed {cmd} rc={rc}")
    norm = load_and_normalize_docetl_output(DOCETL_JSON)
    return {"output_path": DOCETL_JSON, "articles_generated": len(norm["articles"])}

def calculate_quality_score(article: Dict[str, Any]) -> float:
    score=maxs=0.0
    h=article.get("headline",""); 
    if h: maxs+=0.2; score+=0.2 if 10<=len(h)<=60 else (0.1 if h else 0.0)
    body=article.get("article_body",""); wc=len(body.split()) if body else 0
    maxs+=0.2; score+=0.2 if 700<=wc<=1000 else (0.15 if 500<=wc<=1200 else (0.1 if wc>200 else 0))
    req=["headline","subtitle","article_body","meta_description"]
    maxs+=0.3; score+=(sum(1 for k in req if str(article.get(k,"")).strip())/len(req))*0.3
    pq=article.get("pull_quotes",[]); maxs+=0.15; score+=0.15 if isinstance(pq,list) and len(pq)>=2 else (0.1 if isinstance(pq,list) and len(pq)>=1 else 0)
    kt=article.get("key_takeaways",[]); maxs+=0.15; score+=0.15 if isinstance(kt,list) and len(kt)>=3 else (0.1 if isinstance(kt,list) and len(kt)>=1 else 0)
    return score/maxs if maxs>0 else 0.0

def validate_article_quality(**context) -> Dict[str, Any]:
    meta = context["task_instance"].xcom_pull(task_ids="docetl_processing.process_with_docetl") or {}
    data = load_and_normalize_docetl_output(meta.get("output_path", DOCETL_JSON))
    articles = data["articles"]
    if not articles: return {"high_quality_path": None, "failed_path": None, "stats": {}}
    hi, lo = [], []
    for a in articles:
        q = calculate_quality_score(a); a["quality_score"]=q
        (hi if q>=QUALITY_THRESHOLD else lo).append(a)
    stats = {
        "total_articles": len(articles),
        "passed_quality": len(hi),
        "failed_quality": len(lo),
        "pass_rate": len(hi)/len(articles) if articles else 0,
        "avg_quality": sum(x.get("quality_score",0) for x in articles)/len(articles) if articles else 0,
    }
    os.makedirs(OUT, exist_ok=True); ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    hp=f"{OUT}/articles_high_quality_{ts}.json"; fp=f"{OUT}/articles_failed_quality_{ts}.json"
    with open(hp,"w",encoding="utf-8") as f: json.dump(hi,f,indent=2,ensure_ascii=False)
    with open(fp,"w",encoding="utf-8") as f: json.dump(lo,f,indent=2,ensure_ascii=False)
    return {"high_quality_path": hp, "failed_path": fp, "stats": stats, "timestamp": ts}

def materialize_articles(**context) -> Dict[str, Any]:
    meta = context["task_instance"].xcom_pull(task_ids="quality_control.validate_article_quality") or {}
    hp = meta.get("high_quality_path")
    if not hp or not os.path.exists(hp): return {"article_dirs": [], "written": []}
    articles = json.load(open(hp, "r", encoding="utf-8"))
    written, dirs = [], []
    for a in articles:
        base_slug = slugify(a.get("headline") or a.get("title") or a.get("arxiv_id",""))
        adir = os.path.join(OUT, "articles", base_slug)
        path = write_article_markdown(a, adir)
        written.append(path); dirs.append(adir)
    return {"article_dirs": dirs, "written": written}

def save_final_outputs(**context) -> Dict[str, Any]:
    ti = context["task_instance"]
    ingestion = ti.xcom_pull(task_ids="data_ingestion.extract_arxiv_papers") or {}
    process_meta = ti.xcom_pull(task_ids="docetl_processing.process_with_docetl") or {}
    quality_meta = ti.xcom_pull(task_ids="quality_control.validate_article_quality") or {}
    material = ti.xcom_pull(task_ids="output_preparation.materialize_articles") or {}
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary = {
        "timestamp": ts,
        "total_papers_processed": ingestion.get("total_papers", 0),
        "articles_generated": process_meta.get("articles_generated", 0),
        "quality_stats": quality_meta.get("stats", {}),
        "artifacts": {
            "docetl_output_path": process_meta.get("output_path", DOCETL_JSON),
            "high_quality_path": quality_meta.get("high_quality_path"),
            "failed_path": quality_meta.get("failed_path"),
            "article_dirs": material.get("article_dirs", []),
            "written_markdown": material.get("written", []),
        },
        "config": {
            "arxiv_categories": ARXIV_CATEGORIES,
            "quality_threshold": QUALITY_THRESHOLD,
            "default_model": DEFAULT_MODEL,
        },
    }
    os.makedirs(OUT, exist_ok=True)
    sp = f"{OUT}/pipeline_summary_{ts}.json"
    with open(sp, "w", encoding="utf-8") as f: json.dump(summary, f, indent=2, ensure_ascii=False)
    return {"saved_files": [sp] + material.get("written", []), "summary_path": sp}

# ---------- DAG ----------
with DAG(
    "zara_hybrid_etl",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    tags=["zara","etl","docetl","arxiv","llm"],
    doc_md=__doc__,
) as dag:
    with TaskGroup("data_ingestion") as ingestion_group:
        extract_papers = PythonOperator(task_id="extract_arxiv_papers", python_callable=extract_arxiv_papers)

    with TaskGroup("docetl_processing") as processing_group:
        prepare_config = PythonOperator(task_id="prepare_docetl_config", python_callable=prepare_docetl_config)
        docetl_process = PythonOperator(task_id="process_with_docetl", python_callable=process_with_docetl)
        prepare_config >> docetl_process

    with TaskGroup("quality_control") as quality_group:
        validate_quality = PythonOperator(task_id="validate_article_quality", python_callable=validate_article_quality)

    with TaskGroup("output_preparation") as output_group:
        materialize = PythonOperator(task_id="materialize_articles", python_callable=materialize_articles)
        save_outputs = PythonOperator(task_id="save_final_outputs", python_callable=save_final_outputs)
        materialize >> save_outputs

    health_check = BashOperator(task_id="health_check", bash_command="echo ok")
    health_check >> ingestion_group >> processing_group >> quality_group >> output_group
