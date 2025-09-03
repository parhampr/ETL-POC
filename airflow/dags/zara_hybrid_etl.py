"""
Zara Hybrid ETL Pipeline - Airflow + DocETL
============================================

This DAG orchestrates the complete pipeline for automated scientific article generation:
1. arXiv paper ingestion
2. DocETL document processing and article generation
3. Quality validation and scoring
4. Output preparation for publishing

Author: Zara ETL Team
Date: 2025
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any

# Airflow 3.0 compatible imports
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Import custom plugins - these should be available at runtime
# from plugins.arxiv_hook import ArxivHook

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

DEFAULT_ARGS = {
    'owner': 'zara-etl',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Get configuration from environment
ARXIV_MAX_RESULTS = int(os.getenv('ARXIV_MAX_RESULTS', 10))
ARXIV_CATEGORIES = os.getenv('ARXIV_CATEGORIES', 'cs.AI,cs.CL').split(',')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 5))
QUALITY_THRESHOLD = float(os.getenv('QUALITY_THRESHOLD', 0.7))

# =============================================================================
# Task Functions
# =============================================================================

def extract_arxiv_papers(**context) -> Dict[str, Any]:
    """
    Extract recent papers from arXiv API based on configured categories.

    Returns:
        Dict containing paper metadata and file paths
    """
    logger.info(f"Extracting {ARXIV_MAX_RESULTS} papers from categories: {ARXIV_CATEGORIES}")

    # Import arxiv hook locally to avoid top-level imports in Airflow 3.0
    try:
        from plugins.arxiv_hook import ArxivHook
    except ImportError:
        # Fallback import path for custom plugins
        import sys
        sys.path.append('/opt/airflow/plugins')
        from arxiv_hook import ArxivHook

    hook = ArxivHook()
    papers = []

    for category in ARXIV_CATEGORIES:
        try:
            category_papers = hook.search_papers(
                query=f"cat:{category}",
                max_results=ARXIV_MAX_RESULTS // len(ARXIV_CATEGORIES),
                sort_by="submittedDate",
                sort_order="descending"
            )
            papers.extend(category_papers)
            logger.info(f"Retrieved {len(category_papers)} papers from {category}")

        except Exception as e:
            logger.error(f"Error retrieving papers from {category}: {str(e)}")
            continue

    # Download PDFs
    downloaded_papers = []
    for paper in papers[:ARXIV_MAX_RESULTS]:
        try:
            pdf_path = hook.download_paper(paper)
            if pdf_path:
                paper['pdf_path'] = pdf_path
                downloaded_papers.append(paper)
                logger.info(f"Downloaded: {paper['title'][:50]}...")
        except Exception as e:
            logger.warning(f"Failed to download {paper.get('title', 'Unknown')}: {str(e)}")
            continue

    result = {
        'total_papers': len(downloaded_papers),
        'papers': downloaded_papers,
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Successfully extracted and downloaded {len(downloaded_papers)} papers")
    return result


def prepare_docetl_config(**context) -> str:
    """
    Prepare DocETL configuration based on downloaded papers.

    Returns:
        Path to generated DocETL config file
    """
    # Access XCom data properly for Airflow 3.0
    task_instance = context['task_instance']
    papers_data = task_instance.xcom_pull(task_ids='data_ingestion.extract_arxiv_papers')

    if not papers_data or not papers_data.get('papers'):
        raise ValueError("No papers available for processing")

    # Create DocETL configuration
    config = {
        'default_model': os.getenv('DEFAULT_MODEL', 'gpt-4o-mini'),
        'datasets': {
            'arxiv_papers': {
                'type': 'file',
                'source': 'local',
                'path': [paper['pdf_path'] for paper in papers_data['papers']],
                'parsing_tools': [
                    {
                        'input_key': 'pdf_path',
                        'function': 'extract_pdf_text',
                        'output_key': 'full_text'
                    }
                ]
            }
        },
        'operations': [
            {
                'name': 'extract_paper_content',
                'type': 'map',
                'prompt': '''Extract structured information from this research paper:
                
                1. Title and authors
                2. Abstract summary (2-3 sentences)
                3. Main research question/hypothesis
                4. Methodology approach
                5. Key findings and results
                6. Significance and implications
                7. Limitations mentioned
                
                Return structured JSON with these fields.''',
                'optimize': True,
                'output_schema': {
                    'title': 'string',
                    'authors': 'list',
                    'abstract_summary': 'string',
                    'research_question': 'string',
                    'methodology': 'string',
                    'key_findings': 'string',
                    'significance': 'string',
                    'limitations': 'string',
                    'arxiv_id': 'string'
                }
            },
            {
                'name': 'generate_news_article',
                'type': 'map',
                'prompt': '''Transform this research summary into an engaging news article for a general audience:

                REQUIREMENTS:
                - Compelling headline (under 60 characters, SEO-optimized)
                - Engaging subtitle/dek (under 150 characters)
                - 800-word article accessible to non-experts
                - Include 2-3 pull quotes highlighting key insights
                - Explain technical concepts in simple terms
                - Add "What This Means" section for practical implications
                - Generate meta description for SEO (under 160 characters)
                - Include relevant topic tags

                TONE: Informative, engaging, scientifically accurate but accessible
                STYLE: Journalistic, clear, conversational

                Return structured JSON with all components.''',
                'optimize': True,
                'output_schema': {
                    'headline': 'string',
                    'subtitle': 'string',
                    'article_body': 'string',
                    'pull_quotes': 'list',
                    'meta_description': 'string',
                    'key_takeaways': 'list',
                    'topic_tags': 'list',
                    'word_count': 'integer'
                },
                'validation': [
                    'Is the article factually accurate based on the research?',
                    'Is the language accessible to non-experts?',
                    'Does the headline capture the key finding compellingly?',
                    'Is the article between 700-900 words?'
                ]
            }
        ]
    }

    # Save configuration file
    config_path = '/opt/airflow/data/processed/docetl_config.yaml'
    os.makedirs(os.path.dirname(config_path), exist_ok=True)

    import yaml
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)

    logger.info(f"DocETL configuration saved to {config_path}")
    return config_path


def process_with_docetl(**context) -> Dict[str, Any]:
    """
    Execute DocETL pipeline for document processing and article generation.

    Returns:
        Processing results with generated articles
    """
    task_instance = context['task_instance']
    config_path = task_instance.xcom_pull(task_ids='docetl_processing.prepare_docetl_config')

    logger.info(f"Starting DocETL processing with config: {config_path}")

    try:
        # Import DocETL within the task to avoid container issues
        import subprocess
        import sys

        # Run DocETL pipeline
        result = subprocess.run([
            sys.executable, '-m', 'docetl.cli', 'run', config_path
        ], capture_output=True, text=True, cwd='/opt/airflow')

        if result.returncode != 0:
            logger.error(f"DocETL processing failed: {result.stderr}")
            raise Exception(f"DocETL processing failed: {result.stderr}")

        logger.info("DocETL processing completed successfully")

        # Parse results
        output_path = '/opt/airflow/data/processed/docetl_output.json'
        if os.path.exists(output_path):
            with open(output_path, 'r') as f:
                results = json.load(f)
        else:
            logger.warning("DocETL output file not found, creating mock results")
            results = {"articles": [], "processing_stats": {}}

        return {
            'articles_generated': len(results.get('articles', [])),
            'processing_time': results.get('processing_stats', {}).get('total_time', 0),
            'results': results,
            'timestamp': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error in DocETL processing: {str(e)}")
        raise


def validate_article_quality(**context) -> Dict[str, Any]:
    """
    Validate generated articles and apply quality scoring.

    Returns:
        Quality validation results and filtered articles
    """
    task_instance = context['task_instance']
    docetl_results = task_instance.xcom_pull(task_ids='docetl_processing.process_with_docetl')

    if not docetl_results or not docetl_results.get('results', {}).get('articles'):
        logger.warning("No articles available for quality validation")
        return {'high_quality_articles': [], 'failed_articles': [], 'stats': {}}

    articles = docetl_results['results']['articles']
    high_quality_articles = []
    failed_articles = []

    for article in articles:
        try:
            # Calculate quality score based on multiple criteria
            quality_score = calculate_quality_score(article)
            article['quality_score'] = quality_score

            if quality_score >= QUALITY_THRESHOLD:
                high_quality_articles.append(article)
                logger.info(f"Article passed quality check: {article.get('headline', 'Unknown')[:50]} (score: {quality_score:.2f})")
            else:
                failed_articles.append(article)
                logger.warning(f"Article failed quality check: {article.get('headline', 'Unknown')[:50]} (score: {quality_score:.2f})")

        except Exception as e:
            logger.error(f"Error validating article: {str(e)}")
            article['quality_score'] = 0.0
            failed_articles.append(article)

    stats = {
        'total_articles': len(articles),
        'passed_quality': len(high_quality_articles),
        'failed_quality': len(failed_articles),
        'pass_rate': len(high_quality_articles) / len(articles) if articles else 0,
        'average_quality_score': sum(a.get('quality_score', 0) for a in articles) / len(articles) if articles else 0
    }

    logger.info(f"Quality validation completed: {stats['passed_quality']}/{stats['total_articles']} articles passed (pass rate: {stats['pass_rate']:.1%})")

    return {
        'high_quality_articles': high_quality_articles,
        'failed_articles': failed_articles,
        'stats': stats,
        'timestamp': datetime.now().isoformat()
    }


def calculate_quality_score(article: Dict[str, Any]) -> float:
    """
    Calculate quality score for an article based on multiple criteria.

    Args:
        article: Article data dictionary

    Returns:
        Quality score between 0.0 and 1.0
    """
    score = 0.0
    max_score = 0.0

    # Check headline quality (20% weight)
    headline = article.get('headline', '')
    if headline:
        max_score += 0.2
        if 10 <= len(headline) <= 60:
            score += 0.2
        elif len(headline) > 0:
            score += 0.1

    # Check article body length (20% weight)
    body = article.get('article_body', '')
    word_count = len(body.split()) if body else 0
    max_score += 0.2
    if 700 <= word_count <= 1000:
        score += 0.2
    elif 500 <= word_count <= 1200:
        score += 0.15
    elif word_count > 200:
        score += 0.1

    # Check required fields presence (30% weight)
    required_fields = ['headline', 'subtitle', 'article_body', 'meta_description']
    fields_present = sum(1 for field in required_fields if article.get(field, '').strip())
    max_score += 0.3
    score += (fields_present / len(required_fields)) * 0.3

    # Check pull quotes (15% weight)
    pull_quotes = article.get('pull_quotes', [])
    max_score += 0.15
    if isinstance(pull_quotes, list) and len(pull_quotes) >= 2:
        score += 0.15
    elif isinstance(pull_quotes, list) and len(pull_quotes) >= 1:
        score += 0.1

    # Check key takeaways (15% weight)
    takeaways = article.get('key_takeaways', [])
    max_score += 0.15
    if isinstance(takeaways, list) and len(takeaways) >= 3:
        score += 0.15
    elif isinstance(takeaways, list) and len(takeaways) >= 1:
        score += 0.1

    return score / max_score if max_score > 0 else 0.0


def save_final_outputs(**context) -> Dict[str, Any]:
    """
    Save final processed outputs to file system for external use.

    Returns:
        Summary of saved outputs
    """
    task_instance = context['task_instance']
    quality_results = task_instance.xcom_pull(task_ids='quality_control.validate_article_quality')

    if not quality_results:
        logger.warning("No quality results available for saving")
        return {'saved_files': [], 'summary': {}}

    # Create output directory
    output_dir = '/opt/airflow/data/output'
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    saved_files = []

    # Save high-quality articles
    if quality_results.get('high_quality_articles'):
        articles_file = f"{output_dir}/articles_high_quality_{timestamp}.json"
        with open(articles_file, 'w', encoding='utf-8') as f:
            json.dump(quality_results['high_quality_articles'], f, indent=2, ensure_ascii=False)
        saved_files.append(articles_file)
        logger.info(f"Saved {len(quality_results['high_quality_articles'])} high-quality articles to {articles_file}")

    # Save failed articles for analysis
    if quality_results.get('failed_articles'):
        failed_file = f"{output_dir}/articles_failed_quality_{timestamp}.json"
        with open(failed_file, 'w', encoding='utf-8') as f:
            json.dump(quality_results['failed_articles'], f, indent=2, ensure_ascii=False)
        saved_files.append(failed_file)
        logger.info(f"Saved {len(quality_results['failed_articles'])} failed articles to {failed_file}")

    # Save processing summary
    ingestion_data = task_instance.xcom_pull(task_ids='data_ingestion.extract_arxiv_papers')
    processing_data = task_instance.xcom_pull(task_ids='docetl_processing.process_with_docetl')
    
    summary = {
        'pipeline_run': {
            'timestamp': timestamp,
            'total_papers_processed': ingestion_data.get('total_papers', 0) if ingestion_data else 0,
            'articles_generated': processing_data.get('articles_generated', 0) if processing_data else 0,
            'quality_stats': quality_results.get('stats', {}),
            'configuration': {
                'arxiv_max_results': ARXIV_MAX_RESULTS,
                'arxiv_categories': ARXIV_CATEGORIES,
                'batch_size': BATCH_SIZE,
                'quality_threshold': QUALITY_THRESHOLD
            }
        }
    }

    summary_file = f"{output_dir}/pipeline_summary_{timestamp}.json"
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    saved_files.append(summary_file)

    logger.info(f"Pipeline run completed successfully. Saved {len(saved_files)} output files.")

    return {
        'saved_files': saved_files,
        'summary': summary,
        'timestamp': timestamp
    }


# =============================================================================
# DAG Definition - Fixed for Airflow 3.0
# =============================================================================

dag = DAG(
    'zara_hybrid_etl',
    default_args=DEFAULT_ARGS,
    description='Zara Hybrid ETL Pipeline - Airflow + DocETL for automated scientific article generation',
    # Fixed: Use 'schedule' instead of deprecated 'schedule_interval'
    schedule='@daily',
    max_active_runs=1,
    catchup=False,
    tags=['zara', 'etl', 'docetl', 'arxiv', 'llm'],
    doc_md=__doc__,
)

# =============================================================================
# Task Definitions - Fixed for Airflow 3.0
# =============================================================================

# Data Ingestion Task Group
with TaskGroup("data_ingestion", dag=dag) as ingestion_group:
    extract_papers = PythonOperator(
        task_id='extract_arxiv_papers',
        python_callable=extract_arxiv_papers,
        doc_md="""
        ### arXiv Paper Extraction
        
        Extracts recent papers from arXiv based on configured categories and downloads PDFs.
        
        **Configuration:**
        - Categories: `ARXIV_CATEGORIES`
        - Max results: `ARXIV_MAX_RESULTS`
        - Sort by: submission date (descending)
        """
    )

# DocETL Processing Task Group
with TaskGroup("docetl_processing", dag=dag) as processing_group:
    prepare_config = PythonOperator(
        task_id='prepare_docetl_config',
        python_callable=prepare_docetl_config,
        doc_md="""
        ### DocETL Configuration Preparation
        
        Creates DocETL pipeline configuration based on downloaded papers.
        Includes extraction and article generation operations.
        """
    )

    docetl_process = PythonOperator(
        task_id='process_with_docetl',
        python_callable=process_with_docetl,
        doc_md="""
        ### DocETL Document Processing
        
        Executes DocETL pipeline for:
        - PDF text extraction and chunking
        - Structured content extraction
        - News article generation
        - Built-in validation and optimization
        """
    )

# Quality Control Task Group
with TaskGroup("quality_control", dag=dag) as quality_group:
    validate_quality = PythonOperator(
        task_id='validate_article_quality',
        python_callable=validate_article_quality,
        doc_md="""
        ### Article Quality Validation
        
        Applies quality scoring based on:
        - Content completeness
        - Word count targets  
        - Required field presence
        - Structural elements (quotes, takeaways)
        """
    )

# Output Task Group
with TaskGroup("output_preparation", dag=dag) as output_group:
    save_outputs = PythonOperator(
        task_id='save_final_outputs',
        python_callable=save_final_outputs,
        doc_md="""
        ### Final Output Preparation
        
        Saves processed articles and pipeline summary to file system:
        - High-quality articles (JSON)
        - Failed articles for analysis (JSON)  
        - Pipeline execution summary (JSON)
        """
    )

# Health Check Task - Fixed for Airflow 3.0 reliability
health_check = BashOperator(
    task_id='health_check',
    bash_command='''
    set -e  # Exit on any error
    
    echo "=== Zara ETL Pipeline Health Check ==="
    echo "Timestamp: $(date)"
    echo "Hostname: $(hostname)"
    echo "Working directory: $(pwd)"
    
    # Basic system check
    echo "System info:"
    echo "- Python: $(python --version 2>&1 || echo 'Python not available')"
    echo "- User: $(whoami)"
    
    # Create essential directories with error handling
    echo "Creating data directories..."
    mkdir -p /opt/airflow/data/input || { echo "Failed to create input dir"; exit 1; }
    mkdir -p /opt/airflow/data/processed || { echo "Failed to create processed dir"; exit 1; }
    mkdir -p /opt/airflow/data/output || { echo "Failed to create output dir"; exit 1; }
    mkdir -p /opt/airflow/data/errors || { echo "Failed to create errors dir"; exit 1; }
    echo "✅ Data directories created successfully"
    
    # Test write permissions
    echo "Testing write permissions..."
    touch /opt/airflow/data/test_write.tmp || { echo "No write permission to data directory"; exit 1; }
    rm -f /opt/airflow/data/test_write.tmp
    echo "✅ Write permissions confirmed"
    
    # Basic environment check
    echo "Configuration:"
    echo "- AIRFLOW_HOME: ${AIRFLOW_HOME:-/opt/airflow}"
    echo "- DEFAULT_MODEL: ${DEFAULT_MODEL:-gpt-4o-mini}"
    echo "- ARXIV_MAX_RESULTS: ${ARXIV_MAX_RESULTS:-10}"
    
    echo "=== Health Check Completed Successfully ✅ ==="
    exit 0
    ''',
    dag=dag,
    # Add retries and timeout for reliability
    retries=1,
    retry_delay=timedelta(seconds=30)
)

# =============================================================================
# Task Dependencies - Fixed for Airflow 3.0
# =============================================================================

# Define the pipeline flow - TaskGroups handle their internal dependencies
health_check >> ingestion_group >> processing_group >> quality_group >> output_group