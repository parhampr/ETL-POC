#!/usr/bin/env python3
"""
DocETL Pipeline Runner Script
============================

This script provides a standalone way to run DocETL pipelines outside of Airflow
for testing and development purposes.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional

import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """Load DocETL configuration from YAML file."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        raise


def update_config_paths(config: Dict[str, Any], input_dir: str, output_dir: str) -> Dict[str, Any]:
    """Update file paths in configuration."""
    try:
        # Update dataset paths
        if 'datasets' in config:
            for dataset_name, dataset_config in config['datasets'].items():
                if 'path' in dataset_config:
                    # Handle both string and list paths
                    if isinstance(dataset_config['path'], str):
                        if dataset_config['path'].startswith('/data/input'):
                            dataset_config['path'] = dataset_config['path'].replace('/data/input', input_dir)
                    elif isinstance(dataset_config['path'], list):
                        updated_paths = []
                        for path in dataset_config['path']:
                            if path.startswith('/data/input'):
                                updated_paths.append(path.replace('/data/input', input_dir))
                            else:
                                updated_paths.append(path)
                        dataset_config['path'] = updated_paths

        logger.info(f"Updated configuration paths - input: {input_dir}, output: {output_dir}")
        return config

    except Exception as e:
        logger.error(f"Error updating config paths: {str(e)}")
        raise


def run_docetl_pipeline(config: Dict[str, Any], output_path: str) -> Dict[str, Any]:
    """Run DocETL pipeline with given configuration."""
    try:
        import subprocess
        import tempfile

        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f, default_flow_style=False)
            temp_config_path = f.name

        try:
            # Run DocETL
            cmd = [sys.executable, '-m', 'docetl.cli', 'run', temp_config_path]
            if output_path:
                cmd.extend(['--output', output_path])

            logger.info(f"Running command: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )

            if result.returncode != 0:
                error_msg = f"DocETL failed with return code {result.returncode}"
                if result.stderr:
                    error_msg += f"\nStderr: {result.stderr}"
                if result.stdout:
                    error_msg += f"\nStdout: {result.stdout}"
                raise Exception(error_msg)

            logger.info("DocETL pipeline completed successfully")

            return {
                'success': True,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr
            }

        finally:
            # Clean up temp file
            os.unlink(temp_config_path)

    except subprocess.TimeoutExpired:
        logger.error("DocETL pipeline timed out after 1 hour")
        raise Exception("Pipeline execution timed out")
    except Exception as e:
        logger.error(f"Error running DocETL pipeline: {str(e)}")
        raise


def process_results(output_path: str) -> Optional[Dict[str, Any]]:
    """Load and process pipeline results."""
    try:
        if os.path.exists(output_path):
            with open(output_path, 'r', encoding='utf-8') as f:
                results = json.load(f)

            # Add processing statistics
            stats = {
                'total_items': len(results) if isinstance(results, list) else 1,
                'output_file_size': os.path.getsize(output_path),
                'timestamp': datetime.now().isoformat()
            }

            if isinstance(results, list):
                # Analyze articles if present
                articles_with_scores = [item for item in results if 'quality_score' in item]
                if articles_with_scores:
                    scores = [item['quality_score'] for item in articles_with_scores]
                    stats.update({
                        'articles_with_scores': len(articles_with_scores),
                        'average_quality_score': sum(scores) / len(scores),
                        'high_quality_articles': len([s for s in scores if s >= 0.7])
                    })

            logger.info(f"Results processed: {stats}")
            return {'results': results, 'stats': stats}
        else:
            logger.warning(f"Output file not found: {output_path}")
            return None

    except Exception as e:
        logger.error(f"Error processing results: {str(e)}")
        return None


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Run DocETL pipeline for Zara ETL POC')
    parser.add_argument('--config',
                        default='/app/docetl/configs/paper_extraction.yaml',
                        help='Path to DocETL configuration file')
    parser.add_argument('--input',
                        default='/data/input',
                        help='Input directory containing PDF files')
    parser.add_argument('--output',
                        default='/data/processed/docetl_output.json',
                        help='Output file path for results')
    parser.add_argument('--summary',
                        default='/data/processed/pipeline_summary.json',
                        help='Summary file path')
    parser.add_argument('--verbose', '-v',
                        action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        logger.info("Starting DocETL pipeline execution")
        logger.info(f"Config: {args.config}")
        logger.info(f"Input: {args.input}")
        logger.info(f"Output: {args.output}")

        # Load and update configuration
        config = load_config(args.config)
        config = update_config_paths(config, args.input, os.path.dirname(args.output))

        # Create output directory
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        os.makedirs(os.path.dirname(args.summary), exist_ok=True)

        # Run pipeline
        execution_start = datetime.now()
        pipeline_result = run_docetl_pipeline(config, args.output)
        execution_time = (datetime.now() - execution_start).total_seconds()

        # Process results
        processed_results = process_results(args.output)

        # Create summary
        summary = {
            'execution_info': {
                'start_time': execution_start.isoformat(),
                'execution_time_seconds': execution_time,
                'success': pipeline_result.get('success', False),
                'config_file': args.config,
                'input_directory': args.input,
                'output_file': args.output
            },
            'pipeline_result': pipeline_result,
            'processed_results': processed_results
        }

        # Save summary
        with open(args.summary, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        logger.info(f"Pipeline completed successfully in {execution_time:.2f} seconds")
        logger.info(f"Results saved to: {args.output}")
        logger.info(f"Summary saved to: {args.summary}")

        # Print key statistics
        if processed_results and 'stats' in processed_results:
            stats = processed_results['stats']
            print(f"\n📊 Pipeline Statistics:")
            print(f"   Total items processed: {stats.get('total_items', 0)}")
            print(f"   Articles with quality scores: {stats.get('articles_with_scores', 0)}")
            print(f"   Average quality score: {stats.get('average_quality_score', 0):.3f}")
            print(f"   High-quality articles: {stats.get('high_quality_articles', 0)}")
            print(f"   Processing time: {execution_time:.2f} seconds")

        return 0

    except KeyboardInterrupt:
        logger.info("Pipeline execution interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())