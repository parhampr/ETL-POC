"""
DocETL Operator for Airflow
===========================
"""

from __future__ import annotations

import os
import json
import yaml
import subprocess
from typing import Dict, Any, Optional
from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException


class DocETLOperator(BaseOperator):
    """
    Airflow operator for executing DocETL pipelines.

    This operator allows you to run DocETL configurations directly within
    Airflow DAGs with proper error handling, logging, and result processing.
    """

    template_fields = ("config_path", "output_path")
    template_ext = (".yaml", ".yml")

    def __init__(
            self,
            *,
            config_path: str,
            output_path: Optional[str] = None,
            docetl_command: str = "run",
            optimize: bool = True,
            validate_config: bool = True,
            timeout: int = 3600,  # 1 hour default timeout
            **kwargs: Any,
    ) -> None:
        """
        Initialize DocETL operator.

        Args:
            config_path: Path to DocETL YAML configuration file
            output_path: Path for output files (optional)
            docetl_command: DocETL command to run ('run', 'optimize', 'validate')
            optimize: Whether to run DocETL optimization
            validate_config: Whether to validate config before execution
            timeout: Maximum execution time in seconds
        """
        super().__init__(**kwargs)
        self.config_path = config_path
        self.output_path = output_path or "/opt/airflow/data/processed/docetl_output.json"
        self.docetl_command = docetl_command
        self.optimize = optimize
        self.validate_config = validate_config
        self.timeout = timeout

    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Execute DocETL pipeline.

        Args:
            context: Airflow task context

        Returns:
            Dictionary containing execution results and metadata
        """
        self.log.info("Starting DocETL execution with config: %s", self.config_path)

        # Validate configuration if requested
        if self.validate_config:
            self._validate_configuration()

        # Prepare execution environment
        execution_start = datetime.now()

        try:
            # Execute DocETL pipeline
            result = self._execute_docetl_pipeline()

            # Process and validate results
            processed_results = self._process_results(result)

            execution_time = (datetime.now() - execution_start).total_seconds()

            final_result = {
                "success": True,
                "execution_time": execution_time,
                "config_path": self.config_path,
                "output_path": self.output_path,
                "docetl_results": processed_results,
                "timestamp": execution_start.isoformat(),
                "operator_params": {
                    "command": self.docetl_command,
                    "optimize": self.optimize,
                    "timeout": self.timeout,
                },
            }

            self.log.info("DocETL execution completed successfully in %.2f seconds", execution_time)
            return final_result

        except subprocess.TimeoutExpired as e:
            self.log.error("DocETL execution timed out after %s seconds", self.timeout)
            raise AirflowException(f"DocETL execution timed out after {self.timeout} seconds") from e

        except Exception as e:
            execution_time = (datetime.now() - execution_start).total_seconds()
            self.log.error("DocETL execution failed after %.2f seconds: %s", execution_time, str(e))

            error_result = {
                "success": False,
                "error": str(e),
                "execution_time": execution_time,
                "config_path": self.config_path,
                "timestamp": execution_start.isoformat(),
            }

            # Save error details for debugging
            self._save_error_details(error_result)
            raise

    def _validate_configuration(self) -> None:
        """Validate DocETL configuration file."""
        try:
            if not os.path.exists(self.config_path):
                raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}

            # Basic validation
            required_fields = ["default_model", "operations"]
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Required field missing from configuration: {field}")

            # Validate operations
            operations = config.get("operations", [])
            if not operations:
                raise ValueError("No operations defined in configuration")

            for i, op in enumerate(operations):
                if "name" not in op:
                    raise ValueError(f"Operation {i} missing required 'name' field")
                if "type" not in op:
                    raise ValueError(f"Operation {i} missing required 'type' field")
                if "prompt" not in op:
                    raise ValueError(f"Operation {i} missing required 'prompt' field")

            self.log.info("Configuration validation passed")

        except Exception as e:
            self.log.error("Configuration validation failed: %s", str(e))
            raise

    def _execute_docetl_pipeline(self) -> Dict[str, Any]:
        """Execute the DocETL pipeline and return results."""
        try:
            import sys

            # Prepare command
            cmd = [
                sys.executable,
                "-m",
                "docetl.cli",
                self.docetl_command,
                self.config_path,
            ]

            if self.optimize and self.docetl_command == "run":
                cmd.append("--optimize")

            # Add output path if specified
            if self.output_path and self.docetl_command == "run":
                cmd.extend(["--output", self.output_path])

            self.log.info("Executing command: %s", " ".join(cmd))

            # Execute DocETL
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                cwd="/opt/airflow",
                env=dict(os.environ, PYTHONPATH="/opt/airflow"),
                check=False,
            )

            if result.returncode != 0:
                error_msg = f"DocETL execution failed with return code {result.returncode}"
                if result.stderr:
                    error_msg += f"\nStderr: {result.stderr}"
                if result.stdout:
                    error_msg += f"\nStdout: {result.stdout}"
                raise AirflowException(error_msg)

            self.log.info("DocETL pipeline executed successfully")

            return {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "command": " ".join(cmd),
            }

        except subprocess.TimeoutExpired as e:
            self.log.error("DocETL execution timed out: %s", str(e))
            raise
        except Exception as e:
            self.log.error("Error executing DocETL pipeline: %s", str(e))
            raise

    def _process_results(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate DocETL execution results."""
        try:
            processed_results: Dict[str, Any] = {
                "execution_info": {
                    "command": execution_result["command"],
                    "returncode": execution_result["returncode"],
                    "stdout_length": len(execution_result.get("stdout", "")),
                    "stderr_length": len(execution_result.get("stderr", "")),
                }
            }

            # Try to load output file if it exists
            if os.path.exists(self.output_path):
                try:
                    with open(self.output_path, "r", encoding="utf-8") as f:
                        output_data = json.load(f)

                    processed_results["output_data"] = output_data
                    processed_results["output_stats"] = self._analyze_output_data(output_data)

                    self.log.info("Successfully loaded output data from %s", self.output_path)

                except json.JSONDecodeError as e:
                    self.log.warning("Could not parse output file as JSON: %s", str(e))
                    processed_results["output_error"] = f"JSON parse error: {str(e)}"
            else:
                self.log.warning("Output file not found: %s", self.output_path)
                processed_results["output_error"] = f"Output file not found: {self.output_path}"

            # Parse stdout for any additional information
            if execution_result.get("stdout"):
                processed_results["stdout_summary"] = self._parse_stdout(execution_result["stdout"])

            return processed_results

        except Exception as e:
            self.log.error("Error processing results: %s", str(e))
            return {
                "processing_error": str(e),
                "raw_execution_result": execution_result,
            }

    def _analyze_output_data(self, output_data: Any) -> Dict[str, Any]:
        """Analyze DocETL output data and generate statistics."""
        try:
            stats: Dict[str, Any] = {
                "data_type": type(output_data).__name__,
                "timestamp": datetime.now().isoformat(),
            }

            if isinstance(output_data, list):
                stats["total_items"] = len(output_data)

                # Analyze item structure if items exist
                if output_data:
                    first_item = output_data[0]
                    if isinstance(first_item, dict):
                        stats["item_fields"] = list(first_item.keys())
                        stats["sample_item"] = {k: str(v)[:100] for k, v in first_item.items()}

            elif isinstance(output_data, dict):
                stats["top_level_keys"] = list(output_data.keys())

                # Look for common patterns
                if "articles" in output_data:
                    articles = output_data["articles"]
                    if isinstance(articles, list):
                        stats["articles_count"] = len(articles)

                if "processing_stats" in output_data:
                    stats["has_processing_stats"] = True

            return stats

        except Exception as e:
            self.log.warning("Error analyzing output data: %s", str(e))
            return {"analysis_error": str(e)}

    def _parse_stdout(self, stdout: str) -> Dict[str, Any]:
        """Parse DocETL stdout for useful information."""
        try:
            summary: Dict[str, Any] = {
                "total_lines": len(stdout.split("\n")),
                "contains_error": "error" in stdout.lower() or "exception" in stdout.lower(),
                "contains_warning": "warning" in stdout.lower(),
                "contains_success": "success" in stdout.lower() or "completed" in stdout.lower(),
            }

            # Extract any performance metrics if present
            for line in stdout.split("\n"):
                if "processed" in line.lower() and any(char.isdigit() for char in line):
                    summary["processing_info"] = line.strip()
                    break

            return summary

        except Exception as e:
            self.log.warning("Error parsing stdout: %s", str(e))
            return {"parse_error": str(e)}

    def _save_error_details(self, error_result: Dict[str, Any]) -> None:
        """Save error details for debugging."""
        try:
            error_dir = "/opt/airflow/data/errors"
            os.makedirs(error_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_file = f"{error_dir}/docetl_error_{timestamp}.json"

            with open(error_file, "w", encoding="utf-8") as f:
                json.dump(error_result, f, indent=2, ensure_ascii=False)

            self.log.info("Error details saved to %s", error_file)

        except Exception as e:
            self.log.warning("Could not save error details: %s", str(e))


class DocETLConfigGenerator:
    """
    Helper class for generating DocETL configurations dynamically.
    """

    def __init__(self, base_config: Optional[Dict[str, Any]] = None) -> None:
        """
        Initialize configuration generator.

        Args:
            base_config: Base configuration to extend
        """
        self.base_config = base_config or {
            "default_model": os.getenv("DEFAULT_MODEL", "gpt-4o-mini"),
            "datasets": {},
            "operations": [],
        }

    def add_dataset(
            self,
            name: str,
            source_type: str = "file",
            source: str = "local",
            path: Optional[str] = None,
            parsing_tools: Optional[list] = None,
    ) -> "DocETLConfigGenerator":
        """Add a dataset to the configuration."""
        dataset_config: Dict[str, Any] = {
            "type": source_type,
            "source": source,
        }

        if path:
            dataset_config["path"] = path

        if parsing_tools:
            dataset_config["parsing_tools"] = parsing_tools

        self.base_config["datasets"][name] = dataset_config
        return self

    def add_operation(
            self,
            name: str,
            operation_type: str,
            prompt: str,
            optimize: bool = True,
            output_schema: Optional[Dict] = None,
            validation: Optional[list] = None,
    ) -> "DocETLConfigGenerator":
        """Add an operation to the configuration."""
        operation_config: Dict[str, Any] = {
            "name": name,
            "type": operation_type,
            "prompt": prompt,
            "optimize": optimize,
        }

        if output_schema:
            operation_config["output_schema"] = output_schema

        if validation:
            operation_config["validation"] = validation

        self.base_config["operations"].append(operation_config)
        return self

    def save_config(self, path: str) -> str:
        """Save configuration to YAML file."""
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(self.base_config, f, default_flow_style=False, allow_unicode=True)

        return path

    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration dictionary."""
        return self.base_config.copy()
