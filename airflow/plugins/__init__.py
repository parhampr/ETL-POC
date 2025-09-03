"""
Zara ETL Custom Airflow Plugins
===============================

This module contains custom hooks and operators for the Zara ETL pipeline.
"""

from airflow.plugins_manager import AirflowPlugin

from arxiv_hook import ArxivHook  
from docetl_operator import DocETLOperator

class ZaraETLPlugin(AirflowPlugin):
    name = "zara_etl"
    hooks = [ArxivHook]
    operators = [DocETLOperator]