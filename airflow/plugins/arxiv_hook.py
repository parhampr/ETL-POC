"""
ArXiv Hook for Airflow
=====================

Custom hook for interacting with arXiv API to search and download papers.
"""

import logging
import os
from typing import Dict, List, Any, Optional

import arxiv
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class ArxivHook(BaseHook):
    """
    Custom Hook for arXiv API interactions.

    Provides methods for:
    - Searching papers by category/query
    - Downloading PDF files
    - Extracting metadata
    """

    def __init__(self):
        super().__init__()
        self.client = arxiv.Client()
        self.base_data_dir = '/opt/airflow/data'

        # Create necessary directories
        os.makedirs(f"{self.base_data_dir}/input", exist_ok=True)
        os.makedirs(f"{self.base_data_dir}/processed", exist_ok=True)

    def search_papers(
            self,
            query: str,
            max_results: int = 10,
            sort_by: str = "submittedDate",
            sort_order: str = "descending"
    ) -> List[Dict[str, Any]]:
        """
        Search for papers on arXiv.

        Args:
            query: Search query (e.g., 'cat:cs.AI', 'machine learning')
            max_results: Maximum number of results to return
            sort_by: Sort criterion ('submittedDate', 'lastUpdatedDate', 'relevance')
            sort_order: Sort order ('ascending', 'descending')

        Returns:
            List of paper metadata dictionaries
        """
        try:
            logger.info(f"Searching arXiv with query: {query}, max_results: {max_results}")

            # Map sort criteria
            sort_criterion_map = {
                'submittedDate': arxiv.SortCriterion.SubmittedDate,
                'lastUpdatedDate': arxiv.SortCriterion.LastUpdatedDate,
                'relevance': arxiv.SortCriterion.Relevance
            }

            sort_order_map = {
                'ascending': arxiv.SortOrder.Ascending,
                'descending': arxiv.SortOrder.Descending
            }

            search = arxiv.Search(
                query=query,
                max_results=max_results,
                sort_by=sort_criterion_map.get(sort_by, arxiv.SortCriterion.SubmittedDate),
                sort_order=sort_order_map.get(sort_order, arxiv.SortOrder.Descending)
            )

            papers = []
            for result in self.client.results(search):
                paper_data = {
                    'arxiv_id': result.entry_id.split('/')[-1],
                    'title': result.title,
                    'authors': [author.name for author in result.authors],
                    'summary': result.summary,
                    'published': result.published.isoformat() if result.published else None,
                    'updated': result.updated.isoformat() if result.updated else None,
                    'categories': result.categories,
                    'primary_category': result.primary_category,
                    'pdf_url': result.pdf_url,
                    'entry_id': result.entry_id,
                    'doi': result.doi,
                    'journal_ref': result.journal_ref,
                    'comment': result.comment
                }
                papers.append(paper_data)

            logger.info(f"Found {len(papers)} papers matching query: {query}")
            return papers

        except Exception as e:
            logger.error(f"Error searching arXiv: {str(e)}")
            raise

    def download_paper(self, paper: Dict[str, Any]) -> Optional[str]:
        """
        Download PDF for a paper.

        Args:
            paper: Paper metadata dictionary

        Returns:
            Path to downloaded PDF file, or None if failed
        """
        try:
            arxiv_id = paper.get('arxiv_id', '').replace('v', '_v')  # Handle version numbers
            if not arxiv_id:
                logger.error("No arXiv ID found in paper metadata")
                return None

            # Create safe filename
            safe_title = "".join(c for c in paper.get('title', '')[:50] if c.isalnum() or c in (' ', '-', '_')).rstrip()
            filename = f"{arxiv_id}_{safe_title}.pdf".replace(' ', '_')
            filepath = f"{self.base_data_dir}/input/{filename}"

            # Skip if already downloaded
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:  # At least 1KB
                logger.info(f"Paper already downloaded: {filepath}")
                return filepath

            # Download using arxiv library
            paper_obj = next(arxiv.Client().results(arxiv.Search(id_list=[paper['arxiv_id']])))
            download_path = paper_obj.download_pdf(dirpath=f"{self.base_data_dir}/input", filename=filename)

            if os.path.exists(download_path):
                logger.info(f"Successfully downloaded: {download_path}")
                return download_path
            else:
                logger.error(f"Download failed - file not found: {download_path}")
                return None

        except Exception as e:
            logger.error(f"Error downloading paper {paper.get('arxiv_id', 'unknown')}: {str(e)}")
            return None

    def extract_text_from_pdf(self, pdf_path: str) -> Optional[str]:
        """
        Extract text from PDF file using PyMuPDF.

        Args:
            pdf_path: Path to PDF file

        Returns:
            Extracted text content, or None if failed
        """
        try:
            import fitz  # PyMuPDF

            if not os.path.exists(pdf_path):
                logger.error(f"PDF file not found: {pdf_path}")
                return None

            doc = fitz.open(pdf_path)
            text_content = ""

            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                text_content += page.get_text()
                text_content += "\n\n"  # Page separator

            doc.close()

            # Basic cleanup
            text_content = text_content.strip()
            if len(text_content) < 100:  # Very short content might indicate extraction failure
                logger.warning(f"Extracted text seems too short ({len(text_content)} chars): {pdf_path}")

            logger.info(f"Extracted {len(text_content)} characters from {pdf_path}")
            return text_content

        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {str(e)}")
            return None

    def get_paper_categories(self, categories: List[str]) -> Dict[str, str]:
        """
        Get human-readable descriptions for arXiv categories.

        Args:
            categories: List of arXiv category codes

        Returns:
            Dictionary mapping categories to descriptions
        """
        # Common arXiv category mappings
        category_descriptions = {
            'cs.AI': 'Artificial Intelligence',
            'cs.CL': 'Computation and Language',
            'cs.CV': 'Computer Vision and Pattern Recognition',
            'cs.LG': 'Machine Learning',
            'cs.NE': 'Neural and Evolutionary Computing',
            'cs.RO': 'Robotics',
            'stat.ML': 'Machine Learning (Statistics)',
            'math.OC': 'Optimization and Control',
            'eess.IV': 'Image and Video Processing',
            'eess.AS': 'Audio and Speech Processing',
            'physics.data-an': 'Data Analysis, Statistics and Probability',
            'q-bio.QM': 'Quantitative Methods',
            'cond-mat.dis-nn': 'Disordered Systems and Neural Networks'
        }

        result = {}
        for category in categories:
            result[category] = category_descriptions.get(category, f"Category: {category}")

        return result

    def validate_paper_quality(self, paper: Dict[str, Any], text_content: str = None) -> Dict[str, Any]:
        """
        Validate paper quality for processing.

        Args:
            paper: Paper metadata
            text_content: Extracted text content (optional)

        Returns:
            Validation results with quality metrics
        """
        issues = []
        quality_score = 1.0

        # Check required fields
        required_fields = ['title', 'authors', 'summary']
        for field in required_fields:
            if not paper.get(field):
                issues.append(f"Missing {field}")
                quality_score -= 0.2

        # Check title length
        title = paper.get('title', '')
        if len(title) < 10:
            issues.append("Title too short")
            quality_score -= 0.1
        elif len(title) > 200:
            issues.append("Title too long")
            quality_score -= 0.05

        # Check abstract length
        summary = paper.get('summary', '')
        if len(summary) < 100:
            issues.append("Abstract too short")
            quality_score -= 0.1
        elif len(summary) > 5000:
            issues.append("Abstract too long")
            quality_score -= 0.05

        # Check text content if provided
        if text_content:
            if len(text_content) < 1000:
                issues.append("Extracted text too short")
                quality_score -= 0.2
            elif len(text_content) > 100000:
                issues.append("Extracted text very long (may affect processing)")
                quality_score -= 0.1

        # Check author information
        authors = paper.get('authors', [])
        if not authors:
            issues.append("No authors listed")
            quality_score -= 0.1
        elif len(authors) > 20:
            issues.append("Too many authors (may be collaboration paper)")
            quality_score -= 0.05

        return {
            'quality_score': max(0.0, quality_score),
            'issues': issues,
            'is_suitable_for_processing': quality_score > 0.5,
            'recommendations': self._get_processing_recommendations(issues)
        }

    def _get_processing_recommendations(self, issues: List[str]) -> List[str]:
        """Get processing recommendations based on identified issues."""
        recommendations = []

        if "Missing title" in issues:
            recommendations.append("Skip processing - title required for article generation")
        if "Missing abstract" in issues:
            recommendations.append("Use full text extraction for summary generation")
        if "Extracted text too short" in issues:
            recommendations.append("Verify PDF quality and consider manual processing")
        if "Title too long" in issues:
            recommendations.append("Truncate title for headline generation")

        return recommendations