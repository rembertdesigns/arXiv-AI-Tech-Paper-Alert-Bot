#!/usr/bin/env python3
"""
arXiv AI/Tech Paper Alert Bot
Monitors arXiv for new papers and sends automated notifications
"""

import arxiv
import json
import yaml
import logging
import smtplib
import requests
import sqlite3
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Set
import sys
import os


class ArxivAlertBot:
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the bot with configuration"""
        self.config = self._load_config(config_path)
        self._setup_logging()
        self._setup_database()
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML or JSON file"""
        config_file = Path(config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                return yaml.safe_load(f)
            elif config_path.endswith('.json'):
                return json.load(f)
            else:
                raise ValueError("Config file must be YAML or JSON")
    
    def _setup_logging(self):
        """Configure logging"""
        log_level = self.config.get('logging', {}).get('level', 'INFO')
        log_file = self.config.get('logging', {}).get('file', 'arxiv_bot.log')
        
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("ArxivAlertBot initialized")
    
    def _setup_database(self):
        """Setup SQLite database for tracking sent papers"""
        db_path = self.config.get('database', {}).get('path', 'arxiv_papers.db')
        self.conn = sqlite3.connect(db_path)
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sent_papers (
                paper_id TEXT PRIMARY KEY,
                title TEXT,
                sent_date TEXT,
                categories TEXT
            )
        ''')
        self.conn.commit()
        self.logger.info(f"Database initialized at {db_path}")
    
    def _get_sent_papers(self) -> Set[str]:
        """Retrieve set of already sent paper IDs"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT paper_id FROM sent_papers")
        return {row[0] for row in cursor.fetchall()}
    
    def _mark_as_sent(self, paper_id: str, title: str, categories: List[str]):
        """Mark a paper as sent"""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO sent_papers (paper_id, title, sent_date, categories) VALUES (?, ?, ?, ?)",
            (paper_id, title, datetime.now().isoformat(), ','.join(categories))
        )
        self.conn.commit()
    
    def fetch_papers(self) -> List[arxiv.Result]:
        """Fetch papers from arXiv based on configuration"""
        search_config = self.config.get('search', {})
        categories = search_config.get('categories', ['cs.AI'])
        keywords = search_config.get('keywords', [])
        days_back = search_config.get('days_back', 1)
        max_results = search_config.get('max_results', 100)
        
        # Build search query
        category_query = ' OR '.join([f'cat:{cat}' for cat in categories])
        
        if keywords:
            keyword_query = ' OR '.join([f'all:{kw}' for kw in keywords])
            query = f"({category_query}) AND ({keyword_query})"
        else:
            query = category_query
        
        self.logger.info(f"Searching arXiv with query: {query}")
        
        # Search arXiv
        search = arxiv.Search(
            query=query,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending
        )
        
        # Filter by date
        cutoff_date = datetime.now() - timedelta(days=days_back)
        papers = []
        
        for result in search.results():
            if result.published.replace(tzinfo=None) >= cutoff_date:
                papers.append(result)
        
        self.logger.info(f"Found {len(papers)} papers from last {days_back} days")
        return papers
    
    def filter_papers(self, papers: List[arxiv.Result]) -> List[arxiv.Result]:
        """Filter papers based on sent history and optional keyword matching"""
        sent_papers = self._get_sent_papers()
        filtered = []
        
        filter_config = self.config.get('filter', {})
        title_keywords = filter_config.get('title_keywords', [])
        abstract_keywords = filter_config.get('abstract_keywords', [])
        
        for paper in papers:
            paper_id = paper.entry_id.split('/')[-1]
            
            # Skip if already sent
            if paper_id in sent_papers:
                continue
            
            # Optional keyword filtering
            if title_keywords:
                if not any(kw.lower() in paper.title.lower() for kw in title_keywords):
                    continue
            
            if abstract_keywords:
                if not any(kw.lower() in paper.summary.lower() for kw in abstract_keywords):
                    continue
            
            filtered.append(paper)
        
        self.logger.info(f"Filtered to {len(filtered)} new papers")
        return filtered
    
    def format_paper_summary(self, paper: arxiv.Result) -> str:
        """Format paper information as text"""
        authors = ', '.join([author.name for author in paper.authors[:3]])
        if len(paper.authors) > 3:
            authors += ' et al.'
        
        return f"""
Title: {paper.title}
Authors: {authors}
Published: {paper.published.strftime('%Y-%m-%d')}
Categories: {', '.join(paper.categories)}
URL: {paper.entry_id}

Abstract: {paper.summary[:300]}...
{"="*80}
"""
    
    def send_email(self, papers: List[arxiv.Result]):
        """Send email notification"""
        email_config = self.config.get('notifications', {}).get('email', {})
        
        if not email_config.get('enabled', False):
            return
        
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"arXiv Alert: {len(papers)} New Papers"
            msg['From'] = email_config['from_address']
            msg['To'] = email_config['to_address']
            
            # Create text body
            body = f"Found {len(papers)} new papers matching your criteria:\n\n"
            for paper in papers:
                body += self.format_paper_summary(paper)
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            with smtplib.SMTP(email_config['smtp_server'], email_config.get('smtp_port', 587)) as server:
                server.starttls()
                server.login(email_config['username'], email_config['password'])
                server.send_message(msg)
            
            self.logger.info(f"Email sent successfully to {email_config['to_address']}")
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")
            raise
    
    def send_slack(self, papers: List[arxiv.Result]):
        """Send Slack notification"""
        slack_config = self.config.get('notifications', {}).get('slack', {})
        
        if not slack_config.get('enabled', False):
            return
        
        try:
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"📚 {len(papers)} New arXiv Papers"
                    }
                }
            ]
            
            for paper in papers[:10]:  # Limit to 10 papers for Slack
                authors = ', '.join([author.name for author in paper.authors[:2]])
                if len(paper.authors) > 2:
                    authors += ' et al.'
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{paper.title}*\n{authors}\n<{paper.entry_id}|View Paper>"
                    }
                })
                blocks.append({"type": "divider"})
            
            payload = {"blocks": blocks}
            
            response = requests.post(
                slack_config['webhook_url'],
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            
            self.logger.info("Slack notification sent successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {e}")
            raise
    
    def send_webhook(self, papers: List[arxiv.Result]):
        """Send custom webhook notification"""
        webhook_config = self.config.get('notifications', {}).get('webhook', {})
        
        if not webhook_config.get('enabled', False):
            return
        
        try:
            payload = {
                "timestamp": datetime.now().isoformat(),
                "count": len(papers),
                "papers": [
                    {
                        "id": paper.entry_id.split('/')[-1],
                        "title": paper.title,
                        "authors": [author.name for author in paper.authors],
                        "published": paper.published.isoformat(),
                        "categories": paper.categories,
                        "url": paper.entry_id,
                        "abstract": paper.summary
                    }
                    for paper in papers
                ]
            }
            
            response = requests.post(
                webhook_config['url'],
                json=payload,
                headers=webhook_config.get('headers', {}),
                timeout=10
            )
            response.raise_for_status()
            
            self.logger.info("Webhook notification sent successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to send webhook notification: {e}")
            raise
    
    def send_notifications(self, papers: List[arxiv.Result]):
        """Send notifications via all configured methods"""
        if not papers:
            self.logger.info("No papers to notify about")
            return
        
        retry_config = self.config.get('notifications', {}).get('retry', {})
        max_retries = retry_config.get('max_attempts', 3)
        
        notification_methods = [
            ('email', self.send_email),
            ('slack', self.send_slack),
            ('webhook', self.send_webhook)
        ]
        
        for method_name, method_func in notification_methods:
            for attempt in range(max_retries):
                try:
                    method_func(papers)
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Retry {attempt + 1}/{max_retries} for {method_name}")
                    else:
                        self.logger.error(f"Failed to send {method_name} after {max_retries} attempts")
    
    def run(self):
        """Main execution flow"""
        try:
            self.logger.info("Starting arXiv alert bot run")
            
            # Fetch papers
            papers = self.fetch_papers()
            
            # Filter papers
            filtered_papers = self.filter_papers(papers)
            
            if not filtered_papers:
                self.logger.info("No new papers to report")
                return
            
            # Send notifications
            self.send_notifications(filtered_papers)
            
            # Mark papers as sent
            for paper in filtered_papers:
                paper_id = paper.entry_id.split('/')[-1]
                self._mark_as_sent(paper_id, paper.title, paper.categories)
            
            self.logger.info(f"Successfully processed and sent {len(filtered_papers)} papers")
            
        except Exception as e:
            self.logger.error(f"Error during bot run: {e}", exc_info=True)
            raise
        finally:
            self.conn.close()


def main():
    """Entry point for the script"""
    config_path = os.getenv('ARXIV_BOT_CONFIG', 'config.yaml')
    
    try:
        bot = ArxivAlertBot(config_path)
        bot.run()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()