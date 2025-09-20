# src/scrapers/mock_linkedin_scraper.py
# 模擬 LinkedIn 爬蟲 - 用於測試 DAG 流程

import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
import uuid

class MockLinkedInScraper:
    """
    模擬 LinkedIn 爬蟲 - 不實際連接 LinkedIn
    生成假資料用於測試完整的 ETL 流程
    """
    
    def __init__(self, config: Dict):
        self.config = config
        self.scraped_jobs = []
        self.success_count = 0
        self.total_attempts = 0
        
        # 設定 logger
        self.logger = logging.getLogger(__name__)
        
        # 模擬資料池
        self.mock_data = self._create_mock_data_pools()
        
    def _create_mock_data_pools(self) -> Dict:
        """建立模擬資料池"""
        return {
            'job_titles': [
                'Senior Data Engineer',
                'Data Engineer',
                'Staff Data Engineer', 
                'Principal Data Engineer',
                'Lead Data Engineer',
                'Data Engineer II',
                'Data Platform Engineer',
                'Senior Data Scientist',
                'Data Scientist',
                'Machine Learning Engineer',
                'Analytics Engineer',
                'Data Infrastructure Engineer',
                'Big Data Engineer',
                'Data Pipeline Engineer',
                'Senior Software Engineer - Data'
            ],
            
            'companies': [
                'Google', 'Meta', 'Amazon', 'Apple', 'Microsoft',
                'Netflix', 'Uber', 'Airbnb', 'Stripe', 'Shopify',
                'Snowflake', 'Databricks', 'Palantir', 'Coinbase',
                'Twitter', 'LinkedIn', 'Salesforce', 'Adobe',
                'Spotify', 'Slack', 'Zoom', 'DocuSign',
                'Square', 'PayPal', 'Tesla', 'SpaceX'
            ],
            
            'locations': [
                'San Francisco, CA',
                'Palo Alto, CA', 
                'Mountain View, CA',
                'Redwood City, CA',
                'San Jose, CA',
                'New York, NY',
                'Seattle, WA',
                'Austin, TX',
                'Los Angeles, CA',
                'Chicago, IL',
                'Boston, MA',
                'Denver, CO'
            ],
            
            'employment_types': [
                'Full-time',
                'Contract', 
                'Full-time (Permanent)',
                'Full-time - Permanent'
            ],
            
            'work_arrangements': [
                'Remote',
                'Hybrid',
                'On-site',
                'Remote (US)',
                'Hybrid (3 days in office)'
            ],
            
            'salary_ranges': [
                '$120,000 - $180,000',
                '$140,000 - $200,000', 
                '$160,000 - $220,000',
                '$180,000 - $250,000',
                '$200,000 - $280,000',
                '$100,000 - $150,000',
                '$130,000 - $170,000'
            ],
            
            'skills': [
                'Python', 'SQL', 'AWS', 'Spark', 'Kafka',
                'Docker', 'Kubernetes', 'Airflow', 'dbt',
                'Snowflake', 'Redshift', 'BigQuery', 'PostgreSQL',
                'MongoDB', 'Redis', 'Elasticsearch', 'Tableau',
                'Looker', 'Git', 'Jenkins', 'Terraform'
            ],
            
            'description_templates': [
                "We are looking for a {title} to join our growing data team. You will be responsible for building and maintaining data pipelines, working with large datasets, and collaborating with cross-functional teams.",
                
                "As a {title}, you will design and implement scalable data infrastructure, optimize data workflows, and ensure data quality across our platform.",
                
                "Join our data engineering team as a {title}! You'll work on cutting-edge data technologies, build real-time streaming pipelines, and help drive data-driven decisions.",
                
                "We're seeking a talented {title} to help us scale our data platform. You'll work with modern tools like Spark, Kafka, and cloud technologies.",
                
                "Exciting opportunity for a {title} to work on challenging data problems at scale. You'll be building the next generation of our data infrastructure."
            ]
        }
    
    def _generate_job_description(self, job_title: str, skills: List[str]) -> str:
        """生成模擬職位描述"""
        template = random.choice(self.mock_data['description_templates'])
        base_description = template.format(title=job_title)
        
        # 加入技能要求
        skills_text = f"\n\nRequired Skills:\n• {' • '.join(skills[:5])}"
        
        # 加入額外內容
        additional_content = f"""
        
Responsibilities:
• Design and build scalable data pipelines
• Collaborate with data scientists and analysts
• Maintain and monitor data infrastructure
• Optimize data processing workflows
• Ensure data quality and reliability

Requirements:
• Bachelor's degree in Computer Science or related field
• 3+ years of experience in data engineering
• Strong programming skills in Python and SQL
• Experience with cloud platforms (AWS/GCP/Azure)
• Knowledge of distributed computing frameworks

Benefits:
• Competitive salary and equity
• Comprehensive health insurance
• Flexible work arrangements
• Learning and development budget
• Modern office with great snacks
        """
        
        return base_description + skills_text + additional_content
    
    def _generate_mock_job(self, index: int) -> Dict:
        """生成單一模擬職缺"""
        job_title = random.choice(self.mock_data['job_titles'])
        company = random.choice(self.mock_data['companies'])
        location = random.choice(self.mock_data['locations'])
        
        # 隨機選擇技能
        selected_skills = random.sample(self.mock_data['skills'], k=random.randint(3, 8))
        
        # 生成唯一 job_id
        job_id = f"mock_job_{self.config['batch_id']}_{index:04d}"
        
        # 生成模擬 URL
        job_url = f"https://www.linkedin.com/jobs/view/{random.randint(1000000000, 9999999999)}"
        
        # 隨機決定是否包含薪資資訊 (70% 機率)
        salary_range = random.choice(self.mock_data['salary_ranges']) if random.random() < 0.7 else ""
        
        # 生成發布日期 (過去 1-7 天)
        days_ago = random.randint(1, 7)
        posted_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        job_data = {
            'job_id': job_id,
            'job_url': job_url,
            'job_title': job_title,
            'company_name': company,
            'location': location,
            'employment_type': random.choice(self.mock_data['employment_types']),
            'work_arrangement': random.choice(self.mock_data['work_arrangements']),
            'salary_range': salary_range,
            'posted_date': posted_date,
            'job_description': self._generate_job_description(job_title, selected_skills),
            'scraped_at': datetime.now().isoformat(),
            'mock_data': True  # 標記為模擬資料
        }
        
        return job_data
    
    def scrape_jobs(self) -> List[Dict]:
        """模擬爬取職缺 - 主要方法"""
        target_jobs = self.config.get('target_jobs', 10)
        
        self.logger.info(f"🎭 Starting MOCK LinkedIn job scraping for batch {self.config['batch_id']}")
        self.logger.info(f"🎯 Target: {target_jobs} mock jobs")
        
        # 模擬爬取過程
        for i in range(target_jobs):
            # 模擬網路延遲
            delay = random.uniform(0.5, 2.0)  # 比真實爬取快一些
            time.sleep(delay)
            
            self.total_attempts += 1
            
            # 模擬 95% 成功率
            if random.random() < 0.95:
                job_data = self._generate_mock_job(i)
                self.scraped_jobs.append(job_data)
                self.success_count += 1
                
                if (i + 1) % 5 == 0:
                    self.logger.info(f"🎭 Mock progress: {i + 1}/{target_jobs} jobs generated")
            else:
                # 模擬失敗情況
                self.logger.warning(f"🎭 Mock failure: simulated error for job {i + 1}")
        
        self.logger.info(f"🎉 Mock scraping completed: {len(self.scraped_jobs)} jobs generated")
        self.logger.info(f"📊 Success rate: {self.get_success_rate():.1%}")
        
        return self.scraped_jobs
    
    def get_success_rate(self) -> float:
        """計算模擬成功率"""
        if self.total_attempts == 0:
            return 0.0
        return self.success_count / self.total_attempts
    
    def get_scraping_stats(self) -> Dict:
        """取得模擬爬取統計"""
        return {
            'total_attempts': self.total_attempts,
            'successful_jobs': self.success_count,
            'failed_jobs': self.total_attempts - self.success_count,
            'success_rate': self.get_success_rate(),
            'scraped_jobs_count': len(self.scraped_jobs),
            'batch_id': self.config['batch_id'],
            'is_mock_data': True
        }


class MockLinkedInBasicScraper(MockLinkedInScraper):
    """
    為了兼容現有 DAG，提供與真實爬蟲相同的介面
    """
    pass