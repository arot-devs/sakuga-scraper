"""
Setup:
- clone repo
- poetry install
- poetry shell
- python run_scraper.py

"""

from sakuga_scraper import SakugaScraper

scraper = SakugaScraper(root_dir='./data/')
post_ids = [i for i in range(22003, 100000)]
scraper.scrape_posts(post_ids)