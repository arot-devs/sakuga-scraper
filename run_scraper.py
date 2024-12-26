"""
Setup:
- clone repo
- poetry install
- poetry shell
- python run_scraper.py

"""

from sakuga_scraper import SakugaScraper
import click

@click.command()
@click.option('--from', 'from_id', default=65911, help='Starting post id')
@click.option('--to', 'to_id', default=272731, help='Ending post id')
def run_scraper(from_id, to_id):
    """Run the SakugaScraper with a range of post IDs."""
    scraper = SakugaScraper(root_dir='./data/')
    post_ids = list(range(from_id, to_id))  # Create a list of post IDs in the specified range
    print(f"Scraping posts {from_id} to {to_id} | Total: {len(post_ids)}")
    scraper.scrape_posts(post_ids)

if __name__ == "__main__":
    run_scraper()
