import os
import json
import httpx
from bs4 import BeautifulSoup
from tqdm.auto import tqdm
from tenacity import retry, stop_after_delay, wait_exponential, retry_if_exception_type


class SakugaScraper:
    BASE_URL = "https://www.sakugabooru.com/post/show/{}"

    def __init__(self, root_dir: str):
        self.root_dir = root_dir
        os.makedirs(root_dir, exist_ok=True)

    @retry(
        retry=retry_if_exception_type(httpx.RequestError),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        stop=stop_after_delay(20),
        reraise=True,
    )
    def fetch_post(self, post_id: str):
        """Fetch the HTML content of the given post ID with retries."""
        url = self.BASE_URL.format(post_id)
        response = httpx.get(url, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser"), url

    @retry(
        retry=retry_if_exception_type(httpx.RequestError),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        stop=stop_after_delay(20),
        reraise=True,
    )
    def download_image(self, url: str, save_path: str):
        """Download an image with retries."""
        response = httpx.get(url, timeout=10)
        response.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(response.content)

    def extract_metadata(self, soup, post_id: str, post_url: str):
        """Extract metadata from the post HTML."""
        metadata = {"post_id": post_id, "post_url": post_url}

        # Extract high-res image link
        highres_link = soup.find("a", id="highres")
        metadata["image_url"] = highres_link["href"] if highres_link else None

        # Extract tags
        metadata["tags"] = {}
        tag_sidebar = soup.find("ul", id="tag-sidebar")
        if tag_sidebar:
            for li in tag_sidebar.find_all("li"):
                tag_type = li.get("class", [None])[0]
                tag_name = li.find_all("a")[1].text.strip() if li.find_all("a") else None
                if tag_name:
                    metadata["tags"].setdefault(tag_type, []).append(tag_name)

        # Extract statistics
        stats = soup.find("div", id="stats")
        if stats:
            for li in stats.find_all("li"):
                text = li.text.strip()
                if ":" in text:
                    key, value = map(str.strip, text.split(":", 1))
                    metadata[key.lower().replace(" ", "_")] = value

        # Extract status notices
        metadata["status_notice"] = []
        metadata["status_notice_parsed"] = {}
        status_notices = soup.find_all("div", class_="status-notice")
        for notice in status_notices:
            notice_text = notice.text.strip()
            metadata["status_notice"].append(notice_text)

            # Parse parent post
            if "belongs to a parent post" in notice_text.lower():
                parent_link = notice.find("a", href=True)
                if parent_link:
                    metadata["status_notice_parsed"]["parent_post_id"] = parent_link["href"].split("/")[-1]

            # Parse deletion flag
            if "flagged for deletion" in notice_text.lower():
                flagger_info = notice_text.split("Reason:")
                if len(flagger_info) > 1:
                    metadata["status_notice_parsed"]["deletion_reason"] = flagger_info[1].strip()
                flagged_by = notice_text.split("by ")
                if len(flagged_by) > 1:
                    metadata["status_notice_parsed"]["flagged_by"] = flagged_by[1].split(".")[0].strip()

        return metadata

    def scrape_post(self, post_id: str):
        """Scrape a single post by ID."""
        soup, post_url = self.fetch_post(post_id)
        metadata = self.extract_metadata(soup, post_id, post_url)

        # Prepare directories and file paths
        post_dir = os.path.join(self.root_dir, f"post_{post_id}")
        os.makedirs(post_dir, exist_ok=True)
        ext = metadata["image_url"].split(".")[-1] if metadata["image_url"] else "jpg"
        image_path = os.path.join(post_dir, f"sankaku_{post_id}.{ext}")
        metadata_path = os.path.join(post_dir, f"sankaku_{post_id}.json")

        # Download image and save metadata
        if metadata["image_url"]:
            self.download_image(metadata["image_url"], image_path)
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)

    def scrape_posts(self, post_ids: list[str]):
        """Scrape multiple posts."""
        for post_id in tqdm(post_ids):
            try:
                print(f"Scraping post ID: {post_id}")
                self.scrape_post(post_id)
                print(f"Successfully downloaded post {post_id}")
            except Exception as e:
                print(f"Failed to download post {post_id}: {e}")


# Example Usage
if __name__ == "__main__":
    scraper = SakugaScraper(root_dir="/rmt/yada/dev/sakuga-scraper/data/sakuga_downloads")

    todo_posts = [i for i in range(0, 1000)]
    scraper.scrape_posts(["44843", "272528"])
