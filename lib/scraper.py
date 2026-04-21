"""HTTP fetch + BeautifulSoup parsing."""

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

REMOVE_TAGS = [
    "script", "style", "nav", "footer", "header", "noscript",
    "iframe", "aside", "form", "svg", "button",
]


def fetch_page(domain: str, timeout: int = 10) -> tuple[str, str, str]:
    """Try URL variants, return (html, final_url, error_or_none).
    Returns scrape_status: success | failed | blocked."""
    prefixes = [
        f"https://www.{domain}",
        f"https://{domain}",
        f"http://www.{domain}",
        f"http://{domain}",
    ]
    last_error = ""
    for url in prefixes:
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=timeout,
                allow_redirects=True,
            )
            if resp.status_code in (403, 429, 503):
                return "", url, "blocked"
            if resp.status_code == 200 and len(resp.text) > 500:
                return resp.text, resp.url, "success"
            last_error = f"status={resp.status_code},len={len(resp.text)}"
        except requests.exceptions.SSLError:
            last_error = "ssl_error"
            continue
        except requests.exceptions.ConnectionError:
            last_error = "connection_error"
            continue
        except requests.exceptions.Timeout:
            last_error = "timeout"
            continue
        except Exception as e:
            last_error = str(e)[:100]
            continue
    return "", "", f"failed:{last_error}"


def parse_html(html: str) -> BeautifulSoup:
    """Parse HTML and remove noise tags."""
    soup = BeautifulSoup(html, "lxml")
    for tag_name in REMOVE_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()
    return soup
