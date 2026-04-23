import hashlib
import pathlib
import time
import urllib.request
from typing import Mapping


DATA_DIR = pathlib.Path("data")
RESOURCES_DIR = pathlib.Path("resources")
DEFAULT_DOWNLOAD_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_INITIAL_BACKOFF = 0.5
DEFAULT_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MAX_BACKOFF = 30.0
DEFAULT_CACHE_DIR = DATA_DIR / ".cache"
BASE_URL = "https://www.unibo.it"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


def download_html_page(
    url: str,
    *,
    timeout: float = DEFAULT_DOWNLOAD_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    initial_backoff: float = DEFAULT_INITIAL_BACKOFF,
    backoff_multiplier: float = DEFAULT_BACKOFF_MULTIPLIER,
    max_backoff: float | None = DEFAULT_MAX_BACKOFF,
    headers: Mapping[str, str] | None = None,
    cache_dir: pathlib.Path | None = None,
    use_cache: bool = True,
    refresh_cache: bool = False,
) -> str:
    if timeout <= 0:
        raise ValueError("timeout must be > 0")
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if initial_backoff < 0:
        raise ValueError("initial_backoff must be >= 0")
    if backoff_multiplier < 1:
        raise ValueError("backoff_multiplier must be >= 1")
    if max_backoff is not None and max_backoff < 0:
        raise ValueError("max_backoff must be >= 0 when provided")

    resolved_cache_dir = cache_dir or DEFAULT_CACHE_DIR
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    cache_path = resolved_cache_dir / f"{digest}.html"

    if use_cache and not refresh_cache and cache_path.exists():
        return cache_path.read_text(encoding="utf-8")

    request_headers = dict(DEFAULT_HEADERS)
    if headers is not None:
        request_headers.update(headers)

    last_error: Exception | None = None
    backoff = initial_backoff

    for attempt in range(max_retries + 1):
        try:
            request = urllib.request.Request(url, headers=request_headers)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                html = response.read().decode(charset, errors="replace")

            if use_cache:
                resolved_cache_dir.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(html, encoding="utf-8")

            return html
        except Exception as error:
            last_error = error
            if attempt >= max_retries:
                break
            if backoff > 0:
                time.sleep(min(backoff, max_backoff) if max_backoff is not None else backoff)
            if backoff > 0:
                backoff *= backoff_multiplier

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Could not download URL: {url}")


def auto_logged(logger):
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.debug(f"Entering {func.__name__} with args={args} kwargs={kwargs}")
            try:
                result = func(*args, **kwargs)
                logger.debug(f"Exiting {func.__name__} with result={result}")
                return result
            except Exception as e:
                logger.debug(f"Error in {func.__name__}: {e}")
                raise
        return wrapper
    return decorator