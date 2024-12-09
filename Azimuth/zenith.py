import time

import httpx

from robot.api.deco import keyword

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait


class url_is_stable:
    """
    Selenium wait condition that waits for the URL to be stable and the page to be ready.
    """
    def __init__(self, duration: float = 5.0):
        self._duration = duration
        # This is the last URL that we saw
        self._last_url = None
        # This is the time that we first saw it
        self._last_url_ready = None

    def __call__(self, driver):
        current_url = driver.current_url
        # If the page is not fully loaded, we are done
        ready_state = driver.execute_script("return document.readyState")
        if not ready_state or ready_state.lower() != "complete":
            return False
        # If the URL has changed, then it is not stable
        if not self._last_url or self._last_url != current_url:
            self._last_url = current_url
            self._last_url_seen = time.monotonic()
            return False
        # Check if the last time the URL changed is long enough ago
        return (time.monotonic() - self._last_url_seen) >= self._duration


def tenancies_loaded(driver):
    """
    Selenium wait condition that waits for the tenancies to load on the tenancies page.
    """
    # Wait until the page is fully loaded
    ready_state = driver.execute_script("return document.readyState")
    if not ready_state or ready_state.lower() != "complete":
        return False
    # Check if there is at least one tenancy list item
    elems = driver.find_elements(By.CSS_SELECTOR, ".card > .list-group > a")
    return len(elems) > 0


class title_contains:
    """
    Selenium wait condition that waits for the given string to be in the page title.
    """
    def __init__(self, expected_title):
        self._expected_title = expected_title

    def __call__(self, driver):
        # Wait until the page is fully loaded
        ready_state = driver.execute_script("return document.readyState")
        if not ready_state or ready_state.lower() != "complete":
            return False
        return self._expected_title in driver.title


class ZenithKeywords:
    """
    Keywords for interacting with Zenith services.
    """
    def __init__(self, ctx):
        self._ctx = ctx
        self._driver = None

    @keyword
    def open_browser(self, browser: str = "firefox"):
        """
        Opens the specified browser.
        """
        if self._driver is not None:
            self.close_browser()

        if browser == "firefox":
            self._driver = webdriver.Firefox()
        else:
            raise AssertionError(f"browser is not supported - {browser}")

    @keyword
    def close_browser(self):
        """
        Closes the current browser.
        """
        if self._driver:
            self._driver.quit()
            self._driver = None

    @keyword
    def authenticate_browser(self):
        """
        Authenticates the browser using the credentials from the Azimuth SDK client.
        """
        # Authenticate using the same credentials that the Azimuth SDK client is using
        authenticator = self._ctx.client.auth.authenticator
        request = self._ctx.client.build_request("GET", f"/auth/{authenticator}/start/")
        self._driver.get(str(request.url))
        # The Azimuth SDK only supports authenticators that render as forms, and the
        # auth data it uses corresponds to the names of the form fields
        for name, data in self._ctx.client.auth.auth_data.items():
            el = self._driver.find_element(By.NAME, name)
            el.clear()
            el.send_keys(data)
        # Click the submit button
        button = self._driver.find_element(By.XPATH, "//*[@type=\"submit\"]")
        button.click()
        # Wait for the URL to settle and the tenancies to load after clicking submit
        # This ensures that the authentication cookie gets set for the next step
        WebDriverWait(self._driver, 86400).until(url_is_stable())
        # Wait for the tenancies to load
        WebDriverWait(self._driver, 86400).until(tenancies_loaded)

    @keyword
    def open_zenith_service(self, fqdn: str, authenticate: bool = True):
        """
        Open a Zenith service using the current browser.
        """
        if authenticate:
            self.authenticate_browser()
        # Use the scheme from the Azimuth base URL
        scheme = self._ctx.client.base_url.scheme
        zenith_url = f"{scheme}://{fqdn}?kc_idp_hint=azimuth"
        # Wait for the Zenith URL to return something other than a 404, 500, 502, 503 or 504
        # These statuses could occur while the Zenith tunnel is establishing or while the
        # proxied service is starting
        # Use the cookies from the browser session so that the requests are authenticated
        while True:
            try:
                response = httpx.get(
                    zenith_url,
                    follow_redirects = True,
                    cookies = { c["name"]: c["value"] for c in self._driver.get_cookies() },
                    verify = scheme == "https"
                )
            except httpx.TransportError:
                # We want to retry these exceptions
                pass
            else:
                if response.status_code < 400:
                    # Not an error - we can exit!
                    break
                elif response.status_code not in [404, 500, 502, 503, 504]:
                    # Not an error we expect to see during Zenith service startup - fail
                    response.raise_for_status()
            time.sleep(5)
        # Visit the Zenith URL and wait for it to stabilise
        self._driver.get(zenith_url)
        WebDriverWait(self._driver, 86400).until(url_is_stable())

    @keyword
    def wait_until_page_title_contains(self, expected_title: str):
        """
        Waits until the current page title contains the given string.
        """
        WebDriverWait(self._driver, 86400).until(title_contains(expected_title))
