import time

from robot.api.deco import keyword

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait


class url_is_stable:
    """
    Selenium wait condition that waits for the URL to be stable and the page to be ready.
    """
    def __init__(self, duration: float = 2.0):
        self._duration = duration
        # This is the last URL that we saw
        self._last_url = None
        # This is the time that we first saw it
        self._last_url_ready = None

    def __call__(self, driver):
        current_url = driver.current_url
        # If the page is not fully loaded, we are done
        ready_state = driver.execute_script("return document.readyState")
        if ready_state.lower() != "complete":
            return False
        # If the URL has changed, then it is not stable
        if not self._last_url or self._last_url != current_url:
            self._last_url = current_url
            self._last_url_seen = time.monotonic()
            return False
        # Check if the last time the URL changed is long enough ago
        return (time.monotonic() - self._last_url_seen) >= self._duration


class title_contains:
    """
    Selenium wait condition that waits for the given string to be in the page title.
    """
    def __init__(self, expected_title):
        self._expected_title = expected_title

    def __call__(self, driver):
        # Wait until the page is fully loaded
        ready_state = driver.execute_script("return document.readyState")
        if ready_state.lower() != "complete":
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
    def open_browser(self, browser: str = "firefox", headless: bool = True):
        """
        Opens the specified browser.
        """
        if self._driver is not None:
            self.close_browser()

        if browser == "firefox":
            options = webdriver.FirefoxOptions()
            options.headless = headless
            self._driver = webdriver.Firefox(options = options)
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
        # Wait for the URL to settle after clicking submit, so that the cookie gets set
        WebDriverWait(self._driver, 86400).until(url_is_stable())

    @keyword
    def open_zenith_service(self, fqdn: str, authenticate: bool = True):
        """
        Open a Zenith service using the current browser.
        """
        if authenticate:
            self.authenticate_browser()
        # Use the scheme from the Azimuth base URL
        scheme = self._ctx.client.base_url.scheme
        # Visit the Zenith URL and wait for it to stabilise
        self._driver.get(f"{scheme}://{fqdn}?kc_idp_hint=azimuth")
        WebDriverWait(self._driver, 86400).until(url_is_stable())

    @keyword
    def wait_until_page_title_contains(self, expected_title: str):
        """
        Waits until the current page title contains the given string.
        """
        WebDriverWait(self._driver, 86400).until(title_contains(expected_title))
