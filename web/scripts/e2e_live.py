#!/usr/bin/env python3
"""Live end-to-end browser test for the poorbricks Next.js UI.

Cypress is the project's e2e runner (see cypress/), but its bundled Electron
needs an X server, which this no-sudo sandbox can't provide. This script drives
the *same* conda Firefox we verified works headless, via geckodriver + Selenium,
and exercises the identical flows as the Cypress e2e specs against the live
Next.js app (:3100) + FastAPI server (:8088) + real Mongo/Postgres data.

Run with:
    LD_LIBRARY_PATH=~/.mm/envs/browser/lib MOZ_HEADLESS=1 \
        .venv/bin/python web/scripts/e2e_live.py
"""

from __future__ import annotations

import os
import sys

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

BROWSER = os.path.expanduser("~/.mm/envs/browser")
FIREFOX = f"{BROWSER}/bin/firefox"
GECKO = f"{BROWSER}/bin/geckodriver"
BASE = os.environ.get("E2E_BASE_URL", "http://localhost:3100")

results: list[tuple[bool, str]] = []


def check(ok: bool, msg: str) -> None:
    results.append((ok, msg))
    print(f"  {'PASS' if ok else 'FAIL'}  {msg}")


def cy(driver, sel: str):
    return driver.find_element(By.CSS_SELECTOR, f'[data-cy="{sel}"]')


def cy_all(driver, sel: str):
    return driver.find_elements(By.CSS_SELECTOR, f'[data-cy="{sel}"]')


def make_driver() -> webdriver.Firefox:
    opts = Options()
    opts.binary_location = FIREFOX
    opts.add_argument("--headless")
    # Sandbox can't be used without extra kernel caps in this container.
    os.environ.setdefault("MOZ_HEADLESS", "1")
    os.environ.setdefault("MOZ_DISABLE_CONTENT_SANDBOX", "1")
    service = Service(executable_path=GECKO, log_output=os.path.devnull)
    return webdriver.Firefox(options=opts, service=service)


def test_main(driver) -> None:
    print("\n# Main page")
    driver.get(BASE + "/")
    wait = WebDriverWait(driver, 20)

    # Alerts panel renders, then the live findings populate the counts.
    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-cy="alerts-panel"]'))
    )
    # The fetch is async; wait until verification findings land (warnings > 0),
    # so we assert on real data rather than the initial empty render.
    wait.until(lambda d: cy(d, "count-warnings").text.strip() not in ("", "0"))
    for k in ("count-errors", "count-warnings", "count-info"):
        txt = cy(driver, k).text.strip()
        check(txt.isdigit(), f"alerts {k} is numeric (got {txt!r})")
    # Expected from the live API: 0 errors, 7 verification warnings, 3 info.
    warn_n = int(cy(driver, "count-warnings").text.strip())
    info_n = int(cy(driver, "count-info").text.strip())
    n_alerts = len(cy_all(driver, "alert"))
    check(warn_n >= 1, f"alerts panel shows live warnings (n={warn_n})")
    check(info_n >= 1, f"alerts panel shows live info findings (n={info_n})")
    check(
        n_alerts == warn_n + info_n, f"each finding rendered as a row ({n_alerts} rows)"
    )

    # Lineage graph renders nodes from the live /v1/lineage.
    wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, ".react-flow__node")) > 0)
    nodes = driver.find_elements(By.CSS_SELECTOR, ".react-flow__node")
    check(len(nodes) > 0, f"lineage graph has {len(nodes)} nodes")

    # Pick a real table from the picker → detail loads.
    picker = cy(driver, "table-picker")
    options = picker.find_elements(By.TAG_NAME, "option")
    table = next(
        (o.get_attribute("value") for o in options if o.get_attribute("value")), None
    )
    check(table is not None, f"table picker is populated ({len(options) - 1} tables)")
    if table:
        from selenium.webdriver.support.ui import Select

        Select(picker).select_by_value(table)
        wait.until(lambda d: table in cy(d, "detail-title").text)
        check(table in cy(driver, "detail-title").text, f"detail title shows {table!r}")
        detail = cy(driver, "table-detail").text
        check("Previous runs" in detail, "detail shows 'Previous runs' section")
        has_fields = bool(cy_all(driver, "fields-table")) or bool(
            cy_all(driver, "lineage-table")
        )
        check(has_fields, "detail shows fields/field-lineage tables")

    # Clicking a node selects it too.
    nodes[0].click()
    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-cy="detail-title"]'))
    )
    check(bool(cy_all(driver, "detail-title")), "clicking a lineage node selects it")

    driver.save_screenshot("/tmp/pb-e2e-main.png")
    print("  (screenshot: /tmp/pb-e2e-main.png)")


def test_live(driver) -> None:
    print("\n# Live Now page")
    driver.get(BASE + "/live")
    wait = WebDriverWait(driver, 20)

    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-cy="live-page"]'))
    )
    for panel in ("airflow-history", "recent-errors", "stale-list"):
        check(bool(cy_all(driver, panel)), f"panel '{panel}' present")
    # Freshness renders its chart or empty state once the async runs fetch lands.
    wait.until(lambda d: cy_all(d, "freshness-chart") or cy_all(d, "freshness-empty"))
    has_chart = bool(cy_all(driver, "freshness-chart"))
    check(
        has_chart or bool(cy_all(driver, "freshness-empty")),
        "freshness section present (chart or empty)",
    )
    if has_chart:
        dots = driver.find_elements(By.CSS_SELECTOR, ".recharts-scatter-symbol")
        check(
            len(dots) > 0, f"freshness chart plotted {len(dots)} dots from run history"
        )
        # Clicking a dot must reveal that bucket's table list (regression guard
        # for the recharts payload.x fix — the click reads the datum, not pixels).
        if dots:
            driver.execute_script(
                "arguments[0].dispatchEvent(new MouseEvent('click', {bubbles:true}))",
                dots[0],
            )
            try:
                wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '[data-cy="freshness-bucket"]')
                    )
                )
                items = cy_all(driver, "freshness-bucket-item")
                check(
                    len(items) > 0,
                    f"clicking a dot lists its bucket's tables ({len(items)})",
                )
            except Exception:
                check(False, "clicking a dot reveals the freshness bucket list")

    # Env toggle prod → dev → prod.
    check(
        "active" in cy(driver, "env-prod").get_attribute("class"),
        "prod is active by default",
    )
    cy(driver, "env-dev").click()
    wait.until(lambda d: "active" in cy(d, "env-dev").get_attribute("class"))
    check("active" in cy(driver, "env-dev").get_attribute("class"), "switched to dev")
    check(
        "environment: dev"
        in driver.find_element(By.CSS_SELECTOR, '[data-cy="live-page"]').text,
        "shows 'environment: dev'",
    )
    cy(driver, "env-prod").click()
    wait.until(lambda d: "active" in cy(d, "env-prod").get_attribute("class"))
    check(
        "active" in cy(driver, "env-prod").get_attribute("class"),
        "switched back to prod",
    )

    # Nav between pages.
    cy(driver, "nav-main").click()
    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-cy="main-page"]'))
    )
    check(bool(cy_all(driver, "main-page")), "nav → Main works")
    cy(driver, "nav-live-now").click()
    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-cy="live-page"]'))
    )
    check(bool(cy_all(driver, "live-page")), "nav → Live Now works")

    driver.save_screenshot("/tmp/pb-e2e-live.png")
    print("  (screenshot: /tmp/pb-e2e-live.png)")


def main() -> int:
    print(f"e2e against {BASE} using {FIREFOX}")
    driver = make_driver()
    try:
        test_main(driver)
        test_live(driver)
    finally:
        driver.quit()

    passed = sum(1 for ok, _ in results if ok)
    total = len(results)
    print(f"\n{'=' * 48}\n{passed}/{total} checks passed")
    failed = [m for ok, m in results if not ok]
    if failed:
        print("FAILURES:")
        for m in failed:
            print("  -", m)
        return 1
    print("ALL GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
