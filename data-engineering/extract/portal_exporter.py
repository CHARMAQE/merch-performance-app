import os
import re
import time
from pathlib import Path
from datetime import datetime

from playwright.sync_api import Playwright, TimeoutError as PWTimeout, sync_playwright


PORTAL_URL = os.getenv("PORTAL_URL", "https://smartmanagement.smollan.com/#/login")
PORTAL_USER = os.getenv("PORTAL_USER", "")
PORTAL_PASS = os.getenv("PORTAL_PASS", "")
PORTAL_ENTITY = os.getenv("PORTAL_ENTITY", "Morocco Unilever").strip()

DOWNLOAD_DIR = Path(
    os.getenv("UNILEVER_INBOUND_DIR", r"C:\Users\hamza\CHARMAQE\Unilever\inbound")
)

HEADLESS = os.getenv("PORTAL_HEADLESS", "false").lower() in {"1", "true", "yes", "y"}
SLOW_MO_MS = int(os.getenv("PORTAL_SLOW_MO_MS", "250"))
DEFAULT_TIMEOUT_MS = int(os.getenv("PORTAL_TIMEOUT_MS", "30000"))
EXPORT_TIMEOUT_MS = int(os.getenv("PORTAL_EXPORT_TIMEOUT_MS", "300000"))


def debug_dump(page, name: str) -> None:
    try:
        page.screenshot(path=f"{name}.png", full_page=True)
    except Exception:
        pass
    try:
        with open(f"{name}.html", "w", encoding="utf-8") as f:
            f.write(page.content())
    except Exception:
        pass


def find_first_visible(locators, timeout=15000):
    end = time.time() + (timeout / 1000)
    while time.time() < end:
        for loc in locators:
            try:
                if loc.count() > 0 and loc.first.is_visible():
                    return loc.first
            except Exception:
                pass
        time.sleep(0.25)
    raise RuntimeError("No expected element became visible.")


def safe_click(locator, timeout=10000):
    locator.scroll_into_view_if_needed()
    locator.click(force=True, timeout=timeout)


def wait_overlay_gone(page, timeout=30000):
    end = time.time() + (timeout / 1000)
    while time.time() < end:
        try:
            loading = page.locator(".ngx-spinner-overlay:visible").count()
            backdrop = page.locator(
                ".cdk-overlay-backdrop.cdk-overlay-backdrop-showing:visible"
            ).count()
            if loading == 0 and backdrop == 0:
                return
        except Exception:
            return
        page.wait_for_timeout(250)


def run(playwright: Playwright) -> str:
    if not PORTAL_USER or not PORTAL_PASS:
        raise RuntimeError("Missing PORTAL_USER or PORTAL_PASS environment variables.")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    browser = playwright.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.set_default_timeout(DEFAULT_TIMEOUT_MS)

    try:
        # 1) Login
        page.goto(PORTAL_URL, wait_until="domcontentloaded")

        user_input = find_first_visible(
            [
                page.locator("#floatingInput"),
                page.locator("input[type='text']").first,
            ],
            timeout=20000,
        )
        pass_input = find_first_visible(
            [
                page.locator("#mat-input-1"),
                page.locator("input[type='password']").first,
            ],
            timeout=20000,
        )

        user_input.fill(PORTAL_USER)
        pass_input.fill(PORTAL_PASS)

        login_btn = find_first_visible(
            [
                page.get_by_role("button", name=re.compile(r"^\s*Login\s*$", re.I)),
                page.locator("button").filter(has_text=re.compile(r"login", re.I)),
            ],
            timeout=10000,
        )
        safe_click(login_btn, timeout=5000)
        wait_overlay_gone(page, timeout=30000)

        # 2) Open three-dots user menu, then find Change Entity from the opened menu
        more_btn = find_first_visible(
            [
                page.locator("app-user-profile-new mat-icon", has_text="more_vert"),
                page.locator("mat-icon", has_text="more_vert"),
                page.get_by_text("more_vert"),
            ],
            timeout=20000,
        )

        change_entity = None
        for _ in range(3):
            safe_click(more_btn, timeout=5000)
            try:
                change_entity = find_first_visible(
                    [
                        page.locator(".cdk-overlay-pane [role='menuitem']").filter(
                            has_text=re.compile(r"^\s*Change Entity\s*$", re.I)
                        ),
                        page.get_by_role("menuitem", name=re.compile(r"change\s*entity", re.I)),
                        page.get_by_text(re.compile(r"^\s*Change Entity\s*$", re.I)),
                    ],
                    timeout=4000,
                )
                break
            except Exception:
                page.wait_for_timeout(400)

        if change_entity is None:
            raise RuntimeError("Could not open user menu or find Change Entity item.")

        safe_click(change_entity, timeout=5000)

        # 3) Ensure Change Entity modal is open
        find_first_visible(
            [page.get_by_text(re.compile(r"^\s*Change Entity\s*$", re.I))],
            timeout=10000,
        )

        # 4) Open Select Entity dropdown first, then choose entity
        find_first_visible([page.get_by_text(re.compile(r"^\s*Change Entity\s*$", re.I))], timeout=10000)

        entity_select = find_first_visible(
            [
                page.locator("#mat-select-6"),
                page.locator("mat-select").nth(0),
            ],
            timeout=10000,
        )
        safe_click(entity_select, timeout=5000)

        if PORTAL_ENTITY:
            entity_pattern = re.compile(rf"^\s*{re.escape(PORTAL_ENTITY)}\s*$", re.I)
            try:
                entity_option = find_first_visible(
                    [
                        page.get_by_role("option", name=entity_pattern),
                        page.locator("mat-option").filter(has_text=entity_pattern),
                        page.get_by_text(entity_pattern),
                    ],
                    timeout=8000,
                )
                safe_click(entity_option, timeout=5000)
            except Exception:
                print(f"Entity '{PORTAL_ENTITY}' not found. Keeping current value.")
                try:
                    page.keyboard.press("Escape")
                except Exception:
                    pass

        done_btn = find_first_visible(
            [page.get_by_role("button", name=re.compile(r"^\s*Done\s*$", re.I))],
            timeout=10000,
        )
        safe_click(done_btn, timeout=5000)
        wait_overlay_gone(page, timeout=20000)

        # 5) Click Report and confirm page is really open
        report_item = find_first_visible(
            [
                page.locator("li.list-group-item").filter(has_text=re.compile(r"^\s*Report\s*$", re.I)),
                page.get_by_text(re.compile(r"^\s*Report\s*$", re.I)),
            ],
            timeout=20000,
        )

        # Detect empty report result early (instead of timing out on row search)
        if page.get_by_text(re.compile(r"No Rows To Show", re.I)).count() > 0:
            current_entity = page.locator("app-user-profile-new .project-name").first.inner_text().strip()
            raise RuntimeError(f"Reports grid is empty for entity: {current_entity}")

        for _ in range(3):
            safe_click(report_item, timeout=8000)
            page.wait_for_timeout(1200)
            on_report_page = (
                "/#/report" in page.url.lower()
                or page.get_by_role("heading", name=re.compile(r"^\s*Reports\s*$", re.I)).count() > 0
            )
            if on_report_page:
                break

        wait_overlay_gone(page, timeout=60000)
        try:
            page.locator("text=Loading...").first.wait_for(state="hidden", timeout=90000)
        except Exception:
            pass

        # 6) Click Data Dump Report row action (eye/button)
        row_pattern = re.compile(r"Data\s*Dump\s*-?\s*Report", re.I)
        row = find_first_visible(
            [
                page.get_by_role("row", name=row_pattern),
                page.locator("tr, .ag-row, .mat-row, .card, .list-group-item").filter(has_text=row_pattern),
            ],
            timeout=40000,
        )
        row.scroll_into_view_if_needed()

        row_action = find_first_visible(
            [
                row.get_by_role("button"),
                row.locator("button"),
                row.locator("a[role='button'], a"),
            ],
            timeout=10000,
        )
        safe_click(row_action, timeout=5000)
        wait_overlay_gone(page, timeout=30000)

                # 7) Click + Export report and download
        export_btn = find_first_visible(
            [
                page.locator("a,button").filter(has_text=re.compile(r"\+?\s*Export\s*report", re.I)),
                page.get_by_role("button", name=re.compile(r"Export\s*report", re.I)),
                page.get_by_role("link", name=re.compile(r"Export\s*report", re.I)),
            ],
            timeout=60000,
        )

        download = None
        last_error = None

        for attempt in range(2):
            try:
                with page.expect_download(timeout=EXPORT_TIMEOUT_MS) as download_info:
                    safe_click(export_btn, timeout=10000)
                download = download_info.value
                break
            except Exception as exc:
                last_error = exc
                page.wait_for_timeout(5000)
                wait_overlay_gone(page, timeout=15000)

        if download is None:
            debug_dump(page, "debug_timeout_export")
            raise RuntimeError(f"Export click succeeded but no download started: {last_error}")

        download = download_info.value

        now = datetime.now()
        month_name = now.strftime("%B")  # April, May, June...
        month_dir = DOWNLOAD_DIR / month_name
        month_dir.mkdir(parents=True, exist_ok=True)

        base_name = f"UL__Data_Dump-{now.strftime('%d-%b').upper()}"
        target_file = month_dir / f"{base_name}.xlsx"

        # Avoid overwrite if same-day file already exists
        if target_file.exists():
            target_file = month_dir / f"{base_name}-{now.strftime('%H%M%S')}.xlsx"

        download.save_as(str(target_file))

        return str(target_file)

    except PWTimeout as exc:
        debug_dump(page, "debug_timeout")
        raise RuntimeError(f"Playwright timeout: {exc}") from exc
    except Exception as exc:
        debug_dump(page, "debug_failure")
        raise RuntimeError(f"Automation failed: {exc}") from exc
    finally:
        context.close()
        browser.close()


if __name__ == "__main__":
    with sync_playwright() as playwright:
        out = run(playwright)
    print(f"Downloaded: {out}")