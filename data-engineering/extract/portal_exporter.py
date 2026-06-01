import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from playwright.sync_api import Playwright, TimeoutError as PWTimeout, sync_playwright

from config.env_loader import load_project_env


load_project_env()


def env_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()

    return default


PORTAL_URL = env_value(
    "PORTAL_BASE_URL",
    "PORTAL_URL",
    default="https://smartmanagement.smollan.com/#/login",
)
PORTAL_USER = env_value("PORTAL_USERNAME", "PORTAL_USER")
PORTAL_PASS = env_value("PORTAL_PASSWORD", "PORTAL_PASS")
PORTAL_ENTITY = os.getenv("PORTAL_ENTITY", "Morocco Unilever").strip()
DATA_DUMP_REPORT_NAME = os.getenv("DATA_DUMP_REPORT_NAME", "Data Dump").strip()
COVERAGE_REPORT_NAME = os.getenv("COVERAGE_REPORT_NAME", "Coverage Data").strip()
COVERAGE_LOOKBACK_DAYS = int(os.getenv("COVERAGE_LOOKBACK_DAYS", "3"))

DEFAULT_DOWNLOAD_DIR = Path(__file__).resolve().parents[1] / "downloads"

HEADLESS = os.getenv("PORTAL_HEADLESS", "false").lower() in {"1", "true", "yes", "y"}
SLOW_MO_MS = int(os.getenv("PORTAL_SLOW_MO_MS", "250"))
DEFAULT_TIMEOUT_MS = int(os.getenv("PORTAL_TIMEOUT_MS", "30000"))
EXPORT_TIMEOUT_MS = int(os.getenv("PORTAL_EXPORT_TIMEOUT_MS", "300000"))


def resolve_download_dir(download_dir=None) -> Path:
    if download_dir:
        return Path(download_dir).expanduser().resolve()

    env_dir = env_value("DOWNLOAD_DIR", "UNILEVER_DOWNLOAD_DIR", "UNILEVER_INBOUND_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()

    return DEFAULT_DOWNLOAD_DIR


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
    last_error = None
    for _ in range(3):
        try:
            target = locator.first
            target.wait_for(state="visible", timeout=timeout)
            target.scroll_into_view_if_needed(timeout=timeout)
            target.click(force=True, timeout=timeout)
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.5)

    raise last_error


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


def _safe_filename_part(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_")
    return name or "Portal_Report"


def _target_file_path(download_dir: Path, file_prefix: str) -> Path:
    now = datetime.now()
    month_dir = download_dir / now.strftime("%B")
    month_dir.mkdir(parents=True, exist_ok=True)

    base_name = f"UL__{_safe_filename_part(file_prefix)}-{now.strftime('%d-%b').upper()}"
    target_file = month_dir / f"{base_name}.xlsx"

    if target_file.exists():
        target_file = month_dir / f"{base_name}-{now.strftime('%H%M%S')}.xlsx"

    return target_file


def _format_portal_date(value: date) -> str:
    return f"{value.month}/{value.day}/{value.year}"


def _set_input_value(input_locator, value: str):
    input_locator.scroll_into_view_if_needed()
    input_locator.evaluate(
        """
        (element, value) => {
            element.removeAttribute('readonly');
            element.value = value;
            element.dispatchEvent(new Event('input', { bubbles: true }));
            element.dispatchEvent(new Event('change', { bubbles: true }));
            element.dispatchEvent(new Event('blur', { bubbles: true }));
        }
        """,
        value,
    )


def _apply_coverage_date_filter(page, start_date: date, end_date: date):
    filter_btn = find_first_visible(
        [
            page.locator("a,button").filter(has=page.locator(".fa-filter, .fas.fa-filter, i[class*='filter']")),
            page.locator("a[mattooltip='Filter'], button[mattooltip='Filter']"),
            page.locator("a,button").filter(has_text=re.compile(r"^\s*Filter\s*$", re.I)),
        ],
        timeout=60000,
    )
    safe_click(filter_btn, timeout=10000)

    dialog = find_first_visible(
        [
            page.locator("mat-dialog-container"),
            page.locator(".p-dialog:visible"),
            page.locator("p-dialog:visible"),
            page.locator(".modal:visible, [role='dialog']:visible"),
        ],
        timeout=20000,
    )

    try:
        select_type = dialog.locator("mat-select").first
        if select_type.count() > 0:
            safe_click(select_type, timeout=5000)
            by_date = find_first_visible(
                [
                    page.get_by_role("option", name=re.compile(r"^\s*By Date\s*$", re.I)),
                    page.locator("mat-option").filter(has_text=re.compile(r"By Date", re.I)),
                    page.get_by_text(re.compile(r"^\s*By Date\s*$", re.I)),
                ],
                timeout=8000,
            )
            safe_click(by_date, timeout=5000)
    except Exception:
        pass

    inputs = dialog.locator("input:visible")
    if inputs.count() < 2:
        inputs = page.locator("input:visible")
    if inputs.count() < 2:
        raise RuntimeError("Coverage export date dialog did not show start/end date inputs.")

    _set_input_value(inputs.nth(0), _format_portal_date(start_date))
    _set_input_value(inputs.nth(1), _format_portal_date(end_date))

    submit_btn = find_first_visible(
        [
            dialog.get_by_role("button", name=re.compile(r"^\s*Submit\s*$", re.I)),
            dialog.locator("button").filter(has_text=re.compile(r"Submit", re.I)),
        ],
        timeout=10000,
    )
    safe_click(submit_btn, timeout=10000)
    wait_overlay_gone(page, timeout=60000)


def _login_select_entity_and_open_reports(page):
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

    find_first_visible(
        [page.get_by_text(re.compile(r"^\s*Change Entity\s*$", re.I))],
        timeout=10000,
    )

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
    _open_reports_page(page)


def _open_reports_page(page):
    report_item = find_first_visible(
        [
            page.locator("li.list-group-item").filter(has_text=re.compile(r"^\s*Report\s*$", re.I)),
            page.get_by_role("link", name=re.compile(r"^\s*Report\s*$", re.I)),
            page.get_by_text(re.compile(r"^\s*Report\s*$", re.I)),
        ],
        timeout=20000,
    )

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


def _open_report_detail(page, report_name):
    _open_reports_page(page)
    if page.get_by_text(re.compile(r"No Rows To Show", re.I)).count() > 0:
        current_entity = page.locator("app-user-profile-new .project-name").first.inner_text().strip()
        raise RuntimeError(f"Reports grid is empty for entity: {current_entity}")

    report_words = [re.escape(part) for part in re.split(r"\s+", report_name) if part]
    row_pattern = re.compile(r"\s*".join(report_words), re.I)
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


def _download_open_report(page, download_dir, file_prefix, report_kind="data_dump", start_date=None, end_date=None):
    export_btn = find_first_visible(
        [
            page.locator("a,button").filter(has_text=re.compile(r"\+?\s*Export\s*report", re.I)),
            page.get_by_role("button", name=re.compile(r"Export\s*report", re.I)),
            page.get_by_role("link", name=re.compile(r"Export\s*report", re.I)),
        ],
        timeout=60000,
    )

    if report_kind == "coverage":
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=max(COVERAGE_LOOKBACK_DAYS - 1, 0))
        _apply_coverage_date_filter(page, start_date, end_date)
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
    for _ in range(2):
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

    target_file = _target_file_path(download_dir, file_prefix)
    download.save_as(str(target_file))
    return str(target_file)


def run(
    playwright: Playwright,
    download_dir=None,
    report_name=None,
    file_prefix=None,
    report_kind="data_dump",
    start_date: date | None = None,
    end_date: date | None = None,
) -> str:
    report_name = (report_name or DATA_DUMP_REPORT_NAME).strip()
    file_prefix = (file_prefix or report_name).strip()

    if not PORTAL_USER or not PORTAL_PASS:
        raise RuntimeError("Missing PORTAL_USERNAME or PORTAL_PASSWORD environment variables.")

    resolved_download_dir = resolve_download_dir(download_dir)
    resolved_download_dir.mkdir(parents=True, exist_ok=True)

    browser = playwright.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    page.set_default_timeout(DEFAULT_TIMEOUT_MS)

    try:
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

        find_first_visible(
            [page.get_by_text(re.compile(r"^\s*Change Entity\s*$", re.I))],
            timeout=10000,
        )

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

        report_item = find_first_visible(
            [
                page.locator("li.list-group-item").filter(has_text=re.compile(r"^\s*Report\s*$", re.I)),
                page.get_by_text(re.compile(r"^\s*Report\s*$", re.I)),
            ],
            timeout=20000,
        )

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

        report_words = [re.escape(part) for part in re.split(r"\s+", report_name) if part]
        row_pattern = re.compile(r"\s*".join(report_words), re.I)
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

        export_btn = find_first_visible(
            [
                page.locator("a,button").filter(has_text=re.compile(r"\+?\s*Export\s*report", re.I)),
                page.get_by_role("button", name=re.compile(r"Export\s*report", re.I)),
                page.get_by_role("link", name=re.compile(r"Export\s*report", re.I)),
            ],
            timeout=60000,
        )

        if report_kind == "coverage":
            if end_date is None:
                end_date = date.today()
            if start_date is None:
                start_date = end_date - timedelta(days=max(COVERAGE_LOOKBACK_DAYS - 1, 0))
            _apply_coverage_date_filter(page, start_date, end_date)
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

        for _ in range(2):
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

        now = datetime.now()
        month_dir = resolved_download_dir / now.strftime("%B")
        month_dir.mkdir(parents=True, exist_ok=True)

        base_name = f"UL__{_safe_filename_part(file_prefix)}-{now.strftime('%d-%b').upper()}"
        target_file = month_dir / f"{base_name}.xlsx"

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


def download_report_from_portal(
    report_name,
    file_prefix=None,
    download_dir=None,
    report_kind="data_dump",
    start_date: date | None = None,
    end_date: date | None = None,
) -> str:
    with sync_playwright() as playwright:
        return run(
            playwright,
            download_dir=download_dir,
            report_name=report_name,
            file_prefix=file_prefix,
            report_kind=report_kind,
            start_date=start_date,
            end_date=end_date,
        )


def download_excel_from_portal(download_dir=None) -> str:
    return download_report_from_portal(
        DATA_DUMP_REPORT_NAME,
        file_prefix="Data_Dump",
        download_dir=download_dir,
    )


def download_coverage_from_portal(
    download_dir=None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> str:
    return download_report_from_portal(
        COVERAGE_REPORT_NAME,
        file_prefix="Coverage",
        download_dir=download_dir,
        report_kind="coverage",
        start_date=start_date,
        end_date=end_date,
    )


def download_daily_reports_from_portal(
    include_data_dump=True,
    include_coverage=True,
    download_dir=None,
    coverage_start_date: date | None = None,
    coverage_end_date: date | None = None,
) -> dict:
    if not include_data_dump and not include_coverage:
        return {}

    if not PORTAL_USER or not PORTAL_PASS:
        raise RuntimeError("Missing PORTAL_USERNAME or PORTAL_PASSWORD environment variables.")

    resolved_download_dir = resolve_download_dir(download_dir)
    resolved_download_dir.mkdir(parents=True, exist_ok=True)

    downloads = {}

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(DEFAULT_TIMEOUT_MS)

        try:
            _login_select_entity_and_open_reports(page)

            if include_data_dump:
                _open_report_detail(page, DATA_DUMP_REPORT_NAME)
                downloads["data_dump"] = _download_open_report(
                    page,
                    resolved_download_dir,
                    "Data_Dump",
                    report_kind="data_dump",
                )

            if include_coverage:
                _open_report_detail(page, COVERAGE_REPORT_NAME)
                downloads["coverage"] = _download_open_report(
                    page,
                    resolved_download_dir,
                    "Coverage",
                    report_kind="coverage",
                    start_date=coverage_start_date,
                    end_date=coverage_end_date,
                )

            return downloads

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
    out = download_excel_from_portal()
    print(f"Downloaded: {out}")
