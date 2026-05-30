import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from extract.portal_exporter import download_daily_reports_from_portal
from pipelines import run_coverage_pipeline, run_data_dump_pipeline


class PipelineLauncher(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Merch Performance ETL Launcher")
        self.geometry("920x680")
        self.minsize(820, 600)

        self.log_queue = queue.Queue()
        self.is_running = False

        self.load_data_dump_var = tk.BooleanVar(value=False)
        self.load_coverage_var = tk.BooleanVar(value=False)
        self.source_var = tk.StringVar(value="portal")
        self.validation_var = tk.BooleanVar(value=False)
        self.data_dump_path_var = tk.StringVar()
        self.coverage_path_var = tk.StringVar()

        self.master_paths = {
            "Store Master": tk.StringVar(),
            "User Master": tk.StringVar(),
            "Assortment Master": tk.StringVar(),
            "Call Cycle Master": tk.StringVar(),
        }

        self._configure_style()
        self._build_ui()
        self.after(150, self._drain_logs)

    def _configure_style(self):
        style = ttk.Style(self)
        style.configure("TButton", padding=(8, 4))

    def _build_ui(self):
        shell = ttk.Frame(self, padding=16)
        shell.pack(fill="both", expand=True)

        ttk.Label(
            shell,
            text="Unilever Merch Performance ETL",
            font=("Segoe UI", 18, "bold"),
        ).pack(anchor="w")
        ttk.Label(
            shell,
            text="Daily execution files feed fact tables. Monthly master files stay separated as planning dimensions.",
        ).pack(anchor="w", pady=(4, 14))

        notebook = ttk.Notebook(shell)
        notebook.pack(fill="both", expand=True)

        daily_tab = ttk.Frame(notebook, padding=14)
        monthly_tab = ttk.Frame(notebook, padding=14)
        notebook.add(daily_tab, text="Daily Run")
        notebook.add(monthly_tab, text="Monthly Masters")

        self._build_daily_tab(daily_tab)
        self._build_monthly_tab(monthly_tab)

        log_panel = ttk.LabelFrame(shell, text="Run Log", padding=8)
        log_panel.pack(fill="both", expand=True, pady=(14, 0))

        text_frame = ttk.Frame(log_panel)
        text_frame.pack(fill="both", expand=True)
        self.log_text = tk.Text(text_frame, height=12, wrap="word", state="disabled")
        scroll = ttk.Scrollbar(text_frame, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scroll.set)
        self.log_text.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

    def _build_daily_tab(self, parent):
        datasets = ttk.LabelFrame(parent, text="Dataset", padding=12)
        datasets.pack(fill="x")
        ttk.Checkbutton(
            datasets,
            text="Data Dump: OSA, survey responses, detailed execution",
            variable=self.load_data_dump_var,
        ).pack(anchor="w", pady=2)
        ttk.Checkbutton(
            datasets,
            text="Coverage: planned vs executed operational monitoring",
            variable=self.load_coverage_var,
        ).pack(anchor="w", pady=2)

        source = ttk.LabelFrame(parent, text="Daily Source", padding=12)
        source.pack(fill="x", pady=(12, 0))
        ttk.Radiobutton(
            source,
            text="Extract selected files from portal",
            value="portal",
            variable=self.source_var,
        ).pack(anchor="w", pady=2)
        ttk.Radiobutton(
            source,
            text="Load Excel files from computer",
            value="local",
            variable=self.source_var,
        ).pack(anchor="w", pady=2)
        ttk.Label(
            source,
            text="Portal mode downloads all selected files first, then loads them into MySQL.",
        ).pack(anchor="w", pady=(6, 0))

        files = ttk.LabelFrame(parent, text="Input Files", padding=12)
        files.pack(fill="x", pady=(12, 0))
        self._file_row(files, "Data Dump Excel", self.data_dump_path_var)
        self._file_row(files, "Coverage Excel", self.coverage_path_var)

        options = ttk.Frame(parent)
        options.pack(fill="x", pady=(14, 0))
        ttk.Checkbutton(
            options,
            text="Run validation after Data Dump load",
            variable=self.validation_var,
        ).pack(side="left")
        ttk.Button(options, text="Run Daily Pipeline", command=self.run_daily_pipeline).pack(side="right")
        ttk.Button(options, text="Clear Log", command=self.clear_log).pack(side="right", padx=(0, 8))

    def _build_monthly_tab(self, parent):
        panel = ttk.LabelFrame(parent, text="Monthly Planning Masters", padding=12)
        panel.pack(fill="both", expand=True)
        ttk.Label(
            panel,
            text="These files are staged separately from daily execution files.",
        ).pack(anchor="w", pady=(2, 12))

        for label, variable in self.master_paths.items():
            self._file_row(panel, label, variable)

        actions = ttk.Frame(panel)
        actions.pack(fill="x", pady=(16, 0))
        ttk.Button(
            actions,
            text="Stage Monthly Master Selection",
            command=self.stage_monthly_masters,
        ).pack(side="left")

        ttk.Label(
            panel,
            text=(
                "The dimension loaders for dim_store_master, dim_user_master, "
                "dim_assortment_master, and dim_call_cycle_master are the next step."
            ),
        ).pack(anchor="w", pady=(16, 0))

    def _file_row(self, parent, label, variable):
        row = ttk.Frame(parent)
        row.pack(fill="x", pady=5)
        ttk.Label(row, text=label, width=22).pack(side="left")
        ttk.Entry(row, textvariable=variable).pack(side="left", fill="x", expand=True, padx=8)
        ttk.Button(row, text="Browse", command=lambda: self.choose_file(variable)).pack(side="left")

    def choose_file(self, variable):
        path = filedialog.askopenfilename(
            title="Choose Excel file",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            variable.set(path)

    def run_daily_pipeline(self):
        if self.is_running:
            messagebox.showinfo("Pipeline running", "Please wait for the current run to finish.")
            return

        load_data_dump = self.load_data_dump_var.get()
        load_coverage = self.load_coverage_var.get()
        if not load_data_dump and not load_coverage:
            messagebox.showinfo("No dataset selected", "Select Data Dump, Coverage, or both.")
            return

        source = self.source_var.get()
        data_dump_file = self.data_dump_path_var.get().strip()
        coverage_file = self.coverage_path_var.get().strip()

        if source == "local" and load_data_dump and not data_dump_file:
            messagebox.showerror("Missing file", "Choose the Data Dump Excel file.")
            return
        if source == "local" and load_coverage and not coverage_file:
            messagebox.showerror("Missing file", "Choose the Coverage Excel file.")
            return

        self.is_running = True
        self.config(cursor="watch")
        self._log("===== START DAILY PIPELINE =====")

        worker = threading.Thread(
            target=self._run_daily_worker,
            args=(load_data_dump, load_coverage, source, data_dump_file, coverage_file),
            daemon=True,
        )
        worker.start()

    def _run_daily_worker(self, load_data_dump, load_coverage, source, data_dump_file, coverage_file):
        try:
            if source == "portal":
                self._log("Downloading selected portal files in one browser session...")
                downloads = download_daily_reports_from_portal(
                    include_data_dump=load_data_dump,
                    include_coverage=load_coverage,
                )
                data_dump_file = downloads.get("data_dump") or data_dump_file
                coverage_file = downloads.get("coverage") or coverage_file
                self._log(f"Portal downloads completed: {downloads}")

            if load_data_dump:
                run_data_dump_pipeline(
                    data_dump_file,
                    downloaded_from_portal=(source == "portal"),
                    should_run_validation=self.validation_var.get(),
                    source_mode=source,
                    logger=self._log,
                )

            if load_coverage:
                run_coverage_pipeline(coverage_file, source_mode=source, logger=self._log)

            self._log("===== DAILY PIPELINE COMPLETED SUCCESSFULLY =====")
        except Exception as exc:
            error_message = str(exc)
            self._log(f"ERROR: {error_message}")
            self.after(0, lambda msg=error_message: messagebox.showerror("Pipeline failed", msg))
        finally:
            self.after(0, self._finish_run)

    def _finish_run(self):
        self.is_running = False
        self.config(cursor="")

    def stage_monthly_masters(self):
        selected = {
            name: Path(path).resolve()
            for name, path in ((name, var.get().strip()) for name, var in self.master_paths.items())
            if path
        }
        if not selected:
            messagebox.showinfo("Monthly masters", "Choose at least one master file first.")
            return

        self._log("Monthly master files selected:")
        for name, path in selected.items():
            self._log(f"- {name}: {path}")
        self._log("Monthly master ETL is staged. Dimension loaders are still pending.")

    def _log(self, message):
        self.log_queue.put(str(message))

    def _drain_logs(self):
        while True:
            try:
                message = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_text.configure(state="normal")
            self.log_text.insert("end", message + "\n")
            self.log_text.see("end")
            self.log_text.configure(state="disabled")
        self.after(150, self._drain_logs)

    def clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")


def launch_desktop_app():
    app = PipelineLauncher()
    app.mainloop()
