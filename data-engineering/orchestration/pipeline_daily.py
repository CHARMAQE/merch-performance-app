import subprocess
import sys
from pathlib import Path


def run(cmd, cwd):
    print("Running:", " ".join(cmd))
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def main():
    orchestration_dir = Path(__file__).resolve().parent
    data_engineering_dir = orchestration_dir.parent
    py = sys.executable

    run([py, "-m", "extract.portal_exporter"], cwd=data_engineering_dir)
    run([py, "-m", "orchestration.etl_daily_runner"], cwd=data_engineering_dir)

    print("Pipeline completed successfully")


if __name__ == "__main__":
    main()