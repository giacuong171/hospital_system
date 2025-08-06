import subprocess
from pathlib import Path


def run_md5sum(file_path):
    """
    Runs `md5sum` on a file and returns just the checksum string.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"{file_path} not found.")

    result = subprocess.run(["md5sum", str(file_path)], capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"md5sum failed: {result.stderr.strip()}")

    checksum, _ = result.stdout.strip().split()
    return checksum


def write_md5_file(file_path, suffix=".md5"):
    """
    Writes a .md5 file next to the original file containing the checksum.
    """
    file_path = Path(file_path)
    checksum = run_md5sum(file_path)

    checksum_file = file_path.with_suffix(file_path.suffix + suffix)
    with open(checksum_file, "w") as f:
        f.write(f"{checksum}  {file_path.name}\n")

    return checksum_file
