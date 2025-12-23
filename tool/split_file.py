# split_file.py
from pathlib import Path

def split_file(file_path: str, chunk_size_mb: int = 50) -> None:
    file_path = Path(file_path)
    chunk_size = chunk_size_mb * 1024 * 1024  # MB -> bytes
    with file_path.open("rb") as f:
        index = 0
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            part_name = file_path.with_suffix(file_path.suffix + f".part{index:03d}")
            with part_name.open("wb") as part:
                part.write(chunk)
            print(f"write {part_name} ({len(chunk)} bytes)")
            index += 1

if __name__ == "__main__":
    split_file("pyc.tar.gz", chunk_size_mb=50)

