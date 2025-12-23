# join_file.py
from pathlib import Path

def join_file(base_name: str, output_name: str = None) -> None:
    base = Path(base_name)
    if output_name is None:
        output_name = base.name
    output = Path(output_name)

    index = 0
    with output.open("wb") as out:
        while True:
            part_name = base.with_suffix(base.suffix + f".part{index:03d}")
            if not part_name.exists():
                break
            print(f"read {part_name}")
            with part_name.open("rb") as part:
                while True:
                    chunk = part.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
            index += 1
    print(f"merged into {output} (parts: {index})")

if __name__ == "__main__":
    join_file("pyc.tar.gz")
