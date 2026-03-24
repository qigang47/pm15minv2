from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Iterator


def _compress_zstd(payload: bytes, level: int) -> bytes:
    try:
        import zstandard as zstd  # type: ignore

        compressor = zstd.ZstdCompressor(level=int(level))
        return compressor.compress(payload)
    except Exception:
        cmd = ["zstd", "-q", "-c", f"-{int(level)}", "-T1"]
        try:
            proc = subprocess.run(cmd, input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        except FileNotFoundError as exc:
            raise RuntimeError("zstd is not available; install zstd or python-zstandard") from exc
        if proc.returncode != 0:
            stderr = (proc.stderr or b"").decode("utf-8", errors="replace")
            raise RuntimeError(f"zstd compression failed: {stderr}")
        return proc.stdout


def append_ndjson_zst(path: Path, records: list[dict[str, object]], *, level: int = 7) -> Path:
    if not records:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        for record in records
    ).encode("utf-8")
    compressed = _compress_zstd(payload, level=level)
    with path.open("ab") as fh:
        fh.write(compressed)
    return path


def iter_ndjson_zst(path: Path) -> Iterator[dict[str, object]]:
    try:
        import zstandard as zstd  # type: ignore

        decompressor = zstd.ZstdDecompressor()
        with path.open("rb") as fh, decompressor.stream_reader(fh) as reader:
            buffer = b""
            while True:
                chunk = reader.read(1 << 20)
                if not chunk:
                    break
                buffer += chunk
                lines = buffer.splitlines(keepends=False)
                if buffer and not buffer.endswith(b"\n"):
                    buffer = lines.pop() if lines else buffer
                else:
                    buffer = b""
                for raw in lines:
                    text = raw.decode("utf-8").strip()
                    if text:
                        yield json.loads(text)
            tail = buffer.decode("utf-8").strip()
            if tail:
                yield json.loads(tail)
        return
    except Exception:
        pass

    proc = subprocess.Popen(
        ["zstd", "-q", "-d", "-c", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    assert proc.stdout is not None
    try:
        for line in proc.stdout:
            text = line.strip()
            if text:
                yield json.loads(text)
    finally:
        proc.stdout.close()
        stderr = proc.stderr.read() if proc.stderr is not None else ""
        returncode = proc.wait()
        if returncode != 0:
            raise RuntimeError(f"zstd decompression failed for {path}: {stderr}")
