from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

# Ensure `common` package is importable when Airflow parses files directly / Đảm bảo import được `common` khi Airflow parse trực tiếp file
JOBS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if JOBS_DIR not in sys.path:
    sys.path.append(JOBS_DIR)

from common.config import get_value, load_config

if TYPE_CHECKING:
    from minio import Minio


def _parse_minio_endpoint(endpoint: str) -> tuple[str, bool]:
    # Support endpoint with or without scheme / Hỗ trợ endpoint có hoặc không có scheme
    if "://" in endpoint:
        parsed = urlparse(endpoint)
        host = parsed.netloc or parsed.path
        secure = parsed.scheme == "https"
        return host, secure
    return endpoint, False


def get_minio_client(
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    secure: bool | None = None,
) -> "Minio":
    # Create reusable MinIO client from config / Tạo MinIO client từ config
    try:
        from minio import Minio
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing dependency 'minio'. Install it in Airflow runtime before uploading artifacts."
        ) from exc

    cfg = load_config()
    # Laptop port-forward: FEN_MINIO_ENDPOINT=http://127.0.0.1:9000 /
    # Port-forward máy local: FEN_MINIO_ENDPOINT=http://127.0.0.1:9000
    env_endpoint = (os.environ.get("FEN_MINIO_ENDPOINT") or "").strip() or None
    raw_endpoint = endpoint or env_endpoint or get_value(cfg, "minio", "endpoint")
    host, parsed_secure = _parse_minio_endpoint(raw_endpoint)
    return Minio(
        host,
        access_key=access_key or get_value(cfg, "minio", "access_key"),
        secret_key=secret_key or get_value(cfg, "minio", "secret_key"),
        secure=parsed_secure if secure is None else secure,
    )


def upload_file(client: "Minio", bucket: str, object_name: str, local_path: Path) -> None:
    # Upload generated artifact to object storage / Upload output lên object storage
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket_name=bucket, object_name=object_name, file_path=str(local_path))


def _unique_temp_path(suffix: str) -> Path:
    """Create a unique temp file path safe for parallel workers.

    Tạo đường dẫn file tạm duy nhất, an toàn khi nhiều worker song song.
    Basename-only paths like /tmp/1.jpg.tmp.json collide across posts /
    Chỉ dùng basename như /tmp/1.jpg.tmp.json sẽ trùng giữa các post.
    """
    fd, raw_path = tempfile.mkstemp(prefix="fen-upload-", suffix=suffix)
    os.close(fd)
    return Path(raw_path)


def _upload_temp_file(
    client: "Minio",
    bucket: str,
    object_name: str,
    temp_path: Path,
) -> str:
    """Upload then delete temp file / Upload rồi xóa file tạm."""
    try:
        upload_file(client, bucket, object_name, temp_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    return f"{bucket}/{object_name}"


def ensure_bucket(client: "Minio", bucket: str) -> None:
    # Create bucket only when missing / Chỉ tạo bucket khi chưa tồn tại
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def upload_files_with_prefix(
    bucket: str, local_dir: Path, object_prefix: str, glob_pattern: str
) -> list[str]:
    # Upload all files matching pattern under a MinIO prefix / Upload tất cả file theo pattern lên MinIO prefix
    client = get_minio_client()
    ensure_bucket(client, bucket)
    uploaded: list[str] = []
    for file_path in sorted(local_dir.glob(glob_pattern)):
        object_name = f"{object_prefix}/{file_path.name}"
        client.fput_object(bucket_name=bucket, object_name=object_name, file_path=str(file_path))
        uploaded.append(f"{bucket}/{object_name}")
    return uploaded


def v2_preprocessed_page_key(doc_id: str, page_no: int) -> str:
    # v2 PNG key in fen-preprocessed bucket / Key PNG v2 trong bucket fen-preprocessed
    return f"{doc_id}/page_{page_no:04d}.png"


def v2_ocr_page_key(doc_id: str, page_no: int) -> str:
    # v2 OCR JSON key in fen-ocr bucket / Key JSON OCR v2 trong bucket fen-ocr
    return f"{doc_id}/page_{page_no:04d}.json"


def v2_aligned_page_key(doc_id: str, page_no: int) -> str:
    # v2 aligned JSON key in fen-aligned bucket / Key JSON align v2 trong bucket fen-aligned
    return f"{doc_id}/page_{page_no:04d}.json"


def v2_catalog_key(doc_id: str) -> str:
    # Catalog JSON key (STT index) / Key JSON catalog theo STT
    return f"{doc_id}/catalog.json"


def v2_entry_key(doc_id: str, stt: int, tap_id: str | None = None, *, suffix: str | None = None) -> str:
    """Entry JSON key scoped by tap_id (STT resets per volume).

    `suffix` disambiguates a genuine STT-number collision (two different
    physical entries sharing the same stt within a tap) so the second
    entry gets its own file instead of overwriting the first /
    Key JSON entry theo tap_id — STT reset mỗi tập. `suffix` dùng để tách
    file khi 2 entry khác nhau bị trùng số STT trong cùng 1 tập, tránh
    entry sau ghi đè mất entry trước.
    """
    stem = f"stt_{stt:04d}" if not suffix else f"stt_{stt:04d}_{suffix}"
    if tap_id:
        return f"{doc_id}/taps/{tap_id}/{stem}.json"
    # Legacy path without tap / Path cũ khi chưa có tap
    return f"{doc_id}/{stem}.json"


def entry_object_key(doc_id: str, entry: dict) -> str:
    # Resolve MinIO key from entry.tap / Suy ra key MinIO từ entry.tap
    tap = entry.get("tap") if isinstance(entry, dict) else None
    tap_id = None
    if isinstance(tap, dict):
        tap_id = tap.get("tap_id")
    elif isinstance(entry, dict):
        tap_id = entry.get("tap_id")
    # entry_id carries a "__dupN" marker when build_catalog disambiguated a
    # genuine STT collision — reuse it as the file suffix so both entries
    # persist under distinct keys / entry_id có hậu tố "__dupN" khi
    # build_catalog tách 2 entry trùng STT — dùng lại làm suffix file để
    # cả hai đều được lưu riêng, không ghi đè nhau
    entry_id = str(entry.get("entry_id") or "")
    suffix = entry_id.rsplit("__dup", 1)[1] if "__dup" in entry_id else None
    dup_suffix = f"dup{suffix}" if suffix else None
    return v2_entry_key(doc_id, int(entry["stt"]), str(tap_id) if tap_id else None, suffix=dup_suffix)


def object_exists(bucket: str, object_name: str) -> bool:
    # Check whether MinIO object already exists / Kiểm tra object MinIO đã tồn tại chưa
    from minio.error import S3Error

    client = get_minio_client()
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error as exc:
        if getattr(exc, "code", "") in {"NoSuchKey", "NoSuchBucket", "NotFound"}:
            return False
        # Some MinIO setups raise 404 without NoSuchKey / Một số MinIO trả 404 không kèm NoSuchKey
        if "NoSuchKey" in str(exc) or "not found" in str(exc).lower():
            return False
        raise


def delete_object(bucket: str, object_name: str) -> bool:
    # Delete one object if present / Xóa một object nếu có
    client = get_minio_client()
    if not client.bucket_exists(bucket):
        return False
    if not object_exists(bucket, object_name):
        return False
    client.remove_object(bucket, object_name)
    return True


def delete_objects_with_prefix(bucket: str, prefix: str, suffix: str | None = None) -> int:
    # Delete objects under prefix (optional suffix) / Xóa object theo prefix (tuỳ chọn lọc đuôi)
    client = get_minio_client()
    if not client.bucket_exists(bucket):
        return 0
    keys = list_objects_with_prefix(bucket=bucket, prefix=prefix, suffix=suffix)
    for key in keys:
        client.remove_object(bucket, key)
    return len(keys)


def preprocessed_page_object_key(doc_id: str, page_no: int, prefix: str | None = None) -> str:
    # Build MinIO key for denoised page PNG / Tạo object key PNG trang đã lọc nhiễu
    if prefix is None:
        return v2_preprocessed_page_key(doc_id, page_no)
    normalized_prefix = prefix.strip("/")
    if not normalized_prefix:
        return v2_preprocessed_page_key(doc_id, page_no)
    return f"{normalized_prefix}/{doc_id}/page_{page_no:04d}.png"


def upload_png_bytes(bucket: str, object_name: str, png_bytes: bytes) -> str:
    # Upload PNG bytes to MinIO / Upload bytes PNG lên MinIO
    temp_path = _unique_temp_path(".tmp.png")
    temp_path.write_bytes(png_bytes)
    client = get_minio_client()
    return _upload_temp_file(client, bucket, object_name, temp_path)


def upload_json_payload(bucket: str, object_name: str, payload: dict) -> str:
    # Upload in-memory JSON payload via temp file / Upload JSON trong bộ nhớ thông qua file tạm
    temp_path = _unique_temp_path(".tmp.json")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    client = get_minio_client()
    return _upload_temp_file(client, bucket, object_name, temp_path)


def upload_text_payload(bucket: str, object_name: str, text: str, suffix: str = ".txt") -> str:
    # Upload in-memory text (jsonl, csv...) via temp file / Upload text trong bộ nhớ (jsonl, csv...) qua file tạm
    temp_path = _unique_temp_path(f".tmp{suffix}")
    temp_path.write_text(text, encoding="utf-8")
    client = get_minio_client()
    return _upload_temp_file(client, bucket, object_name, temp_path)


def upload_binary_payload(bucket: str, object_name: str, data: bytes) -> str:
    # Upload raw bytes (images, archives...) via temp file / Upload bytes thô (ảnh, archive...) qua file tạm
    temp_path = _unique_temp_path(".tmp.bin")
    temp_path.write_bytes(data)
    client = get_minio_client()
    return _upload_temp_file(client, bucket, object_name, temp_path)


def read_object_text(bucket: str, object_name: str) -> str:
    # Read a small text object fully into memory / Đọc trọn một object text nhỏ vào bộ nhớ
    client = get_minio_client()
    response = client.get_object(bucket, object_name)
    try:
        return response.read().decode("utf-8")
    finally:
        response.close()
        response.release_conn()


def list_objects_with_prefix(bucket: str, prefix: str, suffix: str | None = None) -> list[str]:
    # List object keys under a prefix with optional suffix filter / Liệt kê object theo prefix, có thể lọc đuôi file
    client = get_minio_client()
    ensure_bucket(client, bucket)
    keys: list[str] = []
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if obj.is_dir:
            continue
        if suffix and not obj.object_name.endswith(suffix):
            continue
        keys.append(obj.object_name)
    return sorted(keys)


def download_object(bucket: str, object_name: str, local_path: Path) -> Path:
    # Download one object to local filesystem / Tải một object từ MinIO về local
    client = get_minio_client()
    local_path.parent.mkdir(parents=True, exist_ok=True)
    client.fget_object(bucket_name=bucket, object_name=object_name, file_path=str(local_path))
    return local_path
