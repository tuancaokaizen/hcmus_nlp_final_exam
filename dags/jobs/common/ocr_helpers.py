from __future__ import annotations

from io import BytesIO

PADDLE_MAX_IMAGE_SIDE = 2400


def resize_png_bytes(image_bytes: bytes, max_side: int = PADDLE_MAX_IMAGE_SIDE) -> bytes:
    # Downscale PNG so long side <= max_side / Thu nhỏ PNG nếu cạnh dài vượt max_side
    try:
        from PIL import Image
    except ModuleNotFoundError as exc:
        raise RuntimeError("Missing dependency 'Pillow'.") from exc

    image = Image.open(BytesIO(image_bytes))
    width, height = image.size
    longest = max(width, height)
    if longest <= max_side:
        return image_bytes

    scale = max_side / float(longest)
    new_size = (max(1, int(width * scale)), max(1, int(height * scale)))
    resized = image.resize(new_size, Image.Resampling.LANCZOS)
    buffer = BytesIO()
    resized.save(buffer, format="PNG")
    return buffer.getvalue()
