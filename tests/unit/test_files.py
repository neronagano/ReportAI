from io import BytesIO

import pytest
from PIL import Image

from shiftledger.attachments.service import inspect_image
from shiftledger.errors import ApplicationError


def test_image_type_is_detected_from_content() -> None:
    stream = BytesIO()
    Image.new("RGB", (4, 4), "white").save(stream, format="PNG")
    assert inspect_image(stream.getvalue(), 100) == ("image/png", "png")


def test_arbitrary_bytes_are_not_accepted_as_receipts() -> None:
    with pytest.raises(ApplicationError):
        inspect_image(b"<script>alert('receipt')</script>", 100)


def test_image_dimensions_are_bounded() -> None:
    stream = BytesIO()
    Image.new("RGB", (11, 10), "white").save(stream, format="PNG")
    with pytest.raises(ApplicationError, match="dimensions"):
        inspect_image(stream.getvalue(), 100)
