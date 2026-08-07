__all__ = ["gallery_name_to_cbz_file_name"]

FILE_NAME_LENGTH_LIMIT = 255
CBZ_SUFFIX = ".cbz"


def gallery_name_to_cbz_file_name(gallery_name: str) -> str:
    """Return the historical friendly CBZ leaf for a gallery name."""
    while len(gallery_name.encode("utf-8")) + len(CBZ_SUFFIX) > FILE_NAME_LENGTH_LIMIT:
        gallery_name = gallery_name[1:]
    return f"{gallery_name}{CBZ_SUFFIX}"
