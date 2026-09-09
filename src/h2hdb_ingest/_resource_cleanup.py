"""Release all local resources without replacing the operation failure."""

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Protocol


class Closeable(Protocol):
    def close(self) -> None: ...


def close_resources(
    resources: Iterable[Closeable], *, error: BaseException | None = None
) -> None:
    primary = error
    for resource in resources:
        try:
            resource.close()
        except BaseException as cleanup_error:
            if primary is None:
                primary = cleanup_error
            elif cleanup_error is not primary:
                primary.add_note(
                    f"Additional resource cleanup failed: {cleanup_error!r}"
                )
    if error is None and primary is not None:
        raise primary


@contextmanager
def owned_resource[T: Closeable](resource: T) -> Iterator[T]:
    try:
        yield resource
    except BaseException as error:
        close_resources((resource,), error=error)
        raise
    else:
        close_resources((resource,))
