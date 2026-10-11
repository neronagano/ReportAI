class ApplicationError(Exception):
    """An expected business failure safe to expose to an API client."""

    def __init__(self, code: str, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def not_found(resource: str) -> ApplicationError:
    return ApplicationError("not_found", f"{resource} not found", 404)


class PermanentMessageError(Exception):
    """A message that cannot be processed without correcting its contents."""
