"""Exceptions: UNA clase genérica parametrizable + 2 subclases vacías.

Filosofía portada de `AppHttpException`:
- NO se crean clases por cada caso de error de dominio.
- `DomainError` captura `message`, `context`, `extra`, y `file/function/line/code`.
- `RetryableError`/`NonRetryableError` son subclases vacías — existen SOLO para
  que el loop del BaseConsumer pueda dispatchear con `except` natural.
- "Usuario no encontrado", "firma inválida", etc → `NonRetryableError(message, context={...})`.
- NUNCA crear `UserNotFoundError`, `InvalidSignatureError`, etc — anti-patrón.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any


class DomainError(Exception):
    """Error genérico del dominio del consumer.

    Captura automáticamente file/function/line/code donde se lanza, vía
    `inspect.stack()`. El logger del BaseConsumer emite esos campos como
    parte del structured log.

    Usar SIEMPRE las subclases (`RetryableError` / `NonRetryableError`)
    para que el loop dispatchee correctamente. Esta clase base no debería
    instanciarse directamente.

    Ejemplo:
        raise NonRetryableError(
            "Usuario no encontrado",
            context={"user_id": user_id, "event_id": event_id},
        )
    """

    def __init__(
        self,
        message: str = "Error en procesamiento de mensaje",
        context: str | list[Any] | dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        self.message = message
        self.context = context
        self.extra = extra
        self.loc = self._caller_info()
        super().__init__(message)

    @staticmethod
    def _caller_info() -> dict[str, Any]:
        # stack()[0] = _caller_info, [1] = __init__, [2] = quien lanzó
        frame = inspect.stack()[2]
        absolute = Path(frame.filename)
        try:
            file_path = str(absolute.relative_to(Path.cwd())).replace("\\", "/")
        except ValueError:
            file_path = "/".join(absolute.parts[-2:])

        return {
            "file": file_path,
            "function": frame.function,
            "line": frame.lineno,
            "code": frame.code_context[0].strip() if frame.code_context else None,
        }

    def to_log_fields(self) -> dict[str, Any]:
        """Campos estructurados para el logger del BaseConsumer."""
        return {
            "error_message": self.message,
            "error_context": self.context,
            "error_extra": self.extra,
            "error_file": self.loc["file"],
            "error_function": self.loc["function"],
            "error_line": self.loc["line"],
            "error_code": self.loc["code"],
        }


class RetryableError(DomainError):
    """Transient: red caída, lock contention, 503 upstream, timeout.

    El BaseConsumer hará retry con backoff+jitter. Si supera `max_retries`,
    se manda a DLQ.
    """


class NonRetryableError(DomainError):
    """Permanente: schema inválido, FK que no existe, evento malformado.

    El BaseConsumer manda a DLQ inmediatamente, sin retry.
    """
