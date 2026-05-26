"""Healthcheck para K8s — exec probe contra un archivo con timestamp.

Pattern: escribir `time.time()` a `/tmp/healthcheck` cada N segundos.
El K8s `livenessProbe` corre `python -c "..."` que verifica que el archivo
sea reciente. Sin HTTP server aparte, sin overhead, sin puerto extra.

Si el consumer se cuelga, deja de escribir, y K8s lo mata.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path


class HealthCheckWriter:
    """Escribe timestamps a un archivo en intervalos regulares.

    Diseñado para correr como background task del BaseConsumer.
    """

    def __init__(
        self,
        path: Path | str = "/tmp/healthcheck",
        *,
        interval_seconds: float = 10.0,
    ) -> None:
        self._path = Path(path)
        self._interval = interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    def write_now(self) -> None:
        """Escribe el timestamp actual inmediatamente."""
        self._path.write_text(str(time.time()))

    async def _loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                self.write_now()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval)
                except TimeoutError:
                    continue
        except asyncio.CancelledError:
            pass

    async def start(self) -> None:
        """Arranca el loop de escritura como background task."""
        if self._task is not None:
            return
        self.write_now()  # primera escritura inmediata
        self._task = asyncio.create_task(self._loop(), name="healthcheck-writer")

    async def stop(self) -> None:
        """Detiene el loop y borra el archivo."""
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._path.unlink(missing_ok=True)
