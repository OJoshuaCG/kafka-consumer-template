"""Tests del módulo de exceptions — captura de file/line, jerarquía, etc."""

from __future__ import annotations

import pytest

from src.core.exceptions import DomainError, NonRetryableError, RetryableError


class TestDomainError:
    def test_captures_file_function_line(self) -> None:
        # Esta línea es donde se lanza — la regex que la capture debería matchear
        with pytest.raises(DomainError) as exc_info:
            raise NonRetryableError("boom", context={"foo": "bar"})

        exc = exc_info.value
        assert exc.message == "boom"
        assert exc.context == {"foo": "bar"}
        assert exc.loc["function"] == "test_captures_file_function_line"
        assert exc.loc["file"].endswith("test_exceptions.py")
        assert isinstance(exc.loc["line"], int)
        assert exc.loc["code"] is not None
        assert "boom" in exc.loc["code"]

    def test_to_log_fields_includes_loc(self) -> None:
        exc = NonRetryableError("oops", context={"k": "v"}, extra_field="foo")
        fields = exc.to_log_fields()
        assert fields["error_message"] == "oops"
        assert fields["error_context"] == {"k": "v"}
        assert fields["error_extra"] == {"extra_field": "foo"}
        assert "error_file" in fields
        assert "error_function" in fields
        assert "error_line" in fields

    def test_subclass_hierarchy(self) -> None:
        assert issubclass(RetryableError, DomainError)
        assert issubclass(NonRetryableError, DomainError)
        assert not issubclass(RetryableError, NonRetryableError)

    def test_can_be_caught_as_domain_error(self) -> None:
        with pytest.raises(DomainError):
            raise RetryableError("transient")
