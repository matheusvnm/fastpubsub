"""Tests for DI handler and annotations."""

from typing import Annotated

import pytest
from fast_depends import Depends
from pydantic import BaseModel

from fastpubsub.di.annotations import Body, Context, Header
from fastpubsub.di.handler import Handler, _has_custom_field_annotation
from fastpubsub.di.repository import ContextRepository, context_repo


class TestHasCustomFieldAnnotation:
    """Tests for the _has_custom_field_annotation helper function."""

    def test_empty_annotation_returns_false(self):
        """Test that empty annotation returns False."""
        import inspect

        param = inspect.Parameter("test", inspect.Parameter.POSITIONAL_OR_KEYWORD)
        assert _has_custom_field_annotation(param) is False

    def test_annotated_with_header_returns_true(self):
        """Test that Annotated with Header returns True."""
        import inspect

        param = inspect.Parameter(
            "test",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Annotated[str, Header()],
        )
        assert _has_custom_field_annotation(param) is True

    def test_annotated_with_body_returns_true(self):
        """Test that Annotated with Body returns True."""
        import inspect

        param = inspect.Parameter(
            "test",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Annotated[str, Body()],
        )
        assert _has_custom_field_annotation(param) is True

    def test_annotated_with_context_returns_true(self):
        """Test that Annotated with Context returns True."""
        import inspect

        param = inspect.Parameter(
            "test",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Annotated[str, Context("some_key")],
        )
        assert _has_custom_field_annotation(param) is True

    def test_annotated_with_depends_returns_true(self):
        """Test that Annotated with Depends returns True."""
        import inspect

        def dependency() -> str:
            return "value"

        param = inspect.Parameter(
            "test",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Annotated[str, Depends(dependency)],
        )
        assert _has_custom_field_annotation(param) is True

    def test_plain_type_annotation_returns_false(self):
        """Test that plain type annotation returns False."""
        import inspect

        param = inspect.Parameter(
            "test",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=str,
        )
        assert _has_custom_field_annotation(param) is False

    def test_non_annotated_header_default_returns_false(self):
        """Test that non-Annotated Header as default returns False.

        When using `param: str = Header()` syntax (non-annotated),
        the annotation is just `str`, not the Header. The Header
        becomes the default value, not part of the annotation.
        """
        import inspect

        # This simulates: param: str = Header()
        # The annotation is str, Header() is the default
        param = inspect.Parameter(
            "test",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=str,
            default=Header(),
        )
        # This returns False because annotation is just `str`
        assert _has_custom_field_annotation(param) is False


class TestHandler:
    """Tests for Handler class."""

    def test_handler_stores_function_name(self):
        """Test that handler stores the function name."""

        async def my_handler():
            pass

        handler = Handler(my_handler)

        assert handler.name == "my_handler"

    def test_handler_identifies_unannotated_params(self):
        """Test that handler identifies parameters without custom field annotations.

        Note: The `_has_custom_field_annotation` function specifically checks for
        CustomField or Dependant instances inside Annotated types. A plain type
        annotation like `str` is considered "unannotated" for DI purposes.
        """

        async def handler(
            unannotated_param: str,
            another_unannotated: int,
        ):
            pass

        h = Handler(handler)

        assert "unannotated_param" in h.unannotated_param_names
        assert "another_unannotated" in h.unannotated_param_names

    def test_handler_with_annotated_header(self):
        """Test handler with Annotated Header param."""
        # Create the Header instance outside the function to avoid B008
        header_field = Header()

        async def handler(
            header_val: Annotated[str, header_field],
        ):
            pass

        h = Handler(handler)

        assert "header_val" not in h.unannotated_param_names

    def test_handler_with_annotated_body(self):
        """Test handler with Annotated Body param."""
        body_field = Body()

        async def handler(
            body_val: Annotated[str, body_field],
        ):
            pass

        h = Handler(handler)

        assert "body_val" not in h.unannotated_param_names

    def test_handler_with_no_params(self):
        """Test handler with no parameters."""

        async def handler():
            pass

        h = Handler(handler)

        assert len(h.unannotated_param_names) == 0

    def test_handler_with_depends_annotation(self):
        """Test that Depends-annotated params are not in unannotated list."""

        def get_service() -> str:
            return "service"

        async def handler(
            service: Annotated[str, Depends(get_service)],
            plain_param: str,
        ):
            pass

        h = Handler(handler)

        assert "service" not in h.unannotated_param_names
        assert "plain_param" in h.unannotated_param_names

    def test_handler_non_annotated_header_default(self):
        """Test non-annotated syntax: param: str = Header().

        When using `param: str = Header()` syntax (non-Annotated style),
        the annotation is just `str`, so it appears as "unannotated" for
        the _has_custom_field_annotation check. The Header() becomes the
        default value, which fast_depends handles separately.
        """
        header_field = Header()
        body_field = Body()

        async def handler(
            header_val: str = header_field,
            body_val: str = body_field,
        ):
            pass

        h = Handler(handler)

        # These show up as unannotated because the annotation is just `str`
        # The Header/Body is in the default, not the annotation
        assert "header_val" in h.unannotated_param_names
        assert "body_val" in h.unannotated_param_names

    def test_handler_with_context_annotation(self):
        """Test handler using Context annotation."""
        ctx = Context("some_key")

        async def handler(value: Annotated[str, ctx]):
            pass

        h = Handler(handler)

        assert "value" not in h.unannotated_param_names

    def test_handler_mixed_params(self):
        """Test handler with a mix of annotated and unannotated params."""
        header_field = Header()

        async def handler(
            annotated: Annotated[str, header_field],
            plain: str,
            also_plain: int,
        ):
            pass

        h = Handler(handler)

        assert "annotated" not in h.unannotated_param_names
        assert "plain" in h.unannotated_param_names
        assert "also_plain" in h.unannotated_param_names


class TestContextAnnotation:
    """Tests for Context annotation."""

    @pytest.fixture(autouse=True)
    def clear_context(self):
        """Clear context before and after each test."""
        context_repo.clear()
        yield
        context_repo.clear()

    def test_context_resolves_simple_key(self):
        """Test Context resolves a simple key."""
        context_repo.set_global("test_key", "test_value")

        ctx = Context("test_key")
        ctx.param_name = "param"

        result = ctx.use()

        assert result["param"] == "test_value"

    def test_context_resolves_nested_key(self):
        """Test Context resolves nested keys."""
        context_repo.set_global("message", {"data": {"name": "John"}})

        ctx = Context("message.data.name")
        ctx.param_name = "name"

        result = ctx.use()

        assert result["name"] == "John"

    def test_context_uses_default_when_key_not_found(self):
        """Test Context returns default when key not found."""
        ctx = Context("missing_key", default="default_value")
        ctx.param_name = "param"

        result = ctx.use()

        assert result["param"] == "default_value"

    def test_context_uses_param_name_when_real_name_empty(self):
        """Test Context uses param_name when real_name is empty."""
        context_repo.set_global("my_param", "value_from_param_name")

        ctx = Context("")  # Empty real_name
        ctx.param_name = "my_param"

        result = ctx.use()

        assert result["my_param"] == "value_from_param_name"

    def test_context_with_prefix(self):
        """Test Context with prefix resolves correctly."""
        context_repo.set_global("message", MagicMockWithAttrs(attributes={"user_id": "123"}))

        ctx = Context("user_id", prefix="message.attributes.")
        ctx.param_name = "user_id"

        result = ctx.use()

        assert result["user_id"] == "123"


class TestHeaderAnnotation:
    """Tests for Header annotation."""

    @pytest.fixture(autouse=True)
    def clear_context(self):
        """Clear context before and after each test."""
        context_repo.clear()
        yield
        context_repo.clear()

    def test_header_extracts_from_attributes(self):
        """Test Header extracts value from message.attributes."""
        message = MagicMockWithAttrs(attributes={"x-user-id": "user-123"})
        context_repo.set_global("message", message)

        header = Header("x-user-id")
        header.param_name = "user_id"

        result = header.use()

        assert result["user_id"] == "user-123"

    def test_header_uses_param_name_as_key(self):
        """Test Header uses param_name when real_name is empty."""
        message = MagicMockWithAttrs(attributes={"user_id": "user-456"})
        context_repo.set_global("message", message)

        header = Header()  # No real_name
        header.param_name = "user_id"

        result = header.use()

        assert result["user_id"] == "user-456"

    def test_header_with_missing_attribute(self):
        """Test Header when attribute is not found in message.

        When a key is missing, Context.use() falls back to the entire source
        dict (message.attributes) for Pydantic model support. This allows
        injecting the entire attributes dict into a Pydantic model.
        """
        message = MagicMockWithAttrs(attributes={})
        context_repo.set_global("message", message)

        header = Header("missing", default="default-value")
        header.param_name = "header_val"

        result = header.use()

        # When the key is not found, it falls back to the entire source dict
        # for Pydantic model support. With empty attributes, result contains {}
        assert result["header_val"] == {}


class TestBodyAnnotation:
    """Tests for Body annotation."""

    @pytest.fixture(autouse=True)
    def clear_context(self):
        """Clear context before and after each test."""
        context_repo.clear()
        yield
        context_repo.clear()

    def test_body_extracts_from_decoded_data(self):
        """Test Body extracts value from message.decoded_data."""
        message = MagicMockWithAttrs(decoded_data={"name": "John", "age": 30})
        context_repo.set_global("message", message)

        body = Body("name")
        body.param_name = "name"

        result = body.use()

        assert result["name"] == "John"

    def test_body_uses_param_name_as_key(self):
        """Test Body uses param_name when real_name is empty."""
        message = MagicMockWithAttrs(decoded_data={"email": "test@example.com"})
        context_repo.set_global("message", message)

        body = Body()  # No real_name
        body.param_name = "email"

        result = body.use()

        assert result["email"] == "test@example.com"

    def test_body_with_missing_key(self):
        """Test Body when key is not found in decoded_data.

        When a key is missing, Context.use() falls back to the entire source
        dict (message.decoded_data) for Pydantic model support. This allows
        injecting the entire body into a Pydantic model.
        """
        message = MagicMockWithAttrs(decoded_data={})
        context_repo.set_global("message", message)

        body = Body("missing", default="default-body")
        body.param_name = "body_val"

        result = body.use()

        # When the key is not found, it falls back to the entire source dict
        # for Pydantic model support. With empty decoded_data, result contains {}
        assert result["body_val"] == {}

    def test_body_extracts_entire_decoded_data_for_pydantic_model(self):
        """Test Body can extract entire decoded_data for Pydantic models."""

        class UserModel(BaseModel):
            name: str
            age: int

        message = MagicMockWithAttrs(decoded_data={"name": "Jane", "age": 25})
        context_repo.set_global("message", message)

        # When no key matches, Body falls back to the source dict
        body = Body()
        body.param_name = "user"

        result = body.use()

        # Falls back to entire decoded_data dict
        assert result["user"] == {"name": "Jane", "age": 25}


class TestContextRepository:
    """Tests for ContextRepository."""

    @pytest.fixture
    def repo(self):
        """Create a fresh repository."""
        return ContextRepository()

    def test_set_and_get_global(self, repo: ContextRepository):
        """Test setting and getting global values."""
        repo.set_global("key", "value")

        assert repo.get("key") == "value"

    def test_reset_global(self, repo: ContextRepository):
        """Test resetting global values."""
        repo.set_global("key", "value")
        repo.reset_global("key")

        assert repo.get("key") is None

    def test_set_and_get_local(self, repo: ContextRepository):
        """Test setting and getting scoped values."""
        token = repo.set_local("key", "local_value")

        assert repo.get_local("key") == "local_value"

        repo.reset_local("key", token)
        assert repo.get_local("key") is None

    def test_scope_context_manager(self, repo: ContextRepository):
        """Test scoped context manager."""
        repo.set_global("key", "global")

        with repo.scope("key", "scoped"):
            assert repo.get_local("key") == "scoped"

        assert repo.get_local("key") is None

    def test_resolve_simple_path(self, repo: ContextRepository):
        """Test resolving simple path."""
        repo.set_global("simple", "value")

        assert repo.resolve("simple") == "value"

    def test_resolve_nested_path_dict(self, repo: ContextRepository):
        """Test resolving nested path through dict."""
        repo.set_global("nested", {"level1": {"level2": "deep_value"}})

        assert repo.resolve("nested.level1.level2") == "deep_value"

    def test_resolve_nested_path_object(self, repo: ContextRepository):
        """Test resolving nested path through object attributes."""
        obj = MagicMockWithAttrs(attr1=MagicMockWithAttrs(attr2="attr_value"))
        repo.set_global("obj", obj)

        assert repo.resolve("obj.attr1.attr2") == "attr_value"

    def test_resolve_raises_on_missing_key(self, repo: ContextRepository):
        """Test resolve raises ContextError on missing key."""
        from fastpubsub.di.repository import ContextError

        with pytest.raises(ContextError):
            repo.resolve("missing.path")

    def test_clear_removes_all_context(self, repo: ContextRepository):
        """Test clear removes all global and scoped context."""
        repo.set_global("global_key", "value")
        repo.set_local("local_key", "value")

        repo.clear()

        assert repo.get("global_key") is None
        assert repo.get_local("local_key") is None

    def test_context_property_merges_global_and_local(self, repo: ContextRepository):
        """Test context property returns merged view."""
        repo.set_global("global", "g_value")
        repo.set_local("local", "l_value")

        ctx = repo.context

        assert ctx["global"] == "g_value"
        assert ctx["local"] == "l_value"


class MagicMockWithAttrs:
    """Helper class to create objects with dynamic attributes."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)
