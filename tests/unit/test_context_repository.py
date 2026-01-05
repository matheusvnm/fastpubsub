"""Unit tests for context repository and resolution."""

import pytest

from fastpubsub.di.constants import EMPTY
from fastpubsub.di.repository import (
    ContextError,
    ContextRepository,
    context_repo,
    resolve_context_by_name,
)


class TestContextRepo:
    """Tests for the ContextRepo class."""

    @pytest.fixture
    def repo(self) -> ContextRepository:
        return ContextRepository()

    def test_set_and_get_global(self, repo: ContextRepository):
        repo.set_global("test_key", "test_value")
        assert repo.get("test_key") == "test_value"

    def test_reset_global(self, repo: ContextRepository):
        repo.set_global("test_key", "test_value")
        repo.reset_global("test_key")
        assert repo.get("test_key") is None

    def test_reset_nonexistent_global(self, repo: ContextRepository):
        # Should not raise
        repo.reset_global("nonexistent_key")

    def test_set_and_get_local(self, repo: ContextRepository):
        token = repo.set_local("local_key", "local_value")
        assert repo.get_local("local_key") == "local_value"
        repo.reset_local("local_key", token)

    def test_get_local_default(self, repo: ContextRepository):
        assert repo.get_local("nonexistent", "default") == "default"

    def test_scope_context_manager(self, repo: ContextRepository):
        with repo.scope("scoped_key", "scoped_value"):
            assert repo.get_local("scoped_key") == "scoped_value"
        # After scope exits, value should be reset
        assert repo.get_local("scoped_key") is None

    def test_nested_scopes(self, repo: ContextRepository):
        with repo.scope("key", "outer"):
            assert repo.get_local("key") == "outer"
            with repo.scope("key", "inner"):
                assert repo.get_local("key") == "inner"
            assert repo.get_local("key") == "outer"
        assert repo.get_local("key") is None

    def test_get_prefers_global_over_local(self, repo: ContextRepository):
        repo.set_global("key", "global")
        repo.set_local("key", "local")
        # Global takes precedence
        assert repo.get("key") == "global"

    def test_get_returns_local_if_no_global(self, repo: ContextRepository):
        repo.set_local("key", "local")
        assert repo.get("key") == "local"

    def test_get_with_default(self, repo: ContextRepository):
        assert repo.get("nonexistent", "default") == "default"

    def test_context_property_merges_global_and_local(self, repo: ContextRepository):
        repo.set_global("global_key", "global_value")
        repo.set_local("local_key", "local_value")
        context = repo.context
        assert context["global_key"] == "global_value"
        assert context["local_key"] == "local_value"

    def test_clear(self, repo: ContextRepository):
        repo.set_global("key1", "value1")
        repo.set_local("key2", "value2")
        repo.clear()
        assert repo.get("key1") is None
        assert repo.get_local("key2") is None

    def test_attribute_access(self, repo: ContextRepository):
        repo.set_global("test_attr", "attr_value")
        assert repo.test_attr == "attr_value"

    def test_initial_context(self):
        repo = ContextRepository({"initial_key": "initial_value"})
        assert repo.get("initial_key") == "initial_value"

    def test_context_contains_self_reference(self, repo: ContextRepository):
        assert repo.get("context") is repo


class TestContextRepoResolve:
    """Tests for the ContextRepo.resolve method."""

    @pytest.fixture
    def repo(self) -> ContextRepository:
        return ContextRepository()

    def test_resolve_simple_key(self, repo: ContextRepository):
        repo.set_global("key", "value")
        assert repo.resolve("key") == "value"

    def test_resolve_dotted_key(self, repo: ContextRepository):
        repo.set_global("user.name", "John")
        assert repo.resolve("user.name") == "John"

    def test_resolve_walks_object_attributes(self, repo: ContextRepository):
        class User:
            def __init__(self):
                self.name = "John"

        repo.set_global("user", User())
        assert repo.resolve("user.name") == "John"

    def test_resolve_walks_dict_keys(self, repo: ContextRepository):
        repo.set_global("data", {"user": {"name": "John"}})
        assert repo.resolve("data.user.name") == "John"

    def test_resolve_mixed_dict_and_object(self, repo: ContextRepository):
        class Config:
            settings = {"debug": True}

        repo.set_global("config", Config())
        assert repo.resolve("config.settings.debug") is True

    def test_resolve_raises_on_missing_key(self, repo: ContextRepository):
        with pytest.raises(ContextError) as exc_info:
            repo.resolve("nonexistent")
        assert "nonexistent" in str(exc_info.value)

    def test_resolve_prefers_longest_prefix(self, repo: ContextRepository):
        repo.set_global("a.b", {"c": "from_a.b"})
        repo.set_global("a", {"b": {"c": "from_a"}})
        # Should find "a.b" first (longest prefix)
        assert repo.resolve("a.b.c") == "from_a.b"


class TestContextError:
    """Tests for the ContextError exception."""

    def test_context_error_message(self):
        error = ContextError({"key": "value"}, "missing_field")
        message = str(error)
        assert "missing_field" in message
        assert "not found" in message

    def test_context_error_attributes(self):
        context = {"key": "value"}
        error = ContextError(context, "field_name")
        assert error.context == context
        assert error.field == "field_name"


class TestResolveContextByName:
    """Tests for the resolve_context_by_name helper function."""

    @pytest.fixture(autouse=True)
    def clear_context(self):
        context_repo.clear()
        yield
        context_repo.clear()

    def test_resolve_existing_key(self):
        context_repo.set_global("key", "value")
        result = resolve_context_by_name("key")
        assert result == "value"

    def test_resolve_with_default(self):
        result = resolve_context_by_name("nonexistent", default="default_value")
        assert result == "default_value"

    def test_resolve_with_initial_factory(self):
        def factory():
            return "factory_value"

        result = resolve_context_by_name("new_key", initial=factory)
        assert result == "factory_value"
        # Should also be stored in global context
        assert context_repo.get("new_key") == "factory_value"

    def test_resolve_returns_empty_on_missing_without_default(self):
        result = resolve_context_by_name("nonexistent")
        assert result is EMPTY
