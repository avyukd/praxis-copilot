import copy
import json
import os
import sys
import types

# Ensure repo root is importable when running with system pytest.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


# Some local environments have a broken pydantic-core binary for system Python.
# For unit tests that don't depend on full pydantic behavior, provide a tiny shim.
try:
    import pydantic  # noqa: F401
except Exception:
    class _FieldInfo:
        def __init__(self, default=None, default_factory=None, alias=None):
            self.default = default
            self.default_factory = default_factory
            self.alias = alias

    def _fake_field(*args, **kwargs):
        default = kwargs.get("default")
        default_factory = kwargs.get("default_factory")
        alias = kwargs.get("alias")
        return _FieldInfo(default=default, default_factory=default_factory, alias=alias)

    class _FakeBaseModel:
        def __init__(self, **kwargs):
            annotations = getattr(self.__class__, "__annotations__", {})
            for field in annotations:
                if field in kwargs:
                    value = kwargs[field]
                else:
                    attr = getattr(self.__class__, field, None)
                    if isinstance(attr, _FieldInfo):
                        if attr.default_factory is not None:
                            value = attr.default_factory()
                        else:
                            value = copy.deepcopy(attr.default)
                    else:
                        value = copy.deepcopy(attr)
                setattr(self, field, value)

            for key, value in kwargs.items():
                if key not in annotations:
                    setattr(self, key, value)

        def model_dump(self, **kwargs):
            return {
                k: v for k, v in self.__dict__.items()
                if not callable(v) and not k.startswith("__")
            }

        def model_dump_json(self, **kwargs):
            return json.dumps(self.model_dump())

        @classmethod
        def model_validate(cls, data):
            if isinstance(data, cls):
                return data
            if isinstance(data, dict):
                return cls(**data)
            raise TypeError("Unsupported input for model_validate")

        @classmethod
        def model_validate_json(cls, text):
            return cls.model_validate(json.loads(text))

    sys.modules["pydantic"] = types.SimpleNamespace(BaseModel=_FakeBaseModel, Field=_fake_field)


if "litellm" not in sys.modules:
    fake_exceptions = types.SimpleNamespace(
        APIError=Exception,
        APIConnectionError=Exception,
        RateLimitError=Exception,
        ServiceUnavailableError=Exception,
        APITimeoutError=Exception,
    )
    sys.modules["litellm"] = types.SimpleNamespace(completion=lambda **kwargs: None)
    sys.modules["litellm.exceptions"] = fake_exceptions
