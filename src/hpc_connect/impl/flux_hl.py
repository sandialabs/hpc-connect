from ..hookspec import hookimpl

try:
    import flux  # noqa: F401

except ImportError:

    class _FluxScheduler:
        name = "flux"

        def __init__(self):
            raise RuntimeError("FluxScheduler requires the flux module be importable")

        @staticmethod
        def matches(name: str) -> bool:
            return name == "flux"

    @hookimpl
    def hpc_connect_scheduler():
        return _FluxScheduler

else:
    from .flux_api import FluxScheduler

    @hookimpl
    def hpc_connect_scheduler():
        return FluxScheduler
