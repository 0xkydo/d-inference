"""In-memory fixtures only; no real provider state is read or written."""

from dataclasses import dataclass, field


@dataclass
class PreviewState:
    selected: tuple[int, ...] = (0,)
    downloaded: set[int] = field(default_factory=lambda: {0})
    login_attempt: int = 1

    @classmethod
    def for_scene(cls, key):
        state = cls()
        if key in ("install", "download", "offline", "integrity", "enroll", "settings", "resume"):
            state.downloaded.clear()
        if key in ("model-download", "download-paused", "model-resume", "disk-full"):
            state.selected = (1,)
        return state
