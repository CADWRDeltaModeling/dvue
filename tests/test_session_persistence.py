from dvue.session_persistence import SessionManager


class BrokenCache:
    def get(self, *args, **kwargs):
        raise OSError("database unavailable")

    def set(self, *args, **kwargs):
        raise OSError("database unavailable")


def test_session_manager_tolerates_unavailable_disk_cache():
    manager = SessionManager(persist=False)
    manager._session_cache = BrokenCache()

    assert manager.load_state("user") == {}
    manager.save_state("user", {"selection": [1]})
