"""Unit tests for dvue.views (ViewDefinition, ViewsManager, YAML round-trip)."""

from __future__ import annotations

import pytest
import pandas as pd

from dvue.views import ViewDefinition, ViewsManager, _row_matches_criteria


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Catalog-like DataFrame with name, station, variable, unit columns."""
    return pd.DataFrame(
        {
            "name": ["a_ec", "b_ec", "c_flow", "d_flow", "e_stage"],
            "station": ["RSAC075", "RSAC128", "RSAN007", "ROLD024", "RSAN007"],
            "variable": ["EC", "EC", "flow", "flow", "stage"],
            "unit": ["uS/cm", "uS/cm", "cfs", "cfs", "ft"],
        }
    )


# ---------------------------------------------------------------------------
# _row_matches_criteria
# ---------------------------------------------------------------------------


class TestRowMatchesCriteria:
    def test_exact_match(self, sample_df):
        row = sample_df.iloc[0]
        assert _row_matches_criteria(row, {"variable": "EC"})
        assert not _row_matches_criteria(row, {"variable": "flow"})

    def test_regex_match(self, sample_df):
        row_rsac075 = sample_df.iloc[0]
        row_rsan007 = sample_df.iloc[2]
        assert _row_matches_criteria(row_rsac075, {"station": "~RSAC.*"})
        assert not _row_matches_criteria(row_rsan007, {"station": "~RSAC.*"})

    def test_regex_case_insensitive(self, sample_df):
        row = sample_df.iloc[0]
        assert _row_matches_criteria(row, {"variable": "~ec"})

    def test_list_or_match(self, sample_df):
        row_a = sample_df.iloc[0]  # RSAC075
        row_d = sample_df.iloc[3]  # ROLD024
        row_e = sample_df.iloc[4]  # RSAN007 / stage
        assert _row_matches_criteria(row_a, {"station": ["RSAC075", "ROLD024"]})
        assert _row_matches_criteria(row_d, {"station": ["RSAC075", "ROLD024"]})
        assert not _row_matches_criteria(row_e, {"station": ["RSAC075", "ROLD024"]})

    def test_multi_criteria_and(self, sample_df):
        row = sample_df.iloc[0]  # RSAC075 / EC
        assert _row_matches_criteria(row, {"station": "~RSAC.*", "variable": "EC"})
        assert not _row_matches_criteria(row, {"station": "~RSAC.*", "variable": "flow"})

    def test_missing_column(self, sample_df):
        row = sample_df.iloc[0]
        assert not _row_matches_criteria(row, {"nonexistent_col": "value"})


# ---------------------------------------------------------------------------
# ViewDefinition.matches_row
# ---------------------------------------------------------------------------


class TestViewDefinitionMatchesRow:
    def test_criteria_based(self, sample_df):
        vdef = ViewDefinition(name="EC stations", criteria={"variable": "EC"})
        result = sample_df[sample_df.apply(vdef.matches_row, axis=1)]
        assert list(result["name"]) == ["a_ec", "b_ec"]

    def test_names_based_takes_priority(self, sample_df):
        # Even with criteria, names list takes priority
        vdef = ViewDefinition(
            name="manual",
            criteria={"variable": "EC"},  # would match a_ec and b_ec
            names=["c_flow", "d_flow"],    # but names overrides
        )
        result = sample_df[sample_df.apply(vdef.matches_row, axis=1)]
        assert list(result["name"]) == ["c_flow", "d_flow"]

    def test_empty_definition_matches_all(self, sample_df):
        vdef = ViewDefinition(name="all")
        result = sample_df[sample_df.apply(vdef.matches_row, axis=1)]
        assert len(result) == len(sample_df)


# ---------------------------------------------------------------------------
# ViewsManager
# ---------------------------------------------------------------------------


class TestViewsManager:
    def test_initial_state(self):
        mgr = ViewsManager()
        assert mgr.active_view == "All"
        assert mgr.view_names == ["All"]
        assert mgr._views == []

    def test_add_view(self):
        mgr = ViewsManager()
        vdef = ViewDefinition(name="EC", criteria={"variable": "EC"})
        mgr.add_view(vdef)
        assert mgr.view_names == ["All", "EC"]

    def test_add_duplicate_raises(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC"))
        with pytest.raises(ValueError, match="already exists"):
            mgr.add_view(ViewDefinition(name="EC"))

    def test_add_reserved_all_raises(self):
        mgr = ViewsManager()
        with pytest.raises(ValueError, match="reserved"):
            mgr.add_view(ViewDefinition(name="All"))

    def test_remove_view(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC"))
        mgr.remove_view("EC")
        assert mgr.view_names == ["All"]

    def test_remove_active_view_resets_to_all(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC"))
        mgr.active_view = "EC"
        mgr.remove_view("EC")
        assert mgr.active_view == "All"

    def test_remove_all_raises(self):
        mgr = ViewsManager()
        with pytest.raises(ValueError):
            mgr.remove_view("All")

    def test_remove_nonexistent_raises(self):
        mgr = ViewsManager()
        with pytest.raises(KeyError):
            mgr.remove_view("DoesNotExist")

    def test_rename_view(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="Old"))
        mgr.rename_view("Old", "New")
        assert "New" in mgr.view_names
        assert "Old" not in mgr.view_names

    def test_rename_updates_active_view(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="Old"))
        mgr.active_view = "Old"
        mgr.rename_view("Old", "New")
        assert mgr.active_view == "New"

    def test_rename_to_all_raises(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC"))
        with pytest.raises(ValueError, match="reserved"):
            mgr.rename_view("EC", "All")

    def test_rename_to_existing_raises(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC"))
        mgr.add_view(ViewDefinition(name="Flow"))
        with pytest.raises(ValueError, match="already exists"):
            mgr.rename_view("EC", "Flow")

    def test_get_view_def_all_returns_none(self):
        mgr = ViewsManager()
        assert mgr.get_view_def("All") is None

    def test_get_view_def_returns_definition(self):
        mgr = ViewsManager()
        vdef = ViewDefinition(name="EC", criteria={"variable": "EC"})
        mgr.add_view(vdef)
        result = mgr.get_view_def("EC")
        assert result is vdef

    def test_filter_dataframe_all_returns_full(self, sample_df):
        mgr = ViewsManager()
        result = mgr.filter_dataframe(sample_df)
        pd.testing.assert_frame_equal(result, sample_df)

    def test_filter_dataframe_criteria_view(self, sample_df):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC", criteria={"variable": "EC"}))
        mgr.active_view = "EC"
        result = mgr.filter_dataframe(sample_df)
        assert list(result["name"]) == ["a_ec", "b_ec"]

    def test_filter_dataframe_names_view(self, sample_df):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="manual", names=["c_flow", "e_stage"]))
        mgr.active_view = "manual"
        result = mgr.filter_dataframe(sample_df)
        assert set(result["name"]) == {"c_flow", "e_stage"}

    def test_filter_dataframe_regex_view(self, sample_df):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="Sac", criteria={"station": "~RSAC.*"}))
        mgr.active_view = "Sac"
        result = mgr.filter_dataframe(sample_df)
        assert all(r.startswith("RSAC") for r in result["station"])

    def test_version_increments_on_add(self):
        mgr = ViewsManager()
        v0 = mgr._version
        mgr.add_view(ViewDefinition(name="EC"))
        assert mgr._version == v0 + 1

    def test_version_increments_on_remove(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC"))
        v0 = mgr._version
        mgr.remove_view("EC")
        assert mgr._version == v0 + 1


# ---------------------------------------------------------------------------
# YAML round-trip
# ---------------------------------------------------------------------------


class TestYamlRoundTrip:
    def test_criteria_view_round_trip(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC Sac", criteria={"variable": "EC", "station": "~RSAC.*"}))
        yaml_str = mgr.to_yaml_str()
        mgr2 = ViewsManager()
        mgr2.load_from_yaml_str(yaml_str)
        assert mgr2.view_names == ["All", "EC Sac"]
        vdef = mgr2.get_view_def("EC Sac")
        assert vdef.criteria["variable"] == "EC"
        assert vdef.criteria["station"] == "~RSAC.*"

    def test_names_view_round_trip(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="manual", names=["ref_a", "ref_b"]))
        yaml_str = mgr.to_yaml_str()
        mgr2 = ViewsManager()
        mgr2.load_from_yaml_str(yaml_str)
        vdef = mgr2.get_view_def("manual")
        assert vdef.names == ["ref_a", "ref_b"]

    def test_multiple_views_round_trip(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="V1", criteria={"variable": "EC"}))
        mgr.add_view(ViewDefinition(name="V2", names=["x", "y"]))
        yaml_str = mgr.to_yaml_str()
        mgr2 = ViewsManager()
        mgr2.load_from_yaml_str(yaml_str)
        assert mgr2.view_names == ["All", "V1", "V2"]

    def test_load_invalid_yaml_raises(self):
        mgr = ViewsManager()
        with pytest.raises(ValueError, match="Invalid YAML"):
            mgr.load_from_yaml_str(": : : not valid yaml :::")

    def test_load_missing_views_key_raises(self):
        mgr = ViewsManager()
        with pytest.raises(ValueError, match="'views' key"):
            mgr.load_from_yaml_str("other_key: value\n")

    def test_load_entry_without_name_raises(self):
        mgr = ViewsManager()
        with pytest.raises(ValueError, match="'name' key"):
            mgr.load_from_yaml_str("views:\n  - criteria:\n      variable: EC\n")

    def test_load_resets_to_all_if_active_view_missing(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="OldView"))
        mgr.active_view = "OldView"
        # Load a YAML that doesn't contain OldView
        mgr.load_from_yaml_str("views:\n  - name: NewView\n    criteria:\n      variable: EC\n")
        assert mgr.active_view == "All"

    def test_empty_views_yaml(self):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC"))
        yaml_str = "views: []\n"
        mgr.load_from_yaml_str(yaml_str)
        assert mgr.view_names == ["All"]


# ---------------------------------------------------------------------------
# ViewsManager.add_to_view
# ---------------------------------------------------------------------------


class TestAddToView:
    def test_add_to_view_basic(self, sample_df):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="Manual", names=["a_ec"]))
        n_added = mgr.add_to_view("Manual", ["b_ec", "c_flow"])
        vdef = mgr.get_view_def("Manual")
        assert vdef.names == ["a_ec", "b_ec", "c_flow"]
        assert n_added == 2

    def test_add_to_view_deduplicates(self, sample_df):
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="Manual", names=["a_ec", "b_ec"]))
        v0 = mgr._version
        n_added = mgr.add_to_view("Manual", ["b_ec", "c_flow"])
        vdef = mgr.get_view_def("Manual")
        assert vdef.names == ["a_ec", "b_ec", "c_flow"]
        assert n_added == 1  # only c_flow was new
        assert mgr._version == v0 + 1  # version still incremented

    def test_add_to_view_criteria_view(self, sample_df):
        """Appending names to a criteria-only view keeps criteria untouched;
        matches_row then uses names (names take priority)."""
        mgr = ViewsManager()
        mgr.add_view(ViewDefinition(name="EC", criteria={"variable": "EC"}))
        mgr.add_to_view("EC", ["c_flow"])
        vdef = mgr.get_view_def("EC")
        # Criteria preserved
        assert vdef.criteria == {"variable": "EC"}
        # Names appended
        assert vdef.names == ["c_flow"]
        # matches_row now uses names — only c_flow matches
        result = sample_df[sample_df.apply(vdef.matches_row, axis=1)]
        assert list(result["name"]) == ["c_flow"]

    def test_add_to_view_unknown_view_raises_key_error(self):
        mgr = ViewsManager()
        with pytest.raises(KeyError):
            mgr.add_to_view("NonExistent", ["ref_a"])
