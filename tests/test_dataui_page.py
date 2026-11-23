"""Tests for the pmui.Page based DataUI views and the shared modal dialog."""

import pandas as pd
import geopandas as gpd
import holoviews as hv
import panel as pn
import panel_material_ui as pmui
import pytest
from bokeh.models import Range1d
from bokeh.plotting import figure
from shapely.geometry import Point

from dvue import dataui
from dvue.catalog import DataCatalog, DataReference, InMemoryDataReferenceReader
from dvue.tsdataui import TimeSeriesDataUIManager


class _Manager(TimeSeriesDataUIManager):
    def __init__(self, disclaimer=None, with_map=False, **kwargs):
        reader = InMemoryDataReferenceReader(pd.DataFrame({"value": [1.0, 2.0]}))
        cat = DataCatalog(
            primary_key=["name"],
            crs="EPSG:4326" if with_map else None,
        )
        for i in range(2):
            attrs = {}
            if with_map:
                attrs["geometry"] = Point(-121.0 + i * 0.1, 38.0)
            cat.add(
                DataReference(
                    reader=reader,
                    name=f"ref_{i}",
                    B=f"STA{i}",
                    C="EC",
                    **attrs,
                )
            )
        self._test_catalog = cat
        self._disclaimer = disclaimer
        super().__init__(**kwargs)

    @property
    def data_catalog(self):
        return self._test_catalog

    def _get_table_column_width_map(self):
        return {"B": "50%", "C": "50%"}

    def get_table_filters(self):
        return {}

    def get_tooltips(self):
        return []

    def get_map_color_columns(self):
        return ["C"]

    def get_map_marker_columns(self):
        return ["C"]

    def get_name_to_color(self):
        return {}

    def get_name_to_marker(self):
        return {}

    def get_time_range(self, dfcat):
        return (pd.Timestamp("2020-01-01"), pd.Timestamp("2020-12-31"))

    def build_station_name(self, r):
        return str(r.get("B", "unknown"))

    def get_sidebar_disclaimer(self):
        return self._disclaimer


def _button(items, label):
    return next(b for b in items if getattr(b, "label", None) == label)


class TestCreateView:
    def test_returns_page_with_dialog_and_about_button(self):
        ui = dataui.DataUI(_Manager())
        page = ui.create_view(title="My App")
        assert isinstance(page, pmui.Page)
        assert page.title == "My App"
        assert any(isinstance(m, pmui.Dialog) for m in page.main)
        _button(page.header, "About App")
        assert all(getattr(h, "label", None) != "Disclaimer" for h in page.header)
        assert page.sidebar_variant == "auto"
        assert page.sidebar_width == 400
        assert ui._sidebar_tabs.active == 3

    def test_disclaimer_button_only_when_manager_provides_text(self):
        ui = dataui.DataUI(_Manager(disclaimer="Be careful"))
        page = ui.create_view()
        _button(page.header, "Disclaimer")

    def test_header_buttons_open_dialog_with_matching_content(self):
        ui = dataui.DataUI(_Manager(disclaimer="Be careful"))
        page = ui.create_view()
        modal = next(m for m in page.main if isinstance(m, pmui.Dialog))

        _button(page.header, "Disclaimer").clicks += 1
        assert modal.open
        assert modal.title == "Disclaimer"
        assert modal.objects[0].object == "Be careful"

        modal.open = False
        _button(page.header, "About App").clicks += 1
        assert modal.open
        assert "About" in modal.objects[0].object

    def test_dialog_can_be_dismissed(self):
        modal = dataui.DataUI(_Manager()).create_view().main[-1]
        assert isinstance(modal, pmui.Dialog)
        assert modal.show_close_button
        assert modal.close_on_click

    def test_mobile_view_is_page_with_collapsed_sidebar(self):
        ui = dataui.DataUI(_Manager())
        page = ui.create_mobile_view()
        assert isinstance(page, pmui.Page)
        assert page.sidebar_open is False
        assert any(isinstance(m, pmui.Dialog) for m in page.main)

    def test_empty_state_contains_guided_steps(self):
        message = _Manager().get_no_selection_message()
        assert "Start here" in message
        assert "Find a station" in message
        assert "Select one or more rows" in message
        assert "Click Plot" in message

    def test_map_is_default_display_tab_when_geometry_exists(self):
        ui = dataui.DataUI(
            _Manager(with_map=True),
            initial_map_extent=(-122.65, 37.55, -121.15, 38.75),
        )
        ui.create_view()

        tabs = ui._display_panel.objects[0]
        assert isinstance(tabs, pn.Tabs)
        assert tabs._names == ["Interactive Map"]
        assert tabs.active == 0
        assert len(ui._dfmapcat) == 2
        assert len(ui._map_source_df) == 2
        assert isinstance(ui._main_map_pane, pn.pane.Bokeh)
        assert ui._main_map_pane.sizing_mode == "scale_width"
        assert ui._main_map_pane.object.sizing_mode == "scale_width"
        assert ui._main_map_pane.object.aspect_ratio == pytest.approx(3.1)
        assert ui._main_map_plot.apply_ranges is False
        assert all(not subplot.apply_ranges for subplot in ui._main_map_plot.subplots.values())
        x_span = ui._main_map_pane.object.x_range.end - ui._main_map_pane.object.x_range.start
        y_span = ui._main_map_pane.object.y_range.end - ui._main_map_pane.object.y_range.start
        assert x_span / y_span == pytest.approx(3.1)
        assert not any(
            isinstance(obj, pn.pane.HTML)
            and "Start here" in obj.object
            for obj in ui._display_panel.objects
        )
        projected = ui._project_initial_map_extent()
        assert projected[0] < projected[2]
        assert projected[1] < projected[3]
        assert projected[3] - projected[1] > 150_000
        assert (projected[2] - projected[0]) / (projected[3] - projected[1]) == pytest.approx(3.1)
        rendered_figure = figure()
        hook = ui._make_initial_extent_hook(projected)
        fake_plot = type("Plot", (), {"handles": {"plot": rendered_figure}})()
        hook(fake_plot, None)
        assert ui._extent_applied_figures
        assert rendered_figure.x_range.start == pytest.approx(projected[0])
        assert rendered_figure.y_range.end == pytest.approx(projected[3])
        assert isinstance(rendered_figure.x_range, Range1d)
        rendered_figure.x_range.start = 123
        hook(fake_plot, None)
        assert rendered_figure.x_range.start == 123

    def test_main_map_selection_filters_table(self):
        ui = dataui.DataUI(
            _Manager(with_map=True),
            station_id_column="B",
            initial_map_extent=(-122.65, 37.55, -121.15, 38.75),
        )
        ui.create_view()

        ui._main_station_select.event(index=[0])

        assert ui._map_filter_station_ids == {"STA0"}
        assert ui.display_table.value["B"].tolist() == ["STA0"]

        ui._clear_map_filter()
        main_figure = ui._main_map_pane.object
        station_source = next(
            renderer.data_source
            for renderer in main_figure.renderers
            if "B" in getattr(getattr(renderer, "data_source", None), "data", {})
        )
        ui._main_bokeh_selection_callbacks[-1](
            "indices",
            [],
            [1],
            station_source.selected,
        )
        assert ui._map_filter_station_ids == {"STA1"}
        assert ui.display_table.value["B"].tolist() == ["STA1"]


class TestAddHeaderButtons:
    def test_page_gets_header_buttons_and_dialog(self):
        ui = dataui.DataUI(_Manager(disclaimer="Be careful"))
        outer = pmui.Page(title="outer", main=["x"])
        ui.add_header_buttons(outer)
        _button(outer.header, "About App")
        _button(outer.header, "Disclaimer")
        assert sum(isinstance(m, pmui.Dialog) for m in outer.main) == 1
        ui.add_header_buttons(outer)
        assert sum(isinstance(m, pmui.Dialog) for m in outer.main) == 1

    def test_classic_template_keeps_modal_workflow(self):
        ui = dataui.DataUI(_Manager())
        tmpl = pn.template.VanillaTemplate(title="legacy")
        opened = []
        tmpl.open_modal = lambda: opened.append(True)
        ui.add_header_buttons(tmpl)
        assert len(tmpl.modal) == 1
        _button(tmpl.header, "About App").clicks += 1
        assert opened
