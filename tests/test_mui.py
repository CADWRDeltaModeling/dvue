"""Tests for dvue.mui, the classic-Panel to Panel Material UI button shim."""

import pytest

from dvue.mui import Button, material_icon


class TestMaterialIcon:
    @pytest.mark.parametrize(
        "tabler, material",
        [
            ("chart-line", "show_chart"),
            ("info-circle", "info"),
            ("device-floppy", "save"),
            ("x", "close"),
            ("some-unmapped-icon", "some_unmapped_icon"),
        ],
    )
    def test_translates_tabler_names(self, tabler, material):
        assert material_icon(tabler) == material

    def test_is_idempotent_and_passes_through_material_and_svg(self):
        assert material_icon(material_icon("chart-line")) == "show_chart"
        assert material_icon("info") == "info"
        svg = "<svg></svg>"
        assert material_icon(svg) == svg
        assert material_icon("") == ""
        assert material_icon(None) is None


class TestButton:
    def test_translates_classic_arguments(self):
        btn = Button(name="Plot", button_type="warning", icon="chart-line", button_style="outline")
        assert btn.label == "Plot"
        assert btn.name == "Plot"
        assert btn.color == "warning"
        assert btn.variant == "outlined"
        assert btn.icon == "show_chart"

    def test_icon_assigned_later_is_translated(self):
        btn = Button(name="Fold", icon="chevron-up")
        btn.icon = "chevron-down"
        assert btn.icon == "keyboard_arrow_down"

    def test_on_click_receives_event(self):
        seen = []
        btn = Button(name="Go")
        btn.on_click(lambda event: seen.append(event.new))
        btn.clicks += 1
        assert seen == [1]

    def test_js_on_click_is_available(self):
        btn = Button(name="Reset")
        assert btn.js_on_click(args={}, code="console.log(1)") is not None
