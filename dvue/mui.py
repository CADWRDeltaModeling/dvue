"""Panel Material UI compatibility helpers.

dvue's public action dictionaries (``get_data_actions``) and many downstream
apps describe buttons with Panel's classic vocabulary: ``name``,
``button_type`` and tabler icon names such as ``"chart-line"``.  Panel Material
UI buttons use ``label``, ``color``/``variant`` and Material icon names.  The
:class:`Button` defined here accepts the classic arguments and translates them,
so callers do not need to change.
"""

import panel_material_ui as pmui
import param
# Tabler icon name -> Material icon name.  Names that are not listed fall back
# to replacing dashes with underscores, which is how most Material icons differ.
TABLER_TO_MATERIAL = {
    "alert-circle": "error_outline",
    "arrow-left": "arrow_back",
    "arrow-right": "arrow_forward",
    "arrows-collapse": "unfold_less",
    "arrows-diff": "compare_arrows",
    "arrows-maximize": "open_in_full",
    "calendar": "calendar_today",
    "chart-bar": "bar_chart",
    "chart-line": "show_chart",
    "chevron-down": "keyboard_arrow_down",
    "chevron-up": "keyboard_arrow_up",
    "clipboard-copy": "content_copy",
    "device-floppy": "save",
    "file-download": "file_download",
    "file-export": "file_download",
    "file-import": "upload_file",
    "filter": "filter_alt",
    "folder-plus": "create_new_folder",
    "function": "functions",
    "info-circle": "info",
    "list-check": "checklist",
    "list-details": "list_alt",
    "map-pin": "place",
    "math-function": "functions",
    "player-play": "play_arrow",
    "report": "assessment",
    "table": "table_chart",
    "trash": "delete",
    "x": "close",
}

# Classic ``button_style`` values -> pmui ``variant``.
_VARIANTS = {"solid": "contained", "outline": "outlined"}


def material_icon(name):
    """Return the Material icon name for a tabler (or Material) icon name.

    SVG markup, empty values and names that are already Material names are
    returned unchanged, so the function is idempotent.
    """
    if not name or name.lstrip().startswith("<"):
        return name
    key = name.strip().lower()
    return TABLER_TO_MATERIAL.get(key, key.replace("-", "_").replace(" ", "_"))


class Button(pmui.Button):
    """A :class:`panel_material_ui.Button` accepting classic Panel arguments.

    ``name`` is used as the label, ``button_type`` as the colour,
    ``button_style`` (``"solid"``/``"outline"``) as the variant, and ``icon``
    may be a tabler icon name, including when assigned after construction.
    """

    def __init__(self, **params):
        name = params.pop("name", None)
        if name is not None and "label" not in params:
            params["label"] = name
        if "button_type" in params:
            button_type = params.pop("button_type")
            params.setdefault("color", button_type)
        if "button_style" in params:
            style = params.pop("button_style")
            params.setdefault("variant", _VARIANTS.get(style, style))
        if params.get("icon"):
            params["icon"] = material_icon(params["icon"])
        super().__init__(**params)
        if name is not None:
            # Keep ``.name`` as callers set it; code that looks buttons up by
            # name relies on it.
            with param.edit_constant(self):
                self.name = name
        self.param.watch(self._translate_icon, "icon")

    def _translate_icon(self, event):
        icon = material_icon(event.new)
        if icon != event.new:
            self.icon = icon

    def js_on_click(self, args=None, code=""):
        """Run *code* in the browser on click (classic ``Button`` API)."""
        return self.jscallback(args=args or {}, clicks=code)
