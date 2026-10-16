"""Sphinx configuration for the Wenbo documentation."""

project = "Wenbo"
copyright = "2026, Wenbo Contributors"
author = "Wenbo Contributors"
version = "0.1"
release = "0.1.0"

extensions = []
templates_path = ["_templates"]
exclude_patterns = []

language = "en"
html_theme = "furo"
html_title = "Wenbo Documentation"
html_show_sourcelink = False
html_theme_options = {
    "sidebar_hide_name": True,
    "navigation_with_keys": True,
    "light_css_variables": {
        "color-brand-primary": "#3498db",
        "color-brand-content": "#2c3e50",
        "color-background-primary": "#ffffff",
        "color-background-secondary": "#f2f2f2",
        "color-sidebar-background": "#ffffff",
        "color-sidebar-brand-text": "#3498db",
        "color-sidebar-caption-text": "#5a5c63",
        "color-sidebar-link-text": "#2c3e50",
        "color-sidebar-link-text--top-level": "#3498db",
    },
    "dark_css_variables": {
        "color-brand-primary": "#60a5fa",
        "color-brand-content": "#93c5fd",
        "color-foreground-primary": "#e5e7eb",
        "color-foreground-secondary": "#cbd5e1",
        "color-foreground-muted": "#94a3b8",
        "color-background-primary": "#111315",
        "color-background-secondary": "#1a1c1e",
        "color-background-hover": "#24282d",
        "color-background-border": "#30363d",
        "color-sidebar-background": "#1a1c1e",
        "color-sidebar-brand-text": "#e5e7eb",
        "color-sidebar-caption-text": "#94a3b8",
        "color-sidebar-link-text": "#cbd5e1",
        "color-sidebar-link-text--top-level": "#60a5fa",
        "color-sidebar-search-background": "#1a1c1e",
        "color-sidebar-search-background--focus": "#111315",
        "color-sidebar-search-text": "#e5e7eb",
    },
}
html_static_path = ["_static"]
html_css_files = ["custom.css"]
