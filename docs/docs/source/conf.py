# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#

import sys
from pathlib import Path
current_dir = str(Path(__file__).parent.absolute())
sys.path.insert(0, current_dir)

import hfai


# -- Project information -----------------------------------------------------

project = 'HAI Platform'
copyright = '2023, High-Flyer'
author = 'High-Flyer'

# The full version
hai_version = "1.0.0"
# release = hai_version.split('+')[0]
# if 'external' in release:
#     release = hai_version.split('-')[0]


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx.ext.mathjax',
    'sphinxcontrib.httpdomain',
    'sphinx_click'
]

# Do not warn about external images (status badges in README.rst)
suppress_warnings = ['image.nonlocal_uri']

# build the templated autosummary files
autosummary_generate = True
numpydoc_show_class_members = False

# Theme has bootstrap already
panels_add_bootstrap_css = False

# autosectionlabel throws warnings if section names are duplicated.
# The following tells autosectionlabel to not throw a warning for
# duplicated section names that are in different documents.
autosectionlabel_prefix_document = True

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_use_ivar = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = 'en'

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}
# source_suffix = ['.rst']

# The master toctree document.
master_doc = 'index'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# Disable docstring inheritance
autodoc_inherit_docstrings = False

# Disable displaying type annotations, these can be very verbose
autodoc_typehints = 'none'

# Enable overriding of function signatures in the first line of the docstring.
autodoc_docstring_signature = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'pytorch_sphinx_theme'
html_theme_path = ["../../pytorch_sphinx_theme"]

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_css_files = ['css/custom.css', 'css/element-plus.css']
html_js_files = [
    'js/vue.global.prod.js',
    'js/element-plus.full.js',
]