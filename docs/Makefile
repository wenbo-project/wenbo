SPHINXOPTS    ?= -W --keep-going
SPHINXBUILD   ?= python -m sphinx
SOURCEDIR     = .
BUILDDIR      = ../.local/docs-build

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
