# Vendored KaTeX

This directory contains the pruned runtime distribution used by
`tools.docs.build_technical_note_html` to build the self-contained Read the
Docs dashboard asset.

- Package: `katex`
- Version: `0.16.11`
- Source command: `npm pack katex@0.16.11`
- npm tarball: `katex-0.16.11.tgz`
- npm shasum: `4bc84d5584f996abece5f01c6ad11304276a33f5`
- npm integrity: `sha512-RQrI8rlHY92OL[...]a/CwbS6HesGNQ==`
- License: MIT, copied in `LICENSE`

Pruned files kept here:

- `katex.min.css`
- `katex.min.js`
- `auto-render.min.js`
- `fonts/` files referenced by `katex.min.css`
- `package.json` for version/license auditability

The generator rewrites every CSS `url(fonts/...)` reference to a base64 `data:`
URI and then asserts that the emitted HTML contains no `http(s)://` URLs and no
relative `url(...)` references.

