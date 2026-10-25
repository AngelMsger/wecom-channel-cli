# Visual design: Blue Current

This candidate replaces the earlier Acid Circuit experiment. The user's design
constraint is explicit: use WeCom's branding colors, but create an independent,
modern developer experience. Do not copy WeCom's official page design or reuse
FX Devkit's palette, layout, typography or visual motifs.

## Direction

- A bright white/ice-blue canvas, precise dark navy text, blue/cyan accents and
  generous spacing. Use dark navy selectively for code surfaces.
- A conversational, sentence-case headline and readable system typography.
  Monospace is reserved for protocol details, commands and small technical labels.
- An original chat/code symbol, not the official WeCom logo. The independent
  connector attribution remains visible.
- Product-specific illustrations: a channel message, structured event, pending
  review and separate context lanes. Avoid ornamental orbital diagrams and
  terminal-poster styling as the page's identity.
- A functioning context explorer with explicit simulated data and server-boundary
  examples. No new runtime capability is implied by an illustrative interface.
- The documentation uses the same light navigation, blue active states and navy
  code blocks as the landing page.

## Color provenance

The public [WeCom website](https://work.weixin.qq.com/) was inspected on
2026-10-25 for color reference only. Its visible styles include cyan `#5fc9ec`
and interface blue `#1b88ee`. These are inspiration points, not a claim that this
project follows a formal brand specification or is endorsed by Tencent.

The site uses `#1b88ee` as the blue anchor and `#1269d3` for stronger text/button
contrast. The supporting palette is white, ice blue, dark navy and muted slate.
Amber/peach is limited to pending/denied examples and is not a brand accent.

## Implementation

The landing page is `docs/index.html`; styles, the original SVG mark and
interactions live under `docs/assets/`. Shared documentation chrome is generated
from `cmd/gen-docs/main.go`. Keep Markdown canonical and run `make docs` after
changing the generator or narrative content.

Assets are local and work without a CDN, external font request or framework
runtime. Keyboard focus remains visible, action controls expose pressed state,
status changes are announced, and motion follows `prefers-reduced-motion`.
Review desktop, intermediate widths and mobile after layout changes. Keep the
full record available in the explorer and let long code scroll or wrap within
its panel rather than widening the page.

This is a project-specific design candidate. Other projects should derive their
own visual identity from their product and audience rather than inherit this
page wholesale. Publication remains subject to the owner's review.

## Navigation and family identity review

Reviewed against [GitHub CLI](https://cli.github.com/), [Vite](https://vite.dev/)
and [uv](https://docs.astral.sh/uv/) on 2026-10-25. Their observed navigation uses
readable, consistent type scales (GitHub CLI around 14px; Vite 16px; uv navigation
15–17px). These are references for hierarchy, not templates for this layout.

The product wordmark is now the single string "WeCom Channel CLI", in one
sans-serif weight and color. The desktop menu and Get started action both use
14px text. Monospace remains for commands and protocol labels, not fragments of
the product name. The family slogan "Built for agents. Designed for humans."
is the primary hero statement and appears prominently in the README image.
