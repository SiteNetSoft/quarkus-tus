/**
 * Render the Antora pages in docs/modules/ROOT into a static documentation
 * site styled to match sitenetsoft.org. The .adoc pages stay the single
 * source of truth (and the Antora layout stays untouched, so a real Antora
 * site can aggregate them later); this script only reshapes them, so the
 * published docs cannot drift from the repository.
 *
 * Output: build/site/ at the repository root, one HTML file per page plus
 * the diagram images. The build fails if any relative link or fragment in
 * the generated pages does not resolve, which is what protects against
 * broken xrefs.
 */
import { readFileSync, writeFileSync, mkdirSync, rmSync, readdirSync, copyFileSync, existsSync } from 'node:fs';
import { dirname, resolve, basename, posix } from 'node:path';
import { fileURLToPath } from 'node:url';
import Asciidoctor from 'asciidoctor';
import hljs from 'highlight.js/lib/core';
import java from 'highlight.js/lib/languages/java';
import xml from 'highlight.js/lib/languages/xml';
import bash from 'highlight.js/lib/languages/bash';
import properties from 'highlight.js/lib/languages/properties';
import kotlin from 'highlight.js/lib/languages/kotlin';
import groovy from 'highlight.js/lib/languages/groovy';
import json from 'highlight.js/lib/languages/json';
import yaml from 'highlight.js/lib/languages/yaml';
import javascript from 'highlight.js/lib/languages/javascript';
import plaintext from 'highlight.js/lib/languages/plaintext';

hljs.registerLanguage('java', java);
hljs.registerLanguage('xml', xml);
hljs.registerLanguage('html', xml);
hljs.registerLanguage('bash', bash);
hljs.registerLanguage('shell', bash);
hljs.registerLanguage('sh', bash);
hljs.registerLanguage('properties', properties);
hljs.registerLanguage('kotlin', kotlin);
hljs.registerLanguage('groovy', groovy);
hljs.registerLanguage('json', json);
hljs.registerLanguage('yaml', yaml);
hljs.registerLanguage('yml', yaml);
hljs.registerLanguage('javascript', javascript);
hljs.registerLanguage('js', javascript);
hljs.registerLanguage('plaintext', plaintext);
hljs.registerLanguage('text', plaintext);

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const DOCS = resolve(ROOT, 'docs/modules/ROOT');
const PAGES = resolve(DOCS, 'pages');
const IMAGES = resolve(DOCS, 'images');
const OUT = resolve(ROOT, 'build/site');

const SITE = 'https://sitenetsoft.org';
const DOCS_URL = `${SITE}/quarkus-tus/`;
const REPO = 'https://github.com/SiteNetSoft/quarkus-tus';
const BLOB = `${REPO}/blob/master/`;
const PROJECT = 'Quarkus TUS';

const asciidoctor = Asciidoctor();

/** Escape text for use in HTML content or attribute values. */
function escapeHtml(text) {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/** Minimal entity decoding, enough for what asciidoctor emits in code and titles. */
function decodeEntities(text) {
  return text
    .replace(/&#(\d+);/g, (all, code) => String.fromCodePoint(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (all, code) => String.fromCodePoint(parseInt(code, 16)))
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, '&');
}

/** Reduce an HTML fragment to escaped plain text. */
function plainText(html) {
  return escapeHtml(decodeEntities(html.replace(/<[^>]+>/g, '')).replace(/\s+/g, ' ').trim());
}

/**
 * The ordered site navigation comes from nav.adoc, which is the same list
 * Antora would use, so the two cannot disagree about page order or titles.
 */
export function parseNav(navSource) {
  const entries = [];
  for (const line of navSource.split('\n')) {
    const match = /^\*+\s+xref:([^[#]+?)(?:#[^[]*)?\[([^\]]*)\]/.exec(line.trim());
    if (match) entries.push({ file: match[1], title: match[2] });
  }
  return entries;
}

/** Output file name for a page: index.adoc -> index.html, foo.adoc -> foo.html. */
function outputName(file) {
  return file.replace(/\.adoc$/, '.html');
}

/**
 * Highlight at build time, so the page ships no runtime highlighter.
 * Asciidoctor already escaped the code, so it is decoded first and
 * highlight.js re-escapes it. Blocks carrying callouts are left alone, since
 * their markup would not survive the round trip.
 */
export function highlightCode(html) {
  return html.replace(
    /<pre class="highlight"><code(?: class="language-([^"]+)" data-lang="[^"]+")?>([\s\S]*?)<\/code><\/pre>/g,
    (all, lang, body) => {
      if (body.includes('<i class="conum"')) return all;
      const language = lang && hljs.getLanguage(lang) ? lang : 'plaintext';
      const value = hljs.highlight(decodeEntities(body), { language }).value;
      const langClass = lang ? ` language-${lang}" data-lang="${lang}` : '';
      return `<pre class="highlight"><code class="hljs${langClass}">${value}</code></pre>`;
    },
  );
}

/**
 * Make each diagram clickable so it can be opened full size, since the C4
 * views carry more detail than fits the column width. Only block images are
 * touched; inline images stay as they are.
 */
export function wrapDiagrams(html) {
  return html.replace(
    /(<div class="imageblock[^"]*">\s*<div class="content">\s*)<img src="([^"]+)"([^>]*)>/g,
    (all, open, src, rest) => {
      const alt = /alt="([^"]*)"/.exec(rest);
      const label = alt ? alt[1] : 'diagram';
      return `${open}<button class="diagram-zoom" type="button" aria-label="Open ${label} full size">`
        + `<img src="${src}"${rest}></button>`;
    },
  );
}

/**
 * Wrap tables so wide content scrolls instead of breaking the layout.
 * Admonitions are tables too in asciidoctor's output, but they are layout,
 * not data, so only the real table blocks are wrapped.
 */
export function wrapTables(html) {
  return html.replace(/<table class="tableblock[^"]*">[\s\S]*?<\/table>/g, m => `<div class="docs-table-wrap">${m}</div>`);
}

/**
 * Take the first paragraph of the preamble as the page tagline when the page
 * declares no :description:, reduced to plain text, and drop it from the body
 * since the hero already shows it. Derived from the page rather than
 * hardcoded, so the two cannot drift apart.
 */
export function extractTagline(html) {
  const match = /<div id="preamble">\s*<div class="sectionbody">\s*<div class="paragraph">\s*<p>([\s\S]*?)<\/p>\s*<\/div>/.exec(html);
  if (!match) return { tagline: null, html };
  const text = plainText(match[1]);
  // A lead-in ("Five artifacts are published:") introduces the block after it,
  // so it must stay in the body; only a self-contained opening paragraph moves.
  if (text.endsWith(':')) return { tagline: text.slice(0, -1), html };
  const body = html.replace(match[0], match[0].slice(0, match[0].indexOf('<div class="paragraph">')));
  return { tagline: text, html: body };
}

/** Collect h2 headings with their h3 children from the parsed document. */
export function buildToc(doc) {
  const entry = (section) => ({
    id: section.getId(),
    text: plainText(section.getTitle()),
    children: section.getSections().map(entry),
  });
  return doc.getSections().map(entry);
}

/** Render the "on this page" table of contents as nested lists. */
function renderToc(toc) {
  const item = (h) => {
    const kids = h.children.length
      ? `<ul class="docs-toc-sub">${h.children.map(item).join('')}</ul>`
      : '';
    return `<li><a href="#${h.id}">${h.text}</a>${kids}</li>`;
  };
  return `<ul class="docs-toc-list">${toc.map(item).join('')}</ul>`;
}

/** Render the site navigation, marking the page being read. */
function renderSiteNav(nav, current) {
  const item = (page) => {
    const href = outputName(page.file);
    const mark = href === current ? ' class="is-active" aria-current="page"' : '';
    return `<li><a href="${href}"${mark}>${escapeHtml(page.title)}</a></li>`;
  };
  return `<ul class="docs-nav-list">${nav.map(item).join('')}</ul>`;
}

const HERO_LINKS = [
  `<a href="${REPO}">GitHub</a>`,
  '<a href="https://central.sonatype.com/namespace/org.sitenetsoft">Maven Central</a>',
  '<a href="https://tus.io/">tus protocol</a>',
  `<a href="${SITE}/">SiteNetSoft</a>`,
].join('\n\t\t\t');

/** Assemble the full page, styled to match sitenetsoft.org. */
function renderPage({ title, tagline, description, content, toc, siteNav, path, isIndex }) {
  const pageTitle = isIndex ? `${PROJECT} — SiteNetSoft` : `${title} — ${PROJECT} — SiteNetSoft`;
  const kicker = isIndex ? '' : `\n\t\t<p class="docs-hero-kicker"><a href="index.html">${PROJECT}</a></p>`;
  return `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>${pageTitle}</title>
	<meta name="description" content="${description}">
	<link rel="canonical" href="${DOCS_URL}${path}">
	<meta property="og:type" content="website">
	<meta property="og:site_name" content="SiteNetSoft">
	<meta property="og:title" content="${isIndex ? PROJECT : `${title} — ${PROJECT}`}">
	<meta property="og:description" content="${description}">
	<meta property="og:url" content="${DOCS_URL}${path}">
	<meta property="og:image" content="${SITE}/assets/og-banner.png">
	<meta name="twitter:card" content="summary_large_image">
	<meta name="theme-color" content="#f6f9fb" media="(prefers-color-scheme: light)">
	<meta name="theme-color" content="#0e161d" media="(prefers-color-scheme: dark)">
	<link rel="icon" type="image/svg+xml" href="${SITE}/assets/sitenetsoft-logo.svg">
	<link rel="icon" href="${SITE}/favicon.ico" sizes="32x32">
	<link rel="apple-touch-icon" href="${SITE}/assets/apple-touch-icon.png">
	<link rel="stylesheet" href="${SITE}/assets/css/main.css">
	<script>
		(function () {
			var s = localStorage.getItem("pha-color-scheme");
			if (s === "light" || s === "dark") document.documentElement.dataset.theme = s;
		})();
	</script>
	<script src="${SITE}/assets/js/theme.js" defer></script>
	<style>
		/* Page-specific layout. Tokens come from the shared sitenetsoft.org stylesheet. */
		.docs-hero { text-align: center; padding-block: var(--space-5) var(--space-4); }
		.docs-hero-logo { width: 4.5rem; height: auto; }
		.docs-hero-kicker {
			margin: var(--space-3) 0 0; font-family: var(--font-mono);
			font-size: 0.9375rem; font-weight: 600; letter-spacing: 0.02em;
		}
		.docs-hero-kicker a { color: var(--color-muted); text-decoration: none; }
		.docs-hero-kicker a:hover { color: var(--color-accent); text-decoration: underline; }
		.docs-hero-kicker + .docs-hero-title { margin-top: var(--space-2); }
		.docs-hero-title {
			margin: var(--space-3) 0 0;
			font-size: clamp(2rem, 5vw, 2.75rem);
			font-weight: 800; letter-spacing: -0.02em; line-height: 1.1;
			font-family: var(--font-mono);
		}
		.docs-hero-tagline {
			margin: var(--space-2) auto 0; max-width: var(--measure);
			font-size: 1.0625rem; color: var(--color-muted);
		}
		.docs-hero-links {
			display: flex; gap: var(--space-3); justify-content: center;
			flex-wrap: wrap; margin-top: var(--space-4);
			font-family: var(--font-mono); font-size: 0.9375rem;
		}
		.docs-layout {
			max-width: var(--page-max); margin-inline: auto;
			padding: 0 var(--space-4) var(--space-6);
			display: grid; gap: var(--space-5);
			grid-template-columns: minmax(0, 1fr);
		}
		@media (min-width: 60rem) {
			.docs-layout { grid-template-columns: 15rem minmax(0, 1fr); align-items: start; }
			.docs-toc { position: sticky; top: var(--space-4); max-height: calc(100vh - 4rem); overflow-y: auto; }
		}
		.docs-toc-label {
			margin: 0 0 var(--space-2); font-size: 0.8125rem; font-weight: 700;
			text-transform: uppercase; letter-spacing: 0.06em; color: var(--color-muted);
		}
		/* The site navigation sits above the page outline, separated by a rule. */
		.docs-toc-label + .docs-toc-label,
		.docs-nav-list + .docs-toc-label {
			margin-top: var(--space-4); padding-top: var(--space-3);
			border-top: 1px solid var(--color-line);
		}
		.docs-nav-list, .docs-toc-list, .docs-toc-sub { list-style: none; margin: 0; padding: 0; }
		.docs-nav-list > li, .docs-toc-list > li { margin-bottom: var(--space-2); }
		.docs-toc-sub { margin: 0.25rem 0 0 var(--space-3); border-left: 1px solid var(--color-line); }
		.docs-toc-sub li { margin: 0.15rem 0; padding-left: var(--space-2); }
		/* Long names such as AbstractUploadStoreContractTest are a single unbreakable
		   token. Without this they overflow the sidebar, and since overflow-y:auto
		   forces overflow-x to compute to auto, that produced a horizontal
		   scrollbar. Wrapping removes the cause rather than hiding it. Matches
		   the site's own .card-name convention. */
		.docs-toc a {
			display: block; font-size: 0.875rem; text-decoration: none;
			color: var(--color-muted); overflow-wrap: anywhere;
		}
		.docs-toc a:hover { color: var(--color-accent); text-decoration: underline; }
		.docs-nav-list > li > a, .docs-toc-list > li > a { font-weight: 600; color: var(--color-text); }

		/* Page being read, and the section being read, updated as the reader scrolls. */
		.docs-toc a.is-active, .docs-toc a.is-current,
		.docs-nav-list > li > a.is-active,
		.docs-toc-list > li > a.is-current { color: var(--color-accent); }
		.docs-nav-list li, .docs-toc-sub li { position: relative; }
		.docs-nav-list li > a.is-active::before,
		.docs-toc-sub li > a.is-current::before {
			content: ""; position: absolute; left: calc(-1 * var(--space-2));
			top: 0; bottom: 0; width: 2px; background: var(--color-accent);
		}
		.docs-nav-list { padding-left: var(--space-2); }
		.docs-content { min-width: 0; }
		.docs-content h2 {
			margin: var(--space-5) 0 var(--space-3); padding-top: var(--space-3);
			border-top: 1px solid var(--color-line);
			font-size: 1.5rem; font-weight: 700; letter-spacing: -0.01em; scroll-margin-top: var(--space-4);
		}
		.docs-content h3 {
			margin: var(--space-4) 0 var(--space-2);
			font-size: 1.125rem; font-weight: 700; scroll-margin-top: var(--space-4);
		}
		.docs-content h4 {
			margin: var(--space-3) 0 var(--space-2);
			font-size: 1rem; font-weight: 700; scroll-margin-top: var(--space-4);
		}
		/* Section anchors (sectanchors): a permalink that only shows on hover. */
		.docs-content h2, .docs-content h3, .docs-content h4 { position: relative; }
		.docs-content .anchor {
			position: absolute; left: -1.25em; width: 1.25em; text-align: center;
			text-decoration: none; color: var(--color-muted); opacity: 0;
		}
		.docs-content .anchor::before { content: "§"; }
		.docs-content h2:hover .anchor, .docs-content h3:hover .anchor,
		.docs-content h4:hover .anchor, .docs-content .anchor:focus { opacity: 1; }
		.docs-content p, .docs-content li, .docs-content dd { max-width: var(--measure); }
		.docs-content .paragraph, .docs-content .ulist, .docs-content .olist,
		.docs-content .dlist, .docs-content .listingblock, .docs-content .literalblock,
		.docs-content .admonitionblock { margin: var(--space-3) 0; }
		.docs-content .paragraph > p { margin: 0; }
		.docs-content li .paragraph, .docs-content li .listingblock { margin: var(--space-2) 0; }
		.docs-content li > p { margin: 0; }
		.docs-content dt { font-weight: 700; margin-top: var(--space-2); }
		.docs-content dd { margin: 0.25rem 0 0 var(--space-3); }
		/* Block titles (.Caption above a listing or table). */
		.docs-content .listingblock > .title, .docs-content .literalblock > .title,
		.docs-content .imageblock > .title, .docs-content .tableblock > caption,
		.docs-content table > caption {
			text-align: left; font-size: 0.875rem; font-weight: 600;
			color: var(--color-muted); margin-bottom: var(--space-2);
		}
		.docs-content code {
			font-family: var(--font-mono); font-size: 0.875em; overflow-wrap: anywhere;
			background: var(--color-surface); border: 1px solid var(--color-line);
			border-radius: 0.3em; padding: 0.1em 0.35em;
		}
		.docs-content pre {
			background: var(--color-surface); border: 1px solid var(--color-line);
			border-radius: var(--radius); padding: var(--space-3); margin: 0;
			overflow-x: auto; font-size: 0.875rem;
		}
		.docs-content pre code { background: none; border: 0; padding: 0; font-size: inherit; }
		.docs-table-wrap { overflow-x: auto; margin: var(--space-3) 0; }
		.docs-content table { border-collapse: collapse; width: 100%; font-size: 0.9375rem; }
		.docs-content th, .docs-content td {
			border: 1px solid var(--color-line); padding: 0.5rem 0.7rem;
			text-align: left; vertical-align: top;
		}
		.docs-content th { background: var(--color-surface); font-weight: 700; white-space: nowrap; }
		.docs-content td p, .docs-content th p { margin: 0; }
		.docs-content td .paragraph + .paragraph { margin-top: var(--space-2); }
		.docs-content blockquote, .docs-content .quoteblock {
			margin: var(--space-3) 0; padding-left: var(--space-3);
			border-left: 3px solid var(--color-accent); color: var(--color-muted);
		}

		/* ---------- admonitions ----------
		   Asciidoctor lays NOTE/TIP/WARNING/IMPORTANT out as a two-cell table with
		   the label in the first cell. No icon font ships with the site, so the
		   label is text on an accent border, and the table borders are dropped. */
		.docs-content .admonitionblock > table {
			border-collapse: collapse; width: 100%; font-size: 0.9375rem;
			border-left: 3px solid var(--color-accent);
			background: var(--color-surface); border-radius: 0 var(--radius) var(--radius) 0;
		}
		.docs-content .admonitionblock td { border: 0; padding: var(--space-2) var(--space-3); }
		.docs-content .admonitionblock td.icon {
			width: 1%; white-space: nowrap; padding-right: 0; vertical-align: top;
		}
		.docs-content .admonitionblock td.icon .title {
			font-size: 0.75rem; font-weight: 700; text-transform: uppercase;
			letter-spacing: 0.06em; color: var(--color-accent); line-height: 1.7;
		}
		.docs-content .admonitionblock td.content { color: var(--color-text); }
		.docs-content .admonitionblock td.content > .title { font-weight: 700; margin-bottom: var(--space-2); }
		.docs-footer {
			max-width: var(--page-max); margin-inline: auto;
			padding: var(--space-4); border-top: 1px solid var(--color-line);
			color: var(--color-muted); font-size: 0.875rem; text-align: center;
		}

		/* ---------- syntax highlighting ----------
		   Applied at build time by highlight.js, so no highlighter ships to the
		   browser. Colours follow the site's own theming pattern. */
		:root {
			--hl-comment: #8a9199;
			--hl-keyword: #a626a4;
			--hl-string: #43843f;
			--hl-number: #97600a;
			--hl-function: #3a6fd8;
			--hl-tag: #d1453b;
			--hl-attr: #97600a;
		}
		@media (prefers-color-scheme: dark) {
			:root:not([data-theme="light"]) {
				--hl-comment: #7f8996;
				--hl-keyword: #c678dd;
				--hl-string: #98c379;
				--hl-number: #d19a66;
				--hl-function: #61afef;
				--hl-tag: #e06c75;
				--hl-attr: #d19a66;
			}
		}
		:root[data-theme="dark"] {
			--hl-comment: #7f8996;
			--hl-keyword: #c678dd;
			--hl-string: #98c379;
			--hl-number: #d19a66;
			--hl-function: #61afef;
			--hl-tag: #e06c75;
			--hl-attr: #d19a66;
		}
		.hljs-comment, .hljs-quote { color: var(--hl-comment); font-style: italic; }
		.hljs-keyword, .hljs-selector-tag, .hljs-literal,
		.hljs-built_in, .hljs-type { color: var(--hl-keyword); }
		.hljs-string, .hljs-regexp, .hljs-addition { color: var(--hl-string); }
		.hljs-number, .hljs-variable, .hljs-template-variable { color: var(--hl-number); }
		.hljs-title, .hljs-title_, .hljs-title.function_,
		.hljs-section, .hljs-meta { color: var(--hl-function); }
		.hljs-tag, .hljs-name, .hljs-selector-id, .hljs-deletion { color: var(--hl-tag); }
		.hljs-attr, .hljs-attribute, .hljs-selector-attr { color: var(--hl-attr); }
		.hljs-tag .hljs-string { color: var(--hl-string); }
		.hljs-emphasis { font-style: italic; }
		.hljs-strong { font-weight: 700; }

		/* ---------- diagrams ----------
		   PlantUML bakes a white background into the SVGs, so they are presented on
		   a light card in both themes rather than glaring out of the dark one. */
		.diagram-zoom {
			display: block; width: 100%; margin: var(--space-3) 0;
			padding: var(--space-3); border: 1px solid var(--color-line);
			border-radius: var(--radius); background: #ffffff;
			cursor: zoom-in; transition: border-color 0.15s ease;
		}
		.diagram-zoom:hover { border-color: var(--color-accent); }
		.diagram-zoom img { display: block; width: 100%; height: auto; }

		.lightbox {
			width: 100vw; max-width: 100vw; height: 100dvh; max-height: 100dvh;
			padding: 0; border: 0; background: transparent; overflow: hidden;
		}
		.lightbox::backdrop { background: rgb(0 0 0 / 0.82); }
		.lightbox-scroll {
			width: 100%; height: 100%; overflow: auto;
			display: grid; padding: var(--space-4);
			/* "safe" keeps the image centred while it fits, but falls back to start
			   alignment once it is larger than the viewport — a centred grid item
			   that overflows is unreachable on its left and top edges. The plain
			   value is the fallback for browsers without safe alignment. */
			place-items: center;
			place-items: safe center;
		}
		.lightbox-img {
			background: #ffffff; border-radius: var(--radius);
			/* Viewport units, not percentages: the scroll container is content-sized,
			   so a percentage max-height would not constrain the image at all. */
			max-width: calc(100vw - 3rem); max-height: calc(100dvh - 3rem);
			width: auto; height: auto; cursor: zoom-in;
		}
		/* Actual size: the container scrolls, so detailed diagrams can be panned. */
		.lightbox.is-zoomed .lightbox-img {
			max-width: none; max-height: none; width: auto; cursor: zoom-out;
		}
		.lightbox-close {
			position: fixed; top: var(--space-3); right: var(--space-3);
			display: inline-flex; align-items: center; justify-content: center;
			width: 2.5rem; height: 2.5rem; font-size: 1.25rem; line-height: 1;
			color: var(--color-text); background: var(--color-surface);
			border: 1px solid var(--color-line); border-radius: 50%; cursor: pointer;
		}
		.lightbox-close:hover { color: var(--color-accent); border-color: var(--color-accent); }
		.lightbox-hint {
			position: fixed; bottom: var(--space-3); left: 50%; translate: -50% 0;
			padding: 0.35rem 0.75rem; font-size: 0.8125rem;
			color: var(--color-muted); background: var(--color-surface);
			border: 1px solid var(--color-line); border-radius: 999px;
		}

		/* ---------- sidebar disclosure ---------- */
		.docs-toc-summary { display: none; }

		@media (min-width: 60rem) {
			/* Wide screens keep the sidebar permanently open; the summary is hidden
			   and the plain labels take its place. */
			.docs-toc-summary { display: none; }
			.docs-toc-label { display: block; }
		}

		@media (max-width: 59.999rem) {
			.docs-toc {
				background: var(--color-surface);
				border: 1px solid var(--color-line);
				border-radius: var(--radius);
				padding: var(--space-3);
			}
			.docs-toc-summary {
				display: list-item;
				cursor: pointer;
				font-size: 0.8125rem; font-weight: 700;
				text-transform: uppercase; letter-spacing: 0.06em;
				color: var(--color-muted);
			}
			.docs-toc-label { display: none; }
			.docs-toc-details[open] .docs-nav-list { margin-top: var(--space-3); }
			.docs-toc-details[open] .docs-nav-list + .docs-toc-label { display: block; }
		}

		/* ---------- phones ---------- */
		@media (max-width: 40rem) {
			.docs-layout { padding-inline: var(--space-3); padding-bottom: var(--space-5); }
			.docs-hero { padding-inline: var(--space-3); padding-block: var(--space-4) var(--space-3); }
			.docs-hero-logo { width: 3.5rem; }
			.docs-hero-links { gap: var(--space-2) var(--space-3); margin-top: var(--space-3); }
			.docs-content pre { padding: var(--space-2); font-size: 0.8125rem; }
			.docs-content h2 { margin-top: var(--space-4); font-size: 1.3125rem; }
			.docs-content table { font-size: 0.875rem; }
			.docs-content th, .docs-content td { padding: 0.4rem 0.55rem; }
			.docs-content .anchor { display: none; }
			.docs-footer { padding-inline: var(--space-3); }
			.diagram-zoom { padding: var(--space-2); }
			.lightbox-scroll { padding: var(--space-2); }
			.lightbox-hint { display: none; }
		}
	</style>
</head>
<body>
	<button class="theme-toggle" id="theme-toggle" type="button" hidden>
		<svg class="icon-system" viewBox="0 0 24 24" width="18" height="18" aria-hidden="true" focusable="false" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="9"/><path d="M 12 3 a 9 9 0 0 1 0 18 Z" fill="currentColor" stroke="none"/></svg>
		<svg class="icon-light" viewBox="0 0 24 24" width="18" height="18" aria-hidden="true" focusable="false" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>
		<svg class="icon-dark" viewBox="0 0 24 24" width="18" height="18" aria-hidden="true" focusable="false" fill="none" stroke="currentColor" stroke-width="2" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
	</button>

	<header class="docs-hero">
		<a href="${SITE}/"><img class="docs-hero-logo" src="${SITE}/assets/sitenetsoft-logo.svg" alt="SiteNetSoft" width="72" height="72"></a>${kicker}
		<h1 class="docs-hero-title">${title}</h1>
		<p class="docs-hero-tagline">${tagline}</p>
		<nav class="docs-hero-links">
			${HERO_LINKS}
		</nav>
	</header>

	<div class="docs-layout">
		<nav class="docs-toc" aria-label="Documentation">
			<details class="docs-toc-details" id="docs-toc" open>
				<summary class="docs-toc-summary">Documentation</summary>
				<p class="docs-toc-label">${PROJECT}</p>
				${siteNav}
				<p class="docs-toc-label">On this page</p>
				${toc}
			</details>
		</nav>
		<main class="docs-content">
${content}
		</main>
	</div>

	<dialog class="lightbox" id="lightbox">
		<button class="lightbox-close" id="lightbox-close" type="button" aria-label="Close">&times;</button>
		<div class="lightbox-scroll" id="lightbox-scroll">
			<img class="lightbox-img" id="lightbox-img" src="" alt="">
		</div>
		<p class="lightbox-hint">Click the diagram to toggle actual size · Esc to close</p>
	</dialog>

	<footer class="docs-footer">
		<p>Released under the <a href="${BLOB}LICENSE">Apache License 2.0</a>. Part of <a href="${SITE}/">SiteNetSoft</a>.</p>
	</footer>
	<script>
		// The sidebar is long, so it is collapsed on narrow screens and pinned
		// open on wide ones. Without JS it stays open, which is still usable.
		// Diagram lightbox. Uses <dialog> so Esc and focus handling come from the
		// platform; without JS the diagrams are still shown inline at full width.
		(function () {
			var dialog = document.getElementById("lightbox");
			var img = document.getElementById("lightbox-img");
			var scroll = document.getElementById("lightbox-scroll");
			if (!dialog || !dialog.showModal) return;

			document.querySelectorAll(".diagram-zoom").forEach(function (button) {
				button.addEventListener("click", function () {
					var source = button.querySelector("img");
					img.src = source.getAttribute("src");
					img.alt = source.getAttribute("alt") || "";
					dialog.classList.remove("is-zoomed");
					dialog.showModal();
				});
			});

			img.addEventListener("click", function (event) {
				event.stopPropagation();
				var zoomed = dialog.classList.toggle("is-zoomed");
				// Zooming in keeps the middle of the diagram in view rather than
				// jumping to its top-left corner, which is where safe alignment
				// parks an image larger than the viewport. scrollIntoView flushes
				// layout itself, so the new size is accounted for without waiting
				// on an animation frame.
				if (zoomed) {
					img.scrollIntoView({ block: "center", inline: "center" });
				} else {
					scroll.scrollTo(0, 0);
				}
			});

			// Clicking the backdrop or anywhere outside the image closes it.
			scroll.addEventListener("click", function () { dialog.close(); });
			document.getElementById("lightbox-close")
				.addEventListener("click", function () { dialog.close(); });
			dialog.addEventListener("close", function () { img.src = ""; });
		})();

		// Highlight the section being read. The activation line sits a little below
		// the top of the viewport so a heading counts as current once it reaches
		// reading position rather than the moment it appears.
		(function () {
			var nav = document.querySelector(".docs-toc");
			if (!nav) return;

			// Prototype-free: a heading with the id "constructor" or "valueOf" would
			// otherwise be truthy through the prototype chain and then throw.
			var links = Object.create(null);
			nav.querySelectorAll("a[href^='#']").forEach(function (a) {
				links[decodeURIComponent(a.getAttribute("href").slice(1))] = a;
			});

			var headings = Array.prototype.filter.call(
				document.querySelectorAll(".docs-content h2[id], .docs-content h3[id]"),
				function (h) { return links[h.id]; },
			);
			if (!headings.length) return;

			var current = null;

			function keepVisible(link) {
				// Only ever scrolls the sidebar, never the page.
				var box = nav.getBoundingClientRect();
				var item = link.getBoundingClientRect();
				if (item.top < box.top) nav.scrollTop -= box.top - item.top + 8;
				else if (item.bottom > box.bottom) nav.scrollTop += item.bottom - box.bottom + 8;
			}

			function update() {
				var active = headings[0];
				for (var i = 0; i < headings.length; i++) {
					if (headings[i].getBoundingClientRect().top <= 120) active = headings[i];
					else break;
				}
				if (active === current) return;
				current = active;
				Object.keys(links).forEach(function (id) {
					links[id].classList.remove("is-current");
					links[id].removeAttribute("aria-current");
				});
				var link = links[active.id];
				if (!link) return;
				link.classList.add("is-current");
				link.setAttribute("aria-current", "true");
				keepVisible(link);
			}

			window.addEventListener("scroll", update, { passive: true });
			window.addEventListener("resize", update, { passive: true });
			update();
		})();

		(function () {
			var toc = document.getElementById("docs-toc");
			if (!toc) return;
			var wide = window.matchMedia("(min-width: 60rem)");
			var sync = function () { toc.open = wide.matches; };
			sync();
			wide.addEventListener("change", sync);
		})();
	</script>
</body>
</html>
`;
}

/**
 * Turn one AsciiDoc page into a finished page. All pages share the shell, so
 * they cannot drift apart in styling or behaviour.
 */
export function buildPage(file, nav) {
  const doc = asciidoctor.loadFile(resolve(PAGES, file), {
    safe: 'safe',
    // Includes such as include::./includes/attributes.adoc[] resolve from here.
    base_dir: PAGES,
    standalone: false,
    attributes: {
      // xref:page.adoc[] becomes page.html, matching the output file names.
      outfilesuffix: '.html',
      imagesdir: 'images',
      sectanchors: '',
      // Asciidoctor's defaults, stated so the auto ids the pages xref to
      // (_why_writes_are_staged and friends) cannot change under us.
      idprefix: '_',
      idseparator: '_',
      // Icons would need Font Awesome; admonition labels are text instead.
      icons: null,
      // Highlighting happens below, on the rendered HTML.
      'source-highlighter': null,
    },
  });

  const name = outputName(file);
  const isIndex = name === 'index.html';
  const title = plainText(doc.getDoctitle() || basename(file, '.adoc'));
  const description = doc.getAttribute('description');

  let html = doc.convert();
  let tagline = description ? escapeHtml(description) : null;
  if (!tagline) {
    // No :description: — the first paragraph is the tagline, and moves to the hero.
    const extracted = extractTagline(html);
    tagline = extracted.tagline || '';
    html = extracted.html;
  }

  const page = renderPage({
    title,
    tagline,
    description: tagline,
    path: isIndex ? '' : name,
    isIndex,
    siteNav: renderSiteNav(nav, name),
    toc: renderToc(buildToc(doc)),
    content: wrapDiagrams(wrapTables(highlightCode(html))),
  });
  return { name, page };
}

/**
 * Check every relative href and src in the generated pages. A missing file,
 * or a #fragment with no matching id in the target page, fails the build.
 * This is what turns a broken xref into a red CI run rather than a 404.
 */
export function checkLinks(out) {
  const files = readdirSync(out).filter(f => f.endsWith('.html'));
  const pages = new Map(files.map(f => [f, readFileSync(resolve(out, f), 'utf8')]));
  const idsOf = new Map();
  const ids = (file) => {
    if (!idsOf.has(file)) {
      const set = new Set();
      for (const m of pages.get(file).matchAll(/\sid="([^"]+)"/g)) set.add(m[1]);
      idsOf.set(file, set);
    }
    return idsOf.get(file);
  };

  const problems = [];
  for (const [file, html] of pages) {
    for (const m of html.matchAll(/\s(?:href|src)="([^"]*)"/g)) {
      const target = decodeEntities(m[1]);
      if (/^([a-z][a-z0-9+.-]*:|\/\/|\/)/i.test(target) || target === '') continue;

      const [pathPart, fragment] = target.split('#');
      const targetFile = pathPart ? posix.normalize(posix.join(dirname(file), pathPart)) : file;
      if (!existsSync(resolve(out, targetFile))) {
        problems.push(`${file}: ${target} -> missing file ${targetFile}`);
        continue;
      }
      if (fragment !== undefined) {
        if (!pages.has(targetFile)) {
          problems.push(`${file}: ${target} -> fragment on a non-HTML target`);
        } else if (!ids(targetFile).has(fragment)) {
          problems.push(`${file}: ${target} -> no id "${fragment}" in ${targetFile}`);
        }
      }
    }
  }
  return problems;
}

function main() {
  const nav = parseNav(readFileSync(resolve(DOCS, 'nav.adoc'), 'utf8'));
  const inNav = new Set(nav.map(p => p.file));
  // Pages not in the nav are still published, after the ones that are.
  const extra = readdirSync(PAGES)
    .filter(f => f.endsWith('.adoc') && !inNav.has(f))
    .sort();
  const files = [...nav.map(p => p.file), ...extra];

  // Clear the directory first so nothing stale is ever picked up by the upload.
  rmSync(OUT, { recursive: true, force: true });
  mkdirSync(resolve(OUT, 'images'), { recursive: true });

  let bytes = 0;
  for (const file of files) {
    const { name, page } = buildPage(file, nav);
    writeFileSync(resolve(OUT, name), page);
    bytes += page.length;
  }

  const images = readdirSync(IMAGES).filter(f => f.endsWith('.svg'));
  for (const file of images) {
    copyFileSync(resolve(IMAGES, file), resolve(OUT, 'images', file));
  }

  const problems = checkLinks(OUT);
  if (problems.length) {
    console.error(`Link check failed with ${problems.length} problem(s):`);
    for (const p of problems) console.error(`  ${p}`);
    process.exit(1);
  }

  console.log(
    `Built ${files.length} pages (${(bytes / 1024).toFixed(1)} kB) into build/site with `
    + `${images.length} images; ${extra.length} page(s) outside nav.adoc; link check passed.`,
  );
}

if (process.argv[1] && process.argv[1].endsWith('build.js')) main();
