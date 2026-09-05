#!/usr/bin/env sh
# Renders docs/diagrams/*.puml to SVG under docs/modules/ROOT/images.
# The SVGs are committed so GitHub and Antora show them without a build step;
# re-run this after editing a .puml and commit both.
#
# Needs a PlantUML jar (1.2026.x or newer bundles the C4 standard library):
#   PLANTUML_JAR=/path/to/plantuml.jar scripts/render-diagrams.sh
set -eu
cd "$(dirname "$0")/.."
JAR="${PLANTUML_JAR:-$HOME/.local/plantuml/plantuml.jar}"
[ -f "$JAR" ] || { echo "PlantUML jar not found at $JAR (set PLANTUML_JAR)" >&2; exit 1; }
java -jar "$JAR" -tsvg -o ../modules/ROOT/images docs/diagrams/*.puml
echo "rendered: $(ls docs/modules/ROOT/images/*.svg | wc -l) diagrams"
