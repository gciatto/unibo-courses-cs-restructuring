"""Re-render the Mermaid diagrams of a restructuring attempt without the LLM.

    .venv/bin/python -m restructuring.render ATTEMPT_DIR CLUSTER_COURSES_YML

Rewrites every restructure-proposal-*.mmd from its validated proposal YAML,
renders it to SVG with mermaid-cli (SVG text labels, so Inkscape can read
them), colours topic keys by source-course scope, and exports a PDF with
Inkscape plus an editable draw.io file laid out as the SVG. Needs
`npm install` and `inkscape` on PATH.
"""
from __future__ import annotations

import argparse
import html
import pathlib
import re
import subprocess

from clustering.export_cluster_courses import build_rows
from restructuring.io import (
    TOPIC_ORIGIN_COLOURS,
    TOPIC_ORIGIN_LEGEND,
    load_clusters,
    load_global_corpus,
    load_yaml_mapping,
    topic_origins,
    write_proposal_mermaid,
)
from restructuring.models import RestructuringProposal, SourceCourseMapping

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parent.parent
OUTER_ROW = re.compile(r'(<tspan class="text-outer-tspan row")([^>]*>)((?:<tspan[^>]*>[^<]*</tspan>)*)</tspan>')
SVG_NODE = re.compile(
    r'<g class="node [^"]*" id="my-svg-flowchart-(\w+)-\d+"[^>]*transform="translate\(([-\d.]+), ?([-\d.]+)\)">'
    r'<rect[^>]*style="fill:([^;"]+);stroke:([^;"]+)"[^>]*width="([\d.]+)" height="([\d.]+)"'
)
SVG_EDGE = re.compile(r'id="my-svg-L_([A-Za-z0-9]+)_([A-Za-z0-9]+)_\d+"')


def fix_svg(svg: str, colours: dict[str, str]) -> str:
    """Make a mermaid-cli SVG Inkscape-friendly and colour label rows whose
    text is a key of `colours`."""
    # Inkscape drops style attributes containing !important (boxes vanish)
    # and the leading spaces of word tspans.
    svg = svg.replace(" !important", "").replace("<svg ", '<svg xml:space="preserve" ', 1)

    def colour(match: re.Match[str]) -> str:
        text = html.unescape(re.sub(r"<[^>]+>", "", match.group(3))).strip()
        if text not in colours:
            return match.group(0)
        return f'{match.group(1)} style="fill:{colours[text]}"{match.group(2)}{match.group(3)}</tspan>'

    return OUTER_ROW.sub(colour, svg)


def svg_to_drawio(svg: str, name: str) -> str:
    """Editable draw.io diagram with the boxes, (coloured) label rows and
    arrows of a fix_svg-processed mermaid-cli flowchart, at the same positions."""
    cells = ['<mxCell id="0"/>', '<mxCell id="1" parent="0"/>']
    nodes = list(SVG_NODE.finditer(svg))
    for index, node in enumerate(nodes):
        alias, x, y, fill, stroke, width, height = node.groups()
        body = svg[node.end():nodes[index + 1].start() if index + 1 < len(nodes) else len(svg)]
        rows = []
        for row in OUTER_ROW.finditer(body):
            text = html.escape(html.unescape(re.sub(r"<[^>]+>", "", row.group(3))).strip())
            colour = re.search(r"fill:(#[0-9a-fA-F]+)", row.group(2))
            rows.append(f'<font color="{colour.group(1)}">{text}</font>' if colour else text)
        style = f"whiteSpace=wrap;html=1;fillColor={fill};strokeColor={stroke};fontSize=14;"
        cells.append(
            f'<mxCell id="{alias}" value="{html.escape("<br>".join(rows))}" style="{style}" vertex="1" parent="1">'
            f'<mxGeometry x="{float(x) - float(width) / 2:.1f}" y="{float(y) - float(height) / 2:.1f}" '
            f'width="{float(width):.1f}" height="{float(height):.1f}" as="geometry"/></mxCell>'
        )
    for index, (source, target) in enumerate(dict.fromkeys(SVG_EDGE.findall(svg))):
        cells.append(
            f'<mxCell id="e{index}" style="edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;endArrow=block;" '
            f'edge="1" parent="1" source="{source}" target="{target}"><mxGeometry relative="1" as="geometry"/></mxCell>'
        )
    return (
        f'<mxfile><diagram name="{html.escape(name)}"><mxGraphModel><root>'
        + "".join(cells) + "</root></mxGraphModel></diagram></mxfile>\n"
    )


def render_attempt(attempt_dir: pathlib.Path, cluster_manifest: pathlib.Path) -> list[pathlib.Path]:
    scopes = {row["course_id"]: row["course_scope"] for row in build_rows(cluster_manifest)}
    clusters = {cluster.cluster_id: cluster for cluster in load_clusters(cluster_manifest)}
    outputs = []
    for yaml_path in sorted(attempt_dir.glob("restructure-proposal-*.yml")):
        payload = load_yaml_mapping(yaml_path, "Restructuring proposal")
        proposal = RestructuringProposal.model_validate(
            {key: payload[key] for key in ("proposed_topics", "proposed_courses", "prerequisites")}
        )
        mappings = [
            SourceCourseMapping(course_id=item["course_id"], proposed_course_keys=item["proposed_course_keys"])
            for item in payload["source_course_mappings"]
        ]
        cluster = (
            clusters[payload["cluster"]["id"]] if "cluster" in payload
            else load_global_corpus(clusters.values())
        )
        origins = topic_origins(proposal, payload["source_course_topic_assignments"], scopes)
        colours = {key: TOPIC_ORIGIN_COLOURS[origin] for key, origin in origins.items()}
        colours.update({row: TOPIC_ORIGIN_COLOURS[scope] for scope, row in TOPIC_ORIGIN_LEGEND.items()})
        for mmd in write_proposal_mermaid(attempt_dir, yaml_path.stem, cluster, proposal, mappings):
            svg, pdf = mmd.with_suffix(".svg"), mmd.with_suffix(".pdf")
            subprocess.run(
                [REPOSITORY_ROOT / "node_modules/.bin/mmdc", "-q", "-c", REPOSITORY_ROOT / "mermaid-config.json",
                 "-i", mmd, "-o", svg],
                check=True,
            )
            fixed = fix_svg(svg.read_text(encoding="utf-8"), colours)
            svg.write_text(fixed, encoding="utf-8")
            drawio = mmd.with_suffix(".drawio")
            drawio.write_text(svg_to_drawio(fixed, mmd.stem), encoding="utf-8")
            subprocess.run(
                ["inkscape", svg, "--export-type=pdf", f"--export-filename={pdf}"],
                check=True, capture_output=True,
            )
            outputs += [svg, pdf, drawio]
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("attempt_dir", type=pathlib.Path)
    parser.add_argument("cluster_manifest", type=pathlib.Path, help="cluster_courses.yml used by the attempt")
    args = parser.parse_args()
    for path in render_attempt(args.attempt_dir, args.cluster_manifest):
        print(path)


if __name__ == "__main__":
    main()
