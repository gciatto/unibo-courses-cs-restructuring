"""Topic co-occurrence graph from a restructuring attempt's topics-global.yml.

Two topics are linked when they are assigned to the same source course; the
edge weight is the number of courses in which they co-occur. Course
memberships come from ``topic_weights.<topic>.per_course``. Topics listed in
``topics`` but assigned to no course are kept as isolated nodes.

    .venv/bin/python -m network_analysis.topic_graph \
        data/restructuring/attempt-.../topics-global.yml [--output-dir DIR]
"""

from __future__ import annotations

import argparse
import csv
import itertools
import os
import pathlib
import statistics
from collections import Counter

import networkx as nx
import yaml


def load_course_topics(path: pathlib.Path) -> tuple[dict[str, str], dict[str, dict[str, float]]]:
    """Return topic descriptions and, per course, its topics with their credit share.

    The share is the course's credits spread evenly over its topics.
    """
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    descriptions = {str(key): str(text) for key, text in (data.get("topics") or {}).items()}
    course_topics: dict[str, dict[str, float]] = {str(course): {} for course in data.get("course_ids") or []}
    for topic, weight in (data.get("topic_weights") or {}).items():
        descriptions.setdefault(str(topic), "")
        for course, share in ((weight or {}).get("per_course") or {}).items():
            # Course IDs are opaque strings: never let YAML turn them into numbers.
            course_topics.setdefault(str(course), {})[str(topic)] = float(share)
    return descriptions, course_topics


def load_course_names(directory: pathlib.Path) -> dict[str, str]:
    """Course names from the attempt's topics-of-course-<id>.yml files, keyed by the ID in the filename."""
    names = {}
    for path in directory.glob("topics-of-course-*.yml"):
        course = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("course") or {}
        names[path.stem.removeprefix("topics-of-course-")] = str(course.get("name") or "")
    return names


def build_graph(descriptions: dict[str, str], course_topics: dict[str, dict[str, float]]) -> nx.Graph:
    courses_per_topic = Counter(topic for topics in course_topics.values() for topic in topics)
    weights: Counter[tuple[str, str]] = Counter()
    for topics in course_topics.values():
        weights.update(itertools.combinations(sorted(topics), 2))

    graph = nx.Graph()
    for topic in sorted(descriptions):
        graph.add_node(topic, description=descriptions[topic], courses=courses_per_topic[topic])
    for (left, right), weight in weights.items():
        graph.add_edge(left, right, weight=weight)
    return graph


def print_statistics(graph: nx.Graph, course_topics: dict[str, dict[str, float]]) -> None:
    degrees = dict(graph.degree())
    strengths = dict(graph.degree(weight="weight"))
    components = sorted(nx.connected_components(graph), key=len, reverse=True)
    edge_weights = Counter(weight for _, _, weight in graph.edges(data="weight"))
    isolated = sorted(nx.isolates(graph))

    def summary(values: list[int]) -> str:
        return (
            f"min {min(values)}, max {max(values)}, "
            f"mean {statistics.mean(values):.2f}, median {statistics.median(values):g}"
        )

    print(f"Courses:              {len(course_topics)}")
    print(f"Nodes (topics):       {graph.number_of_nodes()}")
    print(f"Edges:                {graph.number_of_edges()}")
    print(f"Density:              {nx.density(graph):.4f}")
    print(f"Connected components: {len(components)} (largest: {len(components[0]) if components else 0} nodes)")
    print(f"Isolated topics:      {len(isolated)}" + (f" ({', '.join(isolated)})" if isolated else ""))
    print(f"Degree:               {summary(list(degrees.values()))}")
    print(f"Weighted degree:      {summary(list(strengths.values()))}")

    print("\nEdge weight distribution (courses shared: edges):")
    for weight, count in sorted(edge_weights.items()):
        print(f"  {weight:>3}: {count}")

    print("\nDegree distribution (degree: topics):")
    for degree, count in sorted(Counter(degrees.values()).items()):
        print(f"  {degree:>4}: {count}")

    print("\nTop 15 topics by degree (degree, weighted degree, courses):")
    for topic in sorted(degrees, key=lambda t: (-degrees[t], -strengths[t], t))[:15]:
        print(f"  {topic}: {degrees[topic]}, {strengths[topic]}, {graph.nodes[topic]['courses']}")


def _load_pyplot():
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def layout(graph: nx.Graph) -> dict[str, tuple[float, float]]:
    """Spring layout of the largest component, small components in a row below it.

    A single spring layout of the whole graph pushes the disconnected
    components far apart and shrinks the main one to a dot.
    """
    components = sorted(nx.connected_components(graph), key=lambda c: (-len(c), min(c)))
    if not components:
        return {}
    giant = graph.subgraph(components[0])
    positions = nx.spring_layout(giant, weight="weight", seed=0, iterations=200, k=3 / len(giant) ** 0.5)
    rest = components[1:]
    for index, component in enumerate(rest):
        center = (-1 + 2 * (index + 0.5) / len(rest), -1.25)
        positions.update(nx.spring_layout(graph.subgraph(component), seed=0, scale=0.06, center=center))
    return positions


def plot_graph(graph: nx.Graph, path: pathlib.Path, labels: int = 30) -> None:
    plt = _load_pyplot()
    positions = layout(graph)
    degrees = dict(graph.degree())
    strengths = dict(graph.degree(weight="weight"))
    weights = [weight for _, _, weight in graph.edges(data="weight")]
    max_weight = max(weights, default=1)

    fig, ax = plt.subplots(figsize=(24, 24))
    nx.draw_networkx_edges(
        graph,
        positions,
        ax=ax,
        width=[0.15 + 1.5 * weight / max_weight for weight in weights],
        alpha=0.15,
        edge_color="grey",
    )
    nodes = nx.draw_networkx_nodes(
        graph,
        positions,
        ax=ax,
        node_size=[4 + 1.5 * degrees[node] for node in graph],
        node_color=[degrees[node] for node in graph],
        cmap="viridis",
        linewidths=0,
    )
    hubs = sorted(graph, key=lambda node: (-strengths[node], node))[:labels]
    nx.draw_networkx_labels(graph, positions, labels={node: node for node in hubs}, font_size=8, ax=ax)
    fig.colorbar(nodes, ax=ax, label="Degree", shrink=0.4)
    ax.set_title(
        f"Topic co-occurrence graph: {graph.number_of_nodes()} topics, {graph.number_of_edges()} edges "
        f"(edge width = shared courses; labels: top {labels} by weighted degree)"
    )
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def topic_ects(course_topics: dict[str, dict[str, float]]) -> dict[str, float]:
    """A topic's ECTS as in topics-global.yml: the median of its per-course credit shares."""
    shares_per_topic: dict[str, list[float]] = {}
    for shares in course_topics.values():
        for topic, share in shares.items():
            shares_per_topic.setdefault(topic, []).append(share)
    return {topic: statistics.median(shares) for topic, shares in shares_per_topic.items()}


def estimated_ects(topics, ects: dict[str, float]) -> float:
    """Estimated ECTS of a new course made of ``topics``; unassigned topics count 0."""
    return sum(ects.get(topic, 0.0) for topic in topics)


def louvain_communities(graph: nx.Graph, resolution: float) -> list[set[str]]:
    """Weighted Louvain; a resolution above 1 favours smaller communities everywhere."""
    return nx.community.louvain_communities(graph, weight="weight", resolution=resolution, seed=0)


def ects_capped_communities(graph: nx.Graph, ects: dict[str, float], max_ects: float) -> list[set[str]]:
    """Weighted Louvain, then split only the communities above ``max_ects``.

    Communities within the cap are kept intact. One above it is split by
    Louvain on its own subgraph when that gives several parts and none of a
    single topic; otherwise (typically one course's topics, all equally
    linked) by a weighted Kernighan-Lin bisection into two balanced halves.
    Parts still above the cap are split again. A single topic above the cap
    cannot be split and is kept.
    """
    done, pending = [], louvain_communities(graph, 1.0)
    while pending:
        members = pending.pop()
        if len(members) == 1 or estimated_ects(members, ects) <= max_ects:
            done.append(members)
            continue
        subgraph = graph.subgraph(members)
        parts = louvain_communities(subgraph, 1.0)
        if len(parts) < 2 or any(len(part) == 1 for part in parts):
            parts = nx.community.kernighan_lin_bisection(subgraph, weight="weight", seed=0)
        pending.extend(set(part) for part in parts)
    return done


def order_communities(graph: nx.Graph, found: list[set[str]]) -> list[list[str]]:
    """Communities largest first, topics most central first; numbered 1.. on the nodes.

    Topics in each community are ordered by weighted degree inside it, and the
    1-based ``community`` id is stored on every node.
    """
    communities = []
    for members in found:
        inside = graph.subgraph(members).degree(weight="weight")
        communities.append(sorted(members, key=lambda topic: (-inside[topic], topic)))
    communities.sort(key=lambda topics: (-len(topics), topics[0]))
    for number, topics in enumerate(communities, start=1):
        for topic in topics:
            graph.nodes[topic]["community"] = number
    return communities


def print_community_statistics(
    graph: nx.Graph, communities: list[list[str]], ects: dict[str, float], method: str
) -> None:
    sizes = [len(topics) for topics in communities]
    credits = [estimated_ects(topics, ects) for topics in communities]
    modularity = nx.community.modularity(graph, [set(topics) for topics in communities], weight="weight")
    print(
        f"Communities ({method}): {len(communities)}; "
        f"topics min {min(sizes)}, max {max(sizes)}, median {statistics.median(sizes):g}; "
        f"est. ECTS min {min(credits):.1f}, max {max(credits):.1f}, median {statistics.median(credits):.1f}; "
        f"single-topic {sizes.count(1)}; modularity {modularity:.3f}"
    )


def community_course_links(
    communities: list[list[str]], course_topics: dict[str, dict[str, float]]
) -> list[dict[str, tuple[int, float]]]:
    """For each community (a new course), the old courses sharing topics with it.

    Maps old course ID to (shared topics, credits the old course spends on them),
    old courses ordered by credits, then shared topics, descending.
    """
    links = []
    for topics in communities:
        members = set(topics)
        found = {}
        for course, shares in course_topics.items():
            shared = members.intersection(shares)
            if shared:
                found[course] = (len(shared), sum(shares[topic] for topic in shared))
        links.append(dict(sorted(found.items(), key=lambda item: (-item[1][1], -item[1][0], item[0]))))
    return links


def course_label(course: str, names: dict[str, str]) -> str:
    return f"{course} {names[course]}" if names.get(course) else course


def write_community_table(
    communities: list[list[str]],
    links: list[dict[str, tuple[int, float]]],
    names: dict[str, str],
    ects: dict[str, float],
    csv_path: pathlib.Path,
    md_path: pathlib.Path,
) -> None:
    """One row per community; old courses are listed as "<id> <name> (<shared topics>)"."""

    def old_courses(number: int) -> list[str]:
        return [f"{course_label(course, names)} ({count})" for course, (count, _) in links[number - 1].items()]

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["community", "size", "estimated_ects", "topics", "old_courses"])
        for number, topics in enumerate(communities, start=1):
            writer.writerow([
                number, len(topics), round(estimated_ects(topics, ects), 2),
                "; ".join(topics), "; ".join(old_courses(number)),
            ])
    lines = ["| Community | Size | Est. ECTS | Topics | Old courses (shared topics) |", "|---:|---:|---:|---|---|"]
    lines += [
        f"| {number} | {len(topics)} | {estimated_ects(topics, ects):.1f} | {', '.join(topics)} "
        f"| {'; '.join(old_courses(number))} |"
        for number, topics in enumerate(communities, start=1)
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _ribbon(x0: float, y0: tuple[float, float], x1: float, y1: tuple[float, float]):
    """Closed cubic-Bezier band from interval y0 at x0 to interval y1 at x1."""
    from matplotlib.path import Path

    dx = (x1 - x0) * 0.45
    vertices = [
        (x0, y0[0]), (x0 + dx, y0[0]), (x1 - dx, y1[0]), (x1, y1[0]),
        (x1, y1[1]), (x1 - dx, y1[1]), (x0 + dx, y0[1]), (x0, y0[1]), (x0, y0[0]),
    ]
    codes = [Path.MOVETO] + [Path.CURVE4] * 3 + [Path.LINETO] + [Path.CURVE4] * 3 + [Path.CLOSEPOLY]
    return Path(vertices, codes)


def plot_community_sankey(
    communities: list[list[str]],
    links: list[dict[str, tuple[int, float]]],
    names: dict[str, str],
    course_topics: dict[str, dict[str, float]],
    ects: dict[str, float],
    method: str,
    path: pathlib.Path,
) -> None:
    """Old courses (left) flow into communities as new courses (right).

    A flow is the credits an old course spends on topics of that community, so
    an old course's height is its credits and a community's height the current
    credits spent on its topics across all old courses. That is not the size of
    the new course: as in topics-global.yml, a topic's ECTS is the median of its
    per-course shares, and the new course's estimated ECTS is their sum.
    Communities without any course (unassigned topics) are omitted.
    """
    plt = _load_pyplot()
    from matplotlib.patches import PathPatch, Rectangle

    flows = {
        (course, number): credits
        for number, found in enumerate(links, start=1)
        for course, (_, credits) in found.items()
        if credits > 0
    }
    right = [number for number in range(1, len(communities) + 1) if any(n == number for _, n in flows)]
    totals_right = {number: sum(v for (_, n), v in flows.items() if n == number) for number in right}
    totals_left: dict[str, float] = Counter()
    dominant: dict[str, tuple[int, float]] = {}
    for (course, number), credits in flows.items():
        totals_left[course] += credits
        if credits > dominant.get(course, (0, -1.0))[1]:
            dominant[course] = (number, credits)
    # Old courses grouped under the community they mostly flow into, to limit crossings.
    left = sorted(totals_left, key=lambda course: (dominant[course][0], -dominant[course][1], course))

    gap_left = 0.6
    height = sum(totals_left.values()) + gap_left * (len(left) - 1)
    gap_right = (height - sum(totals_right.values())) / max(1, len(right) - 1)

    def stack(order, totals, gap):
        intervals, top = {}, height
        for key in order:
            intervals[key] = (top - totals[key], top)
            top -= totals[key] + gap
        return intervals

    left_box = stack(left, totals_left, gap_left)
    right_box = stack(right, totals_right, gap_right)

    palette = [plt.get_cmap(name)(index) for name in ("tab20", "tab20b", "tab20c") for index in range(20)]
    colour = {number: palette[(number - 1) % len(palette)] for number in right}

    # About 0.05 inches per credit keeps a 1-credit course label legible; PDF pages max out at 200 inches.
    fig_height = min(199, max(10, height * 0.05 + 2))
    fig, ax = plt.subplots(figsize=(22, fig_height))
    ax.set_xlim(0, 1)
    ax.set_ylim(-1, height + 1)
    ax.axis("off")
    x_left, x_right, bar = 0.3, 0.62, 0.008

    left_cursor = {course: left_box[course][1] for course in left}
    right_cursor = {number: right_box[number][1] for number in right}
    for number in right:
        for course in left:
            credits = flows.get((course, number))
            if not credits:
                continue
            band_left = (left_cursor[course] - credits, left_cursor[course])
            band_right = (right_cursor[number] - credits, right_cursor[number])
            left_cursor[course] -= credits
            right_cursor[number] -= credits
            ax.add_patch(PathPatch(
                _ribbon(x_left + bar, band_left, x_right, band_right),
                facecolor=colour[number], edgecolor="none", alpha=0.55,
            ))

    font = 5
    for course in left:
        y0, y1 = left_box[course]
        # Per-topic shares are rounded in the YAML, so their sum is only close to the real credits.
        credits = round(sum(course_topics[course].values()), 1)
        ax.add_patch(Rectangle((x_left, y0), bar, y1 - y0, facecolor="dimgrey", edgecolor="none"))
        ax.text(x_left - 0.004, (y0 + y1) / 2, f"{course_label(course, names)} ({credits:g} CFU)",
                ha="right", va="center", fontsize=font)
    for number in right:
        y0, y1 = right_box[number]
        topics = communities[number - 1]
        preview = ", ".join(topics[:3]) + (", ..." if len(topics) > 3 else "")
        ax.add_patch(Rectangle((x_right, y0), bar, y1 - y0, facecolor=colour[number], edgecolor="none"))
        ax.text(x_right + bar + 0.004, (y0 + y1) / 2,
                f"{number}: {preview} ({len(topics)} topics, est. {estimated_ects(topics, ects):.1f} ECTS; "
                f"{totals_right[number]:.1f} current CFU)",
                ha="left", va="center", fontsize=font + 1)

    ax.set_title(
        f"Old courses (left) to communities as new courses (right), {method}\n"
        "flow width = credits of the old course spent on topics of the community; "
        "est. ECTS = sum of the community's topic ECTS (median per-course share)",
        fontsize=12,
    )
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def plot_communities(graph: nx.Graph, communities: list[list[str]], method: str, path: pathlib.Path) -> None:
    plt = _load_pyplot()
    positions = layout(graph)
    # 60 qualitative colours; with more communities colours repeat, so each
    # community is also labelled with its number at its centroid.
    palette = [plt.get_cmap(name)(index) for name in ("tab20", "tab20b", "tab20c") for index in range(20)]
    colours = {
        topic: palette[(number - 1) % len(palette)]
        for number, topics in enumerate(communities, start=1)
        for topic in topics
    }
    degrees = dict(graph.degree())

    fig, ax = plt.subplots(figsize=(24, 24))
    nx.draw_networkx_edges(graph, positions, ax=ax, width=0.15, alpha=0.1, edge_color="grey")
    nx.draw_networkx_nodes(
        graph,
        positions,
        ax=ax,
        node_size=[6 + 1.2 * degrees[node] for node in graph],
        node_color=[colours[node] for node in graph],
        linewidths=0,
    )
    for number, topics in enumerate(communities, start=1):
        if len(topics) < 2:
            continue
        x = statistics.mean(positions[topic][0] for topic in topics)
        y = statistics.mean(positions[topic][1] for topic in topics)
        ax.text(
            x, y, str(number), fontsize=9, fontweight="bold", ha="center", va="center",
            bbox={"boxstyle": "round,pad=0.15", "facecolor": "white", "edgecolor": colours[topics[0]], "alpha": 0.8},
        )
    ax.set_title(
        f"Topic co-occurrence graph coloured by community ({method}): "
        f"{len(communities)} communities; numbers match the community table"
    )
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def filter_by_weight(graph: nx.Graph, min_weight: int) -> nx.Graph:
    """Keep edges shared by at least ``min_weight`` courses, then drop isolated topics."""
    filtered = nx.Graph()
    filtered.add_edges_from(
        (left, right, data) for left, right, data in graph.edges(data=True) if data["weight"] >= min_weight
    )
    for node in filtered:
        filtered.nodes[node].update(graph.nodes[node])
    return filtered


def packed_layout(graph: nx.Graph) -> tuple[dict[str, tuple[float, float]], float, float]:
    """Lay out each component on its own and pack them in rows, largest first.

    A component of n topics gets a square of side ~2*sqrt(n), so every topic has
    about the same room for its label regardless of the component it is in.
    Returns the positions and the width/height of the packed drawing.
    """
    components = sorted(nx.connected_components(graph), key=lambda c: (-len(c), min(c)))
    boxes = []
    for component in components:
        sub = graph.subgraph(component)
        radius = max(1.0, len(sub) ** 0.5)
        if len(sub) <= 2:
            local = nx.spring_layout(sub, seed=0, scale=radius)
        else:
            # Unweighted Kamada-Kawai spreads dense cores better than spring layouts.
            local = nx.kamada_kawai_layout(sub, weight=None, scale=radius)
        boxes.append((local, radius))

    gap = 1.5
    row_limit = max(2 * boxes[0][1], sum((2 * r + gap) ** 2 for _, r in boxes) ** 0.5) if boxes else 0
    positions: dict[str, tuple[float, float]] = {}
    x = y = row_height = width = 0.0
    for local, radius in boxes:
        side = 2 * radius + gap
        if x > 0 and x + side > row_limit:
            y -= row_height
            x = row_height = 0.0
        for node, (px, py) in local.items():
            positions[node] = (x + side / 2 + px, y - side / 2 + py)
        x += side
        width = max(width, x)
        row_height = max(row_height, side)
    return positions, width, -(y - row_height)


def plot_filtered_graph(graph: nx.Graph, min_weight: int, path: pathlib.Path) -> None:
    plt = _load_pyplot()
    nodes = len(graph)
    components = nx.number_connected_components(graph)
    print(
        f"min weight {min_weight}: {nodes} topics, {graph.number_of_edges()} edges, "
        f"{components} connected components"
    )
    positions, width, height = packed_layout(graph)
    weights = [weight for _, _, weight in graph.edges(data="weight")]
    max_weight = max(weights, default=1)
    degrees = dict(graph.degree())

    # About 0.55 inches per layout unit leaves room for a 5pt label per topic.
    inches = 0.55
    fig, ax = plt.subplots(figsize=(max(10, width * inches + 3), max(8, height * inches + 1)))
    nx.draw_networkx_edges(
        graph,
        positions,
        ax=ax,
        width=[0.3 + 3 * weight / max_weight for weight in weights],
        alpha=0.35,
        edge_color="grey",
    )
    drawn = nx.draw_networkx_nodes(
        graph,
        positions,
        ax=ax,
        node_size=[15 + 4 * degrees[node] for node in graph],
        node_color=[degrees[node] for node in graph],
        cmap="viridis",
        linewidths=0,
    )
    nx.draw_networkx_labels(graph, positions, font_size=5, ax=ax)
    fig.colorbar(drawn, ax=ax, label="Degree (after filtering)", shrink=0.4)
    ax.set_title(
        f"Topic co-occurrence graph, edges shared by >= {min_weight} courses\n"
        f"{nodes} topics, {graph.number_of_edges()} edges, {components} components "
        "(edge width = shared courses)"
    )
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def plot_degree_distribution(graph: nx.Graph, path: pathlib.Path) -> None:
    plt = _load_pyplot()
    degrees = [degree for _, degree in graph.degree()]
    counts = Counter(degrees)
    xs = sorted(counts)

    fig, (hist_ax, log_ax) = plt.subplots(1, 2, figsize=(12, 5))
    hist_ax.hist(degrees, bins=range(0, max(degrees, default=0) + 2), color="tab:blue")
    hist_ax.set_xlabel("Degree")
    hist_ax.set_ylabel("Number of topics")
    hist_ax.set_title("Degree histogram")

    positive = [x for x in xs if x > 0]
    log_ax.loglog(positive, [counts[x] / len(degrees) for x in positive], "o", markersize=4, color="tab:blue")
    log_ax.set_xlabel("Degree k")
    log_ax.set_ylabel("P(k)")
    log_ax.set_title("Degree distribution (log-log, isolated topics omitted)")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def plot_edge_weight_distribution(graph: nx.Graph, path: pathlib.Path) -> None:
    plt = _load_pyplot()
    weights = [weight for _, _, weight in graph.edges(data="weight")]

    fig, ax = plt.subplots(figsize=(7, 5))
    # One bar per integer weight; log scale because weight-1 edges dominate.
    ax.hist(weights, bins=[w - 0.5 for w in range(1, max(weights, default=1) + 2)], color="tab:blue", log=True)
    ax.set_xlabel("Edge weight (courses in which both topics occur)")
    ax.set_ylabel("Number of edges (log scale)")
    ax.set_title("Edge weight distribution")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("topics_global", type=pathlib.Path, help="Path to an attempt's topics-global.yml")
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        help="Where to write the PDFs and GraphML (default: network_analysis/ next to the input file)",
    )
    parser.add_argument(
        "--min-weights",
        type=int,
        nargs="*",
        default=[2, 5, 10],
        help="Also plot the graph keeping only edges with at least this weight, one PDF per value "
        "(default: 2 5 10; pass no values to skip)",
    )
    communities_group = parser.add_mutually_exclusive_group()
    communities_group.add_argument(
        "--max-ects",
        type=float,
        default=6.0,
        help="Community detection: weighted Louvain, then split only communities whose estimated ECTS "
        "exceed this value (default: 6)",
    )
    communities_group.add_argument(
        "--resolution",
        type=float,
        help="Community detection: plain weighted Louvain at this resolution instead of --max-ects; "
        "higher gives smaller communities",
    )
    args = parser.parse_args()
    if not args.topics_global.is_file():
        parser.error(f"Input file not found: {args.topics_global}")

    output_dir = args.output_dir or args.topics_global.parent / "network_analysis"
    output_dir.mkdir(parents=True, exist_ok=True)

    descriptions, course_topics = load_course_topics(args.topics_global)
    graph = build_graph(descriptions, course_topics)
    print_statistics(graph, course_topics)
    print()
    ects = topic_ects(course_topics)
    if args.resolution is not None:
        found = louvain_communities(graph, args.resolution)
        method, suffix = f"Louvain resolution {args.resolution:g}", f"resolution-{args.resolution:g}"
    else:
        found = ects_capped_communities(graph, ects, args.max_ects)
        method, suffix = f"Louvain split to <= {args.max_ects:g} est. ECTS", f"max-ects-{args.max_ects:g}"
    communities = order_communities(graph, found)
    print_community_statistics(graph, communities, ects, method)

    graph_pdf = output_dir / "topic-cooccurrence-graph.pdf"
    degree_pdf = output_dir / "topic-cooccurrence-degree-distribution.pdf"
    weight_pdf = output_dir / "topic-cooccurrence-edge-weight-distribution.pdf"
    communities_pdf = output_dir / f"topic-cooccurrence-communities-{suffix}.pdf"
    communities_csv = output_dir / f"topic-communities-{suffix}.csv"
    communities_md = output_dir / f"topic-communities-{suffix}.md"
    sankey_pdf = output_dir / f"topic-communities-sankey-{suffix}.pdf"
    graphml = output_dir / "topic-cooccurrence.graphml"
    plot_graph(graph, graph_pdf)
    plot_degree_distribution(graph, degree_pdf)
    plot_edge_weight_distribution(graph, weight_pdf)
    plot_communities(graph, communities, method, communities_pdf)
    names = load_course_names(args.topics_global.parent)
    links = community_course_links(communities, course_topics)
    write_community_table(communities, links, names, ects, communities_csv, communities_md)
    plot_community_sankey(communities, links, names, course_topics, ects, method, sankey_pdf)
    nx.write_graphml(graph, graphml)
    outputs = (graph_pdf, degree_pdf, weight_pdf, communities_pdf, communities_csv, communities_md, sankey_pdf, graphml)
    for path in outputs:
        print(f"Wrote {path}")

    if args.min_weights:
        print()
    for min_weight in args.min_weights:
        filtered_pdf = output_dir / f"topic-cooccurrence-graph-min-weight-{min_weight}.pdf"
        plot_filtered_graph(filter_by_weight(graph, min_weight), min_weight, filtered_pdf)
        print(f"Wrote {filtered_pdf}")


if __name__ == "__main__":
    main()
