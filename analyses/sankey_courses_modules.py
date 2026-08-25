import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.path import Path
from matplotlib.patches import PathPatch, Rectangle
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.lib.pagesizes import landscape, A3

# NaN = no connection; 0 = a real connection shown with a minimal visible width.
data = {
    'Python 1': [3,3,3,3,3,3,3,3,3],
    'Python 2': [np.nan,np.nan,np.nan,2,np.nan,2,np.nan,np.nan,2],
    'Algorithms': [np.nan,np.nan,np.nan,np.nan,np.nan,2,2,2,2],
    'Statistics intro': [np.nan,np.nan,np.nan,np.nan,2,2,np.nan,np.nan,np.nan],
    'CS basics': [np.nan,np.nan,np.nan,1,1,1,1,1,1],
    'Case studies': [np.nan,np.nan,np.nan,0,0,0,np.nan,np.nan,np.nan],
    'Machine Learning Intro': [2,2,np.nan,np.nan,np.nan,2,np.nan,np.nan,np.nan],
    'Deep Learning': [5,5,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan],
    'Bioinformatics': [np.nan,np.nan,3,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan],
}
courses = [
    'Systems and Algorithms for Data Science',
    'Machine Learning Systems For Data Science',
    'Computational Methods for Bioinformatics',
    'Computer Programming',
    "INTRODUZIONE ALL'ANALISI DEI DATI",
    'Programming',
    'INFORMATICA',
    'Information Technology',
    'Computer Science',
]
df = pd.DataFrame(data, index=courses)


def ribbon_path(x0, y0a, y0b, x1, y1a, y1b):
    """Closed cubic-Bezier ribbon from vertical interval at x0 to interval at x1."""
    dx = x1 - x0
    verts = [
        (x0, y0a),
        (x0 + 0.42*dx, y0a), (x1 - 0.42*dx, y1a), (x1, y1a),
        (x1, y1b),
        (x1 - 0.42*dx, y1b), (x0 + 0.42*dx, y0b), (x0, y0b),
        (x0, y0a),
    ]
    codes = [Path.MOVETO,
             Path.CURVE4, Path.CURVE4, Path.CURVE4,
             Path.LINETO,
             Path.CURVE4, Path.CURVE4, Path.CURVE4,
             Path.CLOSEPOLY]
    return Path(verts, codes)


def sankey_courses_modules(df, pdf_filename='/mnt/data/sankey_courses_modules_v6.pdf',
                           png_filename='/mnt/data/sankey_courses_modules_v6.png'):
    courses = list(df.index)
    modules = list(df.columns)

    # Every module has one intrinsic width: its non-missing CFU value.
    # This dataset has a constant value within each module. 0 gets a tiny visible width.
    zero_width = 0.16
    module_value = {}
    for m in modules:
        vals = df[m].dropna().unique()
        positive = [float(v) for v in vals if float(v) > 0]
        module_value[m] = positive[0] if positive else 0.0

    def visual_value(v):
        return zero_width if float(v) == 0 else float(v)

    # Course node width = additive sum of all connected module widths.
    course_total = {
        c: sum(visual_value(df.loc[c, m]) for m in modules if not pd.isna(df.loc[c, m]))
        for c in courses
    }

    # Geometry in data coordinates. Heights are proportional to CFU.
    scale = 0.0062
    gap_course = 0.030
    gap_module = 0.045
    xL, xR = 0.23, 0.77
    node_w = 0.014

    # Center the two node columns independently.
    left_heights = [course_total[c] * scale for c in courses]
    right_heights = [visual_value(module_value[m]) * scale for m in modules]

    def positions(heights, gap):
        total = sum(heights) + gap * (len(heights)-1)
        top = 0.91 + total/2
        # rescale/shift if necessary
        if top > 0.95:
            top = 0.95
        out = []
        y = top
        for h in heights:
            out.append((y-h, y))
            y -= h + gap
        return out

    left_pos = positions(left_heights, gap_course)
    right_pos = positions(right_heights, gap_module)

    # Distinct module colors from a qualitative palette.
    cmap = plt.get_cmap('tab10')
    colors = {m: cmap(i % 10) for i, m in enumerate(modules)}

    fig, ax = plt.subplots(figsize=(13, 7.5))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')

    # Draw course nodes first. Their heights equal the SUM of connected module widths.
    for i, c in enumerate(courses):
        y0, y1 = left_pos[i]
        ax.add_patch(Rectangle((xL-node_w, y0), node_w, y1-y0,
                               facecolor='white', edgecolor='black', linewidth=0.9, zorder=4))
        semantic_course_cfu = sum(float(df.loc[c, m]) for m in modules if not pd.isna(df.loc[c, m]))
        ax.text(xL-node_w/2, (y0+y1)/2, f'{semantic_course_cfu:g}',
                ha='center', va='center', fontsize=7.5, fontweight='bold', zorder=5)
        ax.text(xL-node_w-0.012, (y0+y1)/2, c, ha='right', va='center', fontsize=9)

    # Module nodes: node height equals the width of every flow belonging to that module.
    for j, m in enumerate(modules):
        y0, y1 = right_pos[j]
        ax.add_patch(Rectangle((xR, y0), node_w, y1-y0,
                               facecolor=colors[m], edgecolor='black', linewidth=0.7, zorder=4))
        ax.text(xR+node_w/2, (y0+y1)/2, f'{module_value[m]:g}',
                ha='center', va='center', fontsize=7.5, fontweight='bold', zorder=5)
        ax.text(xR+node_w+0.012, (y0+y1)/2, m, ha='left', va='center', fontsize=9)

    # Stack ribbons at each COURSE, so they are additive rather than overlapping.
    # At each MODULE, all ribbons use the same full module interval, because each
    # connection for that module has the same CFU width in this dataset.
    course_cursor = {c: left_pos[i][0] for i, c in enumerate(courses)}

    for j, m in enumerate(modules):
        my0, my1 = right_pos[j]
        for c in courses:
            v = df.loc[c, m]
            if pd.isna(v):
                continue
            h = visual_value(v) * scale
            cy0 = course_cursor[c]
            cy1 = cy0 + h
            course_cursor[c] = cy1

            path = ribbon_path(xL, cy0, cy1, xR, my0, my1)
            ax.add_patch(PathPatch(path,
                                   facecolor=colors[m], edgecolor=colors[m],
                                   linewidth=0.25, alpha=0.62, zorder=2))

    ax.text(xL-node_w, 0.985, 'COURSES', ha='right', va='top', fontsize=12, fontweight='bold')
    ax.text(xR, 0.985, 'MODULES', ha='left', va='top', fontsize=12, fontweight='bold')

    # Totals are computed from the semantic node widths in CFU.
    # The tiny visual width used to display 0-CFU links is deliberately excluded.
    total_course_cfu = sum(
        float(v) for v in df.to_numpy().ravel() if not pd.isna(v)
    )
    total_module_cfu = sum(float(module_value[m]) for m in modules)

    def fmt_cfu(x):
        return str(int(x)) if float(x).is_integer() else f'{x:g}'

    left_bottom = min(y0 for y0, y1 in left_pos)
    right_bottom = min(y0 for y0, y1 in right_pos)
    ax.text(xL-node_w, left_bottom-0.018, f'Total CFU {fmt_cfu(total_course_cfu)}',
            ha='right', va='top', fontsize=11, fontweight='bold')
    ax.text(xR, right_bottom-0.018, f'Total CFU {fmt_cfu(total_module_cfu)}',
            ha='left', va='top', fontsize=11, fontweight='bold')
    ax.text(0.5, 0.018,
            'Module color is carried by its flows. Module/flow width is proportional to CFU; '
            'course width is the additive sum of connected module widths. 0 CFU is a thin ribbon; missing = no ribbon.',
            ha='center', va='bottom', fontsize=8)

    fig.savefig(png_filename, dpi=240, bbox_inches='tight', pad_inches=0.15)
    plt.close(fig)

    # Put the verified chart image on a single landscape A3 PDF page.
    page_w, page_h = landscape(A3)
    c = canvas.Canvas(pdf_filename, pagesize=(page_w, page_h))
    img = ImageReader(png_filename)
    iw, ih = img.getSize()
    margin = 18
    ratio = min((page_w-2*margin)/iw, (page_h-2*margin)/ih)
    w, h = iw*ratio, ih*ratio
    c.drawImage(img, (page_w-w)/2, (page_h-h)/2, width=w, height=h, mask='auto')
    c.showPage()
    c.save()


if __name__ == '__main__':
    sankey_courses_modules(df)
