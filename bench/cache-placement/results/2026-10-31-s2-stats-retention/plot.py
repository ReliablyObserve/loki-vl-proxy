"""Render the measured retained-memory comparison; requires matplotlib."""
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

root = Path(__file__).parent
heap = json.loads((root/'heap-summary.json').read_text())
fields = [('heap-B/entry', 'Post-GC retained heap'),
          ('retained-cap-B/entry', 'Backing slice capacity'),
          ('charged-B/entry', 'Logical RAM charge')]
fig, ax = plt.subplots(figsize=(10, 5.5))
for source, offset, color in [('main', -.18, '#64748b'), ('pr', .18, '#0f766e')]:
    values = [heap[source]['ram']['median'][key]*256/1024**2 for key, _ in fields]
    bars = ax.bar([x+offset for x in range(3)], values, .34, label=source, color=color)
    ax.bar_label(bars, labels=[f'{v:.2f} MiB' for v in values], padding=4, fontsize=11)
ax.set_xticks(range(3), [label for _, label in fields])
ax.set_ylim(0, 52)
ax.set_ylabel('MiB for 256 independent extents')
ax.set_title('S2 stats cache ownership: same bytes, less retained memory', fontsize=14, pad=18)
ax.legend(frameon=False)
ax.spines[['top', 'right']].set_visible(False)
ax.grid(axis='y', alpha=.15)
ax.set_axisbelow(True)
fig.text(.08, .02, 'Median of three fresh-process trials. 10,000 points per extent; encoded size 50,291 B.\n'
         'Heap includes cache overhead and allocator rounding; it is not RSS or a hard process bound.', fontsize=10)
fig.tight_layout(rect=[0, .12, 1, 1])
fig.savefig(root/'memory-proof.png', dpi=160, bbox_inches='tight')
fig.savefig(root/'memory-proof.svg', bbox_inches='tight')
