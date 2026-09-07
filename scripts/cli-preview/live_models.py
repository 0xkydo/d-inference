#!/usr/bin/env python3
"""Live-data model selection preview. Model downloads remain simulated."""

import argparse
import os
import select
import shutil
import subprocess
import sys
import termios
import textwrap
import tty

from install import Terminal
from live_catalog import clean, load


def groups(models):
    return (
        ('Downloaded', [i for i, m in enumerate(models) if m['downloaded']]),
        ('Available to download', [i for i, m in enumerate(models) if not m['downloaded'] and not m['reason']]),
        ('Additional models', [i for i, m in enumerate(models) if not m['downloaded'] and m['reason']]),
    )


def screen(models, info, chip, chosen, expanded, cursor, note=''):
    lines = [('Choose models to download', '1'),
             (f"{chip} · {info['physicalBytes'] / 2**30:.0f} GiB RAM", '2'),
             ('', None)]
    if models:
        lines.insert(2, (f"{models[0]['estimate']['availableGiB']:.1f} GiB model budget after the system memory reserve", '2'))
    sections = groups(models)
    if not sections[1][1]:
        lines.extend([('No additional models fit this machine’s estimated capacity.', None), ('', None)])
    for section, indices in sections:
        if not indices or (section == 'Additional models' and not expanded):
            continue
        lines.append((section, '1'))
        if section == 'Additional models':
            lines.append(('These models will most likely not fit on this machine, '
                          'or require different hardware. You can still download them.', None))
        for i in indices:
            m = models[i]
            mark = 'x' if i in chosen else ' '
            arrow = '›' if cursor == i else ' '
            lines.append((f"{arrow} [{mark}] {i + 1}. {m['label']} · {m['total_size_bytes'] / 1e9:.1f} GB",
                          '36' if cursor == i else None))
            if m['reason']:
                lines.append(('    ' + m['reason'], '2'))
        lines.append(('', None))
    hidden = sections[2][1]
    if hidden:
        count = len(set(hidden) & chosen)
        suffix = f' · {count} selected' if count else ''
        lines.append((f"{'▾ Hide' if expanded else '▸ Show'} {len(hidden)} additional {'model' if len(hidden) == 1 else 'models'}{suffix} · h", '36'))
    gb = sum(models[i]['total_size_bytes'] for i in chosen if not models[i]['downloaded']) / 1e9
    lines.extend([('', None), (f'{len(chosen)} selected · {gb:.1f} GB to download', '1'),
                  ('Fit is based on total RAM, with reserves for macOS, working memory and minimum KV cache, plus padded weights. '
                   'They are individual estimates, not a promise that all selections can run together.', '2'),
                  ('↑↓ move · Space toggle · h show/hide · Enter confirm · q quit', None),
                  ('LIVE DATA PREVIEW · downloads are simulated', '2'), (note, None)])
    return lines


def choose(term, models, info, chip, expanded=False):
    chosen = set()
    cursor = None
    interactive = sys.stdin.isatty() and term.tty and os.environ.get('TERM') != 'dumb'
    original = termios.tcgetattr(sys.stdin.fileno()) if interactive else None
    previous, previous_size, note = 0, None, ''
    try:
        if interactive:
            tty.setcbreak(sys.stdin.fileno())
        while True:
            sections = groups(models)
            visible = sections[0][1] + sections[1][1] + (sections[2][1] if expanded else [])
            if cursor not in visible:
                cursor = visible[0] if visible else None
            size = shutil.get_terminal_size()
            rows = [(row, code) for value, code in screen(models, info, chip, chosen, expanded, cursor, note)
                    for row in (textwrap.wrap(value, width=max(12, size.columns - 4)) or [''])]
            if interactive and previous and previous_size == size and previous < size.lines:
                print(f'\033[{previous}A\r\033[J', end='')
            for row, code in rows:
                term.line('  ' + row, code)
            previous, previous_size = len(rows), size
            if interactive:
                key = os.read(sys.stdin.fileno(), 1).decode(errors='replace') or 'q'
                if key == '\x1b':
                    while len(key) < 3 and select.select([sys.stdin], [], [], .08)[0]:
                        key += os.read(sys.stdin.fileno(), 1).decode(errors='replace')
            else:
                term.line('  Enter a row number to toggle, h to expand, or Enter to confirm.')
                value = sys.stdin.readline()
                key = value.strip() if value else 'q'
            note = ''
            if key in ('q', '\x04', '\x1b'):
                return []
            if key == 'h':
                expanded = not expanded
            elif key in ('\x1b[A', '\x1b[B', '\x1bOA', '\x1bOB') and visible:
                pos = visible.index(cursor)
                cursor = visible[max(0, min(len(visible)-1, pos + (-1 if key.endswith('A') else 1)))]
            elif key == ' ' and cursor is not None:
                chosen.symmetric_difference_update({cursor})
            elif key.isdigit() and int(key)-1 in visible:
                chosen.symmetric_difference_update({int(key)-1})
            elif key in ('', '\r', '\n'):
                if chosen:
                    return [models[i] for i in sorted(chosen)]
                note = 'Select at least one model. Use h to reveal additional models.'
    finally:
        if original is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, original)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog-file', help='Use an explicitly saved catalog instead of fetching live data')
    parser.add_argument('--show-hidden', action='store_true')
    parser.add_argument('--list', action='store_true', help='Print the groups without prompting')
    parser.add_argument('--instant', action='store_true')
    args = parser.parse_args()
    term = Terminal(args.instant)
    term.line('  Reading catalog, local downloads, and memory policy…', '2')
    if args.catalog_file:
        term.line('  Using a saved catalog; chip and total RAM are read from this Mac.', '2')
    models, info, chip = load(args.catalog_file)
    if not models:
        term.line('  No public models are available in the catalog.')
        return
    if args.list:
        for line, code in screen(models, info, chip, set(), args.show_hidden, None):
            for wrapped in textwrap.wrap(line, max(12, shutil.get_terminal_size().columns - 4)) or ['']:
                term.line('  ' + wrapped, code)
        return
    selected = choose(term, models, info, chip, args.show_hidden)
    if not selected:
        term.line('  Preview closed. Nothing was downloaded.')
        return
    term.line('\n  Download preview', '1')
    for model in selected:
        term.line('  ' + model['label'])
        if model['downloaded']:
            term.line('    Already downloaded.')
        else:
            if model['reason']:
                term.line('    Download only: ' + model['reason'])
            term.progress()
    term.line('\n  Preview complete. No model files were downloaded or loaded.', '2')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n  Preview closed. Nothing was downloaded.')
        sys.exit(130)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print('  Could not prepare live preview: ' + clean(error), file=sys.stderr)
        sys.exit(1)
