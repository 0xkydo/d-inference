#!/usr/bin/env python3
"""Interactive, output-only onboarding mock. No network or system mutations."""

import argparse
import copy
import shutil
import sys
import textwrap

from install import PROFILE_EXPLANATION, Terminal
from scenes import SCENARIOS, SCENES
from model_picker import MODELS, needs_download, pick, summary
from preview_state import PreviewState


def paragraph(term, value, code=None):
    width = max(12, shutil.get_terminal_size().columns - 4)
    for line in textwrap.wrap(value, width=width) or [""]:
        term.line("  " + line, code)


def render(term, key, state):
    scene = SCENES[key]
    term.line()
    paragraph(term, "─" * min(60, max(12, shutil.get_terminal_size().columns - 4)), "2")
    paragraph(term, "Darkbloom", "1;36")
    term.line()
    paragraph(term, scene.title, "1;32" if scene.ready else "1")
    term.line()
    for value in scene.lines:
        paragraph(term, value)
    if key == "authorize":
        paragraph(term, f"Your code: DEMO-{state.login_attempt:04}", "1;36")
        paragraph(term, "If the browser did not open, use the link below.")
        paragraph(term, "https://example.invalid/darkbloom-link (mock URL)", "36")
    if key in ("model-download", "download-paused", "model-resume", "start", "ready"):
        term.line()
        paragraph(term, f"Selected: {summary(state.selected)}", "2")
    if key in ("model-download", "download-paused", "model-resume"):
        pending = tuple(i for i in state.selected if i not in state.downloaded)
        paragraph(term, f"To download: {summary(pending)}", "2")
        paragraph(term, "Progress below is for the combined download.", "2")
    if scene.box:
        term.line()
        term.box("This profile is read-only", PROFILE_EXPLANATION)
    if scene.progress:
        term.progress(start=40 if key == "model-resume" else 0,
                      end=40 if key in ("model-download", "model-resume") else 100)
    if scene.action:
        term.line()
        paragraph(term, scene.action, "36")
    if key == "models":
        return
    term.line()
    paragraph(term, "MOCK CONTROLS · these are not part of the customer UI", "2")
    if scene.next and not scene.action:
        paragraph(term, "Enter: simulate completion and continue", "2")
    for i, (label, _) in enumerate(scene.events, 1):
        paragraph(term, f"e{i}: {label}", "2")
    paragraph(term, "b: back · r: replay screen · m: scenarios · q: quit", "2")


def read(prompt):
    try:
        return input(prompt).strip().lower()
    except EOFError:
        return "q"


def menu(term):
    term.line()
    paragraph(term, "Darkbloom onboarding playground", "1;36")
    paragraph(term, "Simulation only. No installs, browser windows, account changes, "
              "downloads, enrollment, or provider processes.", "2")
    paragraph(term, "Hardware, model choices, versions, codes, and progress are fixtures.", "2")
    term.line()
    for i, (label, _) in enumerate(SCENARIOS, 1):
        paragraph(term, f"{i:2}  {label}")
    term.line()
    while True:
        choice = read("  Scenario number (q to quit): ")
        if choice == "q":
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(SCENARIOS):
            return SCENARIOS[int(choice) - 1][1]
        paragraph(term, "Choose a scenario number from the menu.")


def play(term, initial):
    key = initial
    state = PreviewState.for_scene(initial)
    history = []
    while True:
        render(term, key, state)
        scene = SCENES[key]
        if key == "models":
            action, chosen = pick(term, state.selected, state.downloaded)
            if action in ("q", "m"):
                return action
            if action == "b":
                if history:
                    key, state = history.pop()
                continue
            state.selected = chosen
            history.append((key, copy.deepcopy(state)))
            key = ("model-download" if needs_download(chosen, state.downloaded) else "start") if action == "confirm" else action
            continue
        while True:
            choice = read("  › ")
            destination = None
            if choice in ("q", "m"):
                return choice
            if choice == "r":
                break
            if choice == "b":
                if history:
                    key, state = history.pop()
                    break
                paragraph(term, "Already at the beginning of this scenario.", "2")
                continue
            if choice.startswith("e") and choice[1:].isdigit():
                index = int(choice[1:]) - 1
                if 0 <= index < len(scene.events):
                    destination = scene.events[index][1]
            elif not choice and scene.next:
                destination = scene.next
            if destination:
                history.append((key, copy.deepcopy(state)))
                if key == "expired" and destination == "authorize":
                    state.login_attempt += 1
                if key in ("model-download", "model-resume") and destination == "start":
                    state.downloaded.update(state.selected)
                key = destination
                break
            paragraph(term, "Use the action or mock controls shown above.", "2")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scene", choices=SCENES, help="Jump directly to a screen")
    parser.add_argument("--instant", action="store_true", help="Skip simulated animation delays")
    parser.add_argument("--render-all", action="store_true", help="Print every screen without prompts")
    args = parser.parse_args()
    term = Terminal(args.instant or args.render_all)
    paragraph(term, "PREVIEW ONLY · nothing on this Mac will be changed", "2")
    if args.render_all:
        for key in SCENES:
            state = PreviewState.for_scene(key)
            render(term, key, state)
            if key == "models":
                for i, (name, size) in enumerate(MODELS):
                    paragraph(term, f"[{'x' if i in state.selected else ' '}] {name} · {size} GB · "
                              + ("Downloaded" if i in state.downloaded else "Available to download"))
        return
    initial = args.scene or menu(term)
    while initial:
        if play(term, initial) == "q":
            break
        initial = menu(term)
    paragraph(term, "Preview closed. No system changes were made.", "2")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n  Preview closed. No system changes were made.")
        sys.exit(130)
