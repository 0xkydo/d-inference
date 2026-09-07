"""Illustrative screen copy and event transitions, independent of terminal IO."""

from dataclasses import dataclass
from extra_scenes import extra_scenes


@dataclass(frozen=True)
class Scene:
    title: str
    lines: tuple[str, ...]
    action: str = ""
    next: str = ""
    events: tuple[tuple[str, str], ...] = ()
    box: bool = False
    progress: bool = False
    ready: bool = False


SCENES = {
    "install": Scene("Install Darkbloom", (
        "Serve private AI requests from your Mac.",
        "Apple M2 Pro · 32 GB memory · macOS 15.6",
        "Download the CLI and verify its signature.",
    ), "Press Enter to begin.", "download", (
        ("Release service unavailable", "offline"),
    )),
    "download": Scene("Downloading Darkbloom", (
        "Version 0.8.10 · Downloading the CLI",
    ), "", "enroll", (("Integrity check fails", "integrity"),
                        ("Not enough disk space", "install-disk-full")), progress=True),
    "enroll": Scene("CLI installation complete", (
        "1 of 3 · Enroll this Mac",
    ), "Press Enter to open System Settings.", "settings", (
        ("Darkbloom profile already installed", "enrolled"),
        ("Another organization's MDM detected", "other-mdm"),
        ("Enrollment service unreachable", "enrollment-offline"),
        ("Enrollment status cannot be read", "enrollment-unknown"),
    ), box=True),
    "settings": Scene("Finish enrollment in System Settings", (
        "Open General > Device Management.",
        "If it is not there, search Settings for 'Profiles'.",
        "Open the Darkbloom profile and review its details.",
        "Click Install and complete the confirmation prompts.",
        "",
        "Waiting for the Darkbloom profile to be installed…",
        "Keep this terminal open. Setup will continue automatically.",
    ), events=(("Profile installed", "enrolled"),
               ("Profile still missing", "enrollment-pending"),
               ("Close setup, then rerun the curl command", "resume"))),
    "enrollment-pending": Scene("Waiting for device enrollment", (
        "The Darkbloom profile has not been detected yet.",
        "In System Settings, check that you finished installing it.",
        "You can exit and rerun the install command to continue later.",
    ), "Press Enter to reopen System Settings.", "settings", (
        ("Profile installed", "enrolled"),
    )),
    "resume": Scene("Welcome back", (
        "CLI already installed. Continuing your setup.",
        "Device enrollment is the next step.",
    ), "Press Enter to continue.", "enroll"),
    "enrolled": Scene("Darkbloom profile installed", (
        "Your Mac is enrolled. Network verification happens after startup.",
    ), "", "login"),
    "login": Scene("2 of 3 · Link your account", (
        "Connect this Mac to your account so its earnings are credited to you.",
    ), "Press Enter to continue in your browser.", "authorize", (
        ("Account already linked", "linked"),
        ("Login service unavailable", "login-offline"),
    )),
    "authorize": Scene("Approve this Mac in your browser", (
        "Waiting for account approval…",
    ), events=(("Approve account linking", "linked"),
               ("Code expires", "expired"),
               ("Close browser without approval", "login-pending"))),
    "expired": Scene("This login code has expired", (
        "Your Mac is not linked yet. Get a new code to continue.",
    ), "Press Enter to get a new code.", "authorize"),
    "login-pending": Scene("Account linking is still pending", (
        "Approve the code in your browser to link this Mac.",
    ), "Press Enter to reopen the browser.", "authorize"),
    "linked": Scene("Account linked", (
        "Next, choose what this Mac will serve.",
    ), "", "models"),
    "models": Scene("3 of 3 · Choose models", (
        "Select the models you want this Mac to serve.",
        "These are illustrative model choices, not live catalog entries.",
    ), events=(
        ("No compatible models available", "no-models"),
    )),
    "model-download": Scene("Downloading selected models", (
        "You can interrupt this download and resume later.",
    ), "", "start", (("Connection lost at 40%", "download-paused"),
                       ("Not enough disk space", "disk-full")), progress=True),
    "download-paused": Scene("Download paused · 40% saved", (
        "The connection was interrupted. Downloaded data is retained.",
    ), "Press Enter to retry from 40%.", "model-resume"),
    "model-resume": Scene("Resuming selected model downloads", (
        "Continuing from 40%.",
    ), "", "start", (("Connection lost again", "download-paused"),), progress=True),
    "start": Scene("Ready to start Darkbloom", (
        "Your selected models are available on this Mac.",
        "Start sharing this Mac's compute with the Darkbloom network.",
        "Darkbloom runs in the background. You can stop it at any time.",
    ), "Press Enter to start Darkbloom.", "starting", (
        ("Keep the provider stopped", "stopped"),
    )),
    "verify": Scene("Connected · verifying this Mac", (
        "Waiting for the network to confirm your device's security state…",
        "Your Mac is not receiving requests yet.",
    ), events=(("Network verification succeeds", "ready"),
               ("Verification takes longer", "trust-pending"),
               ("Network rejects device verification", "trust-failed"))),
    "trust-pending": Scene("Device verification is still pending", (
        "Darkbloom is running and waiting for network verification.",
        "Your Mac is not receiving requests yet.",
        "You can close this terminal. Check progress with darkbloom status.",
    ), events=(("Network verification succeeds", "ready"),
               ("Network rejects device verification", "trust-failed"))),
    "ready": Scene("Ready to receive requests", (
        "Darkbloom is running in the background.",
        "You can close this terminal.",
        "",
        "Check status   darkbloom status",
        "Stop serving   darkbloom stop",
    ), ready=True),
    "update": Scene("Darkbloom is already set up", (
        "Installed v0.8.9 · Available v0.8.10",
        "Your account, enrollment, and model selections will be kept.",
        "This provider is currently stopped.",
    ), "Press Enter to update.", "updating"),
    "updating": Scene("Updating Darkbloom", (
        "Downloading and verifying v0.8.10.",
    ), "", "updated", (("Update download fails", "update-failed"),), progress=True),
    "updated": Scene("Update complete · v0.8.10", (
        "Your provider is still stopped.",
        "When you want to serve requests, run darkbloom start.",
    )),
    "stopped": Scene("Setup saved", (
        "Your provider is stopped.",
        "When you want to serve requests, run darkbloom start.",
    )),
    "offline": Scene("Could not fetch the latest release", (
        "Check your internet connection and retry.",
        "Nothing was installed.",
    ), "Press Enter to retry.", "install"),
    "integrity": Scene("Download could not be verified", (
        "Installation stopped. The downloaded copy will not be installed.",
    ), "Press Enter to download a fresh copy.", "download"),
    "other-mdm": Scene("This Mac is enrolled with another organization", (
        "That enrollment does not verify this Mac for Darkbloom.",
        "If this is a managed Mac, contact your administrator before continuing.",
        "Your existing enrollment has not been changed.",
    )),
    "no-models": Scene("No compatible models are available", (
        "Your account and enrollment are saved.",
        "Your Mac has not started serving requests.",
    ), "Press Enter to check the catalog again.", "models"),
}

SCENES.update(extra_scenes(Scene))

SCENARIOS = (
    ("First installation · full journey", "install"),
    ("Device enrollment · read-only explanation", "enroll"),
    ("Resume an unfinished installation", "resume"),
    ("Darkbloom profile already installed", "enrolled"),
    ("Another organization's MDM", "other-mdm"),
    ("Account linking · expired code", "expired"),
    ("Model download · connection interrupted", "download-paused"),
    ("Network verification takes longer", "trust-pending"),
    ("Update an existing, stopped provider", "update"),
    ("Release service unavailable", "offline"),
    ("Download integrity check fails", "integrity"),
    ("Update an existing, running provider", "update-running"),
    ("Already on the latest version", "up-to-date"),
    ("Unattended installation", "unattended"),
    ("Resume at account linking", "resume-login"),
    ("Provider fails to start", "start-failed"),
    ("Device verification rejected", "trust-failed"),
    ("Enrollment service unavailable", "enrollment-offline"),
    ("Not enough space for model downloads", "disk-full"),
    ("Unsupported Mac", "unsupported"),
)
