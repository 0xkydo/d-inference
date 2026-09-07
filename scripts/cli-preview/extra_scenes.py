"""Recovery and lifecycle screens for the onboarding design study."""


def extra_scenes(Scene):
    return {
        "starting": Scene("Starting Darkbloom…", (
            "Loading your selected models and connecting to the network.",
        ), events=(("Provider connects", "verify"),
                   ("Provider fails to start", "start-failed"),
                   ("Network unavailable", "connect-failed"))),
        "start-failed": Scene("Darkbloom could not start", (
            "Your enrollment, account, and model downloads are retained.",
            "Run darkbloom doctor to investigate before retrying.",
        ), "Press Enter to retry startup.", "starting"),
        "connect-failed": Scene("Could not connect to the network", (
            "Your Mac is not receiving requests. Check your internet connection.",
        ), "Press Enter to retry the connection.", "starting"),
        "trust-failed": Scene("Device verification did not pass", (
            "The network could not confirm the required security settings.",
            "Your Mac is not receiving requests.",
            "Run darkbloom doctor for the reason and recovery instructions.",
        ), events=(("Issue resolved; verification retried", "verify"),)),
        "enrollment-offline": Scene("Could not download the enrollment profile", (
            "The CLI is installed. Check your connection to continue enrollment.",
        ), "Press Enter to retry.", "enroll"),
        "enrollment-unknown": Scene("Could not check device enrollment", (
            "We could not read this Mac's enrollment status.",
            "Check the Darkbloom profile in System Settings > General > Device Management.",
        ), "Press Enter to check again.", "enroll"),
        "login-offline": Scene("Could not connect to account linking", (
            "Your device enrollment is retained. Check your internet connection.",
        ), "Press Enter to retry.", "login"),
        "disk-full": Scene("Not enough space to continue the download", (
            "Downloaded data is retained. Free some disk space and retry.",
        ), "Press Enter to retry.", "model-resume"),
        "install-disk-full": Scene("Not enough space to install the CLI", (
            "Free some disk space, then retry installation.",
        ), "Press Enter to retry.", "download"),
        "resume-login": Scene("Welcome back", (
            "CLI installed. Darkbloom profile installed.",
            "Continue by linking this Mac to your account.",
        ), "Press Enter to continue.", "login"),
        "update-running": Scene("Update Darkbloom", (
            "Installed v0.8.9 · Available v0.8.10",
            "This provider is running. It will restart to apply the update.",
            "Your account, enrollment, and model selections will be kept.",
        ), "Press Enter to update.", "update-staging"),
        "update-staging": Scene("Downloading the update", (
            "Your current provider continues running during the download.",
        ), "", "update-drain", (("Download fails", "running-update-failed"),), progress=True),
        "update-drain": Scene("Finishing active requests", (
            "Waiting for current work to finish before installing the update.",
        ), events=(("Requests finish; install and restart", "update-restart"),)),
        "update-restart": Scene("Restarting Darkbloom · v0.8.10", (
            "Waiting for the provider to reconnect and pass verification.",
        ), events=(("Provider reconnects and readiness is confirmed", "ready"),
                   ("New version fails; recovery restores previous version", "update-rollback"))),
        "update-rollback": Scene("Update did not start successfully", (
            "The previous version was restored.",
            "Waiting for it to reconnect before reporting readiness.",
        ), events=(("Previous version reconnects and readiness is confirmed", "ready"),)),
        "running-update-failed": Scene("Update download failed", (
            "Your current provider is still running. The installed version was not replaced.",
        ), "Press Enter to retry.", "update-staging"),
        "update-failed": Scene("Update download failed", (
            "Your existing installation was not replaced. The provider is still stopped.",
        ), "Press Enter to retry.", "updating"),
        "up-to-date": Scene("Darkbloom is up to date · v0.8.10", (
            "Your provider is stopped. No changes were needed.",
            "When you want to serve requests, run darkbloom start.",
        )),
        "unattended": Scene("CLI installation complete · install-only", (
            "No interactive terminal is available. Guided setup was not started.",
            "On this Mac, open an interactive terminal and rerun the install command",
            "to continue onboarding. No provider was started.",
        )),
        "unsupported": Scene("This Mac cannot run Darkbloom", (
            "Darkbloom requires Apple Silicon and a supported macOS version.",
            "Nothing was installed.",
        )),
    }
