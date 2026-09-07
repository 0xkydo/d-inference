package main

func errorText(code string) string {
	switch code {
	case "version_mismatch":
		return "Onboarding versions differ. Install the complete bundle."
	case "busy":
		return "Another setup or model operation is active. Close it, then press r to retry."
	case "invalid_command", "stale_command", "wrong_phase":
		return "This action is no longer available. Press r to refresh."
	case "enrollment_failed":
		return "Enrollment could not finish. Check your connection and Device Management; press r to retry."
	case "enrollment_unknown":
		return "macOS could not report enrollment. Check Device Management, then press r."
	case "other_mdm":
		return "This Mac is managed by another MDM. Darkbloom enrollment is unavailable."
	case "link_failed":
		return "Account linkage failed or expired. Press Enter to request a new code."
	case "catalog_failed":
		return "Could not read the catalog or prepare this Mac's runtime. Run darkbloom doctor, then retry."
	case "invalid_selection":
		return "Choose at least one model that fits. Additional models are for download only."
	case "download_failed":
		return "Download interrupted. Completed and partial files are kept. Press Enter to retry your selection."
	case "prerequisites_changed":
		return "Enrollment, account, or models changed. Press r to recheck before starting."
	case "start_failed":
		return "Could not start the provider. Check darkbloom status and darkbloom doctor; press r to retry setup."
	default:
		return "Onboarding is unavailable. Quit, run darkbloom doctor, and reopen with darkbloom start --tui."
	}
}
