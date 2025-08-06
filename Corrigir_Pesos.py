from pathlib import Path
import os

def find_chromium_exe():
    local_appdata = os.getenv("LOCALAPPDATA")
    base_dir = Path(local_appdata) / "ms-playwright"
    
    if not base_dir.exists():
        return None

    for folder in base_dir.glob("chromium-*"):
        candidate = folder / "chrome-win" / "chrome.exe"
        if candidate.exists():
            return candidate.resolve()
    return None

path = find_chromium_exe()
if path:
    print(f"✅ Chromium found at: {path}")
else:
    print("❌ Chromium not found. Playwright may not be installed.")
