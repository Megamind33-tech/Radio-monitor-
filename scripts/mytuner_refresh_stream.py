#!/usr/bin/env python3
"""
Print decrypted direct stream URL for a MyTuner station page (stdout, one line).
Used by the Node monitor for self-healing when sourceIdsJson has mytuner page URL.

Requires: pycryptodome (same as harvest scripts).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mytuner_zambia import decrypt_first_stream_url, fetch_text


def main() -> None:
    if len(sys.argv) < 2:
        print("", end="")
        sys.exit(2)
    page_url = sys.argv[1].strip()
    if not page_url.startswith("http"):
        print("", end="")
        sys.exit(2)
    try:
        html = fetch_text(page_url)
        su, _name, _rid = decrypt_first_stream_url(html, page_url)
        if su.startswith("http"):
            print(su, end="")
            sys.exit(0)
    except Exception:
        pass
    print("", end="")
    sys.exit(1)


if __name__ == "__main__":
    main()
