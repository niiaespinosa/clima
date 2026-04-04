from __future__ import annotations

import sys

from clima_pipeline import main


if __name__ == "__main__":
    raise SystemExit(main(["gold", *sys.argv[1:]]))
