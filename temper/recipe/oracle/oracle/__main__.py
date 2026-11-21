"""python -m oracle AREA SUBJECT: decide one area's claims for one subject,
printing a JSON object from reference to violation count. Explanations go to
stderr, which the run log keeps."""
import json
import sys

from . import input_errors

AREAS = {
    "input": input_errors.run,
}


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in AREAS:
        raise SystemExit(f"usage: python -m oracle {{{','.join(AREAS)}}} SUBJECT")
    print(json.dumps(AREAS[sys.argv[1]](sys.argv[2]), sort_keys=True))


if __name__ == "__main__":
    main()
