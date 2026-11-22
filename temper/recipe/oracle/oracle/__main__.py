"""python -m oracle AREA SUBJECT: decide one area's claims for one subject,
printing a JSON object from reference to violation count. Explanations go to
stderr, which the run log keeps."""
import json
import sys

from . import adhoc, input_errors, kafka, lateness, legacy, now, pg, pk, txn, type_ops, types_contract, views

AREAS = {
    "input": input_errors.run,
    "views": views.run,
    "lateness": lateness.run,
    "txn": txn.run,
    "pk": pk.run,
    "adhoc": adhoc.run,
    "now": now.run,
    "types": types_contract.run,
    "kafka": kafka.run,
    "type_ops": type_ops.run,
    "pg": pg.run,
    # The original harness's tests, unchanged (legacy.py): views, churn,
    # worker counts, lateness, transactions and operator type semantics.
    "legacy": legacy.run,
}


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "legacy-refs":
        print("\n".join(legacy.refs(sys.argv[2])))
        return
    if len(sys.argv) != 3 or sys.argv[1] not in AREAS:
        raise SystemExit(f"usage: python -m oracle {{{','.join(AREAS)}}} SUBJECT")
    print(json.dumps(AREAS[sys.argv[1]](sys.argv[2]), sort_keys=True))


if __name__ == "__main__":
    main()
