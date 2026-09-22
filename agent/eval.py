"""Genie Doctor — the Doctor's own exam.

Runs the diagnostic agent over the planted-defect benchmark and measures:
  recall    — did it flag every defect it MUST flag?
  precision — did it avoid flagging things it MUST NOT?

Usage:  python agent/eval.py [--profile pulse-azure]
Without a reachable model endpoint it still scores the DETERMINISTIC findings
(LLM cases will simply not be flagged, lowering recall on those two categories).
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import diagnose as dx


def make_query_fn(profile: str):
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
        w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()

        def q(system, user):
            r = w.serving_endpoints.query(
                name=dx.MODEL, max_tokens=1500,
                messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
                          ChatMessage(role=ChatMessageRole.USER, content=user)])
            return r.choices[0].message.content
        return q
    except Exception as e:
        print(f"[warn] no model client ({str(e)[:80]}) — deterministic-only scoring")
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default=os.environ.get("DATABRICKS_PROFILE", ""))
    args = ap.parse_args()

    bench = json.load(open(os.path.join(os.path.dirname(__file__), "doctor_benchmark.json")))
    qfn = make_query_fn(args.profile)

    tp = fn = fp = 0
    print(f"{'case':<26} {'flagged':<45} recall/precision")
    print("-" * 92)
    for c in bench["cases"]:
        steps = dx.diagnose(c["evidence"], query_fn=qfn)
        flagged = {s["category"] for s in steps}
        must = set(c["expected"]["must_flag"])
        mustnot = set(c["expected"]["must_not_flag"])
        missed = must - flagged           # false negatives
        wrong = flagged & mustnot         # false positives
        tp += len(must & flagged); fn += len(missed); fp += len(wrong)
        ok = "OK" if not missed and not wrong else "FAIL"
        print(f"{c['case_id']:<26} {','.join(sorted(flagged)) or '(none)':<45} {ok}"
              + (f"  missed={sorted(missed)}" if missed else "")
              + (f"  false+={sorted(wrong)}" if wrong else ""))
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    precision = tp / (tp + fp) if (tp + fp) else 1.0
    print("-" * 92)
    print(f"RECALL={recall:.0%} (caught {tp}/{tp+fn} required)   "
          f"PRECISION={precision:.0%} (false positives={fp})")


if __name__ == "__main__":
    main()
