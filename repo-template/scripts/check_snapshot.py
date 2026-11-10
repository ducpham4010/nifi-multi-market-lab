#!/usr/bin/env python3
"""Lab policy: no parameter values, local PGs, provider bindings or environment literals.
Run before MR. This is a structural guard, not a general-purpose secret scanner.
"""

import json, re, sys, pathlib

root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "flows/shared")
files = [root] if root.is_file() else sorted(root.glob("*.json"))
errors = []


def walk(obj, path):
    if isinstance(obj, dict):
        for key, value in obj.items():
            loc = path + "/" + key
            if key == "parameters" and isinstance(value, list):
                for p in value:
                    if isinstance(p, dict) and p.get("value") is not None:
                        errors.append(
                            loc
                            + ": parameter value must be null/absent (empty string also rejected)"
                        )
            if key == "parameterProviderConfiguration" and value:
                errors.append(
                    loc
                    + ": remove provider association from portable Parameter Context; use local value copy"
                )
            if key == "name" and isinstance(value, str) and value.startswith("LOCAL_"):
                errors.append(loc + ": local component included")
            if key == "properties" and isinstance(value, dict):
                for prop, val in value.items():
                    if val is None:
                        continue
                    text = str(val)
                    if re.search(
                        r"\b(?:\d{1,3}\.){3}\d{1,3}\b|jdbc:|https?://|nifi-(vn|indo)-lab-token",
                        text,
                    ):
                        errors.append(
                            loc + "/" + prop + ": environment literal; use parameter"
                        )
                    if (
                        re.search(r"password|token|credential", prop, re.I)
                        and text
                        and not re.fullmatch(r"#\{[^}]+\}", text)
                    ):
                        errors.append(
                            loc
                            + "/"
                            + prop
                            + ": credential must be parameter reference"
                        )
            walk(value, loc)
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            walk(value, path + f"/{i}")


if not files:
    errors.append("No snapshot JSON found")
for f in files:
    try:
        data = json.loads(f.read_text())
        if not isinstance(data, dict) or not isinstance(data.get("flowContents"), dict):
            errors.append(
                str(f) + ": expected NiFi registry snapshot with flowContents"
            )
            continue
        walk(data, str(f))
    except (ValueError, OSError) as e:
        errors.append(str(f) + ": " + str(e))
if errors:
    print("\n".join(errors), file=sys.stderr)
    sys.exit(1)
print(f"PASS: {len(files)} snapshot(s); manual diff review still required")
