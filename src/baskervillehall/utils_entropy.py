# utils_entropy.py

import math

def entropy_from_counts(counts: dict[str, int]) -> float:
    """
    Generic reusable Shannon entropy function.
    Accepts a dict of {item -> count}.
    Used by FeatureExtractor and baskerville_rules.
    """
    total = sum(counts.values())
    if total == 0:
        return 0.0

    H = 0.0
    for c in counts.values():
        if c <= 0:
            continue
        p = c / total
        H -= p * math.log(p, 2)
    return H
