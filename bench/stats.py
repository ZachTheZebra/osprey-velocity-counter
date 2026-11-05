"""Nearest-rank percentile. p is 0 to 100."""


def percentile(samples, p):
    ordered = sorted(samples)
    if not ordered:
        raise ValueError("no samples")
    if p <= 0:
        return ordered[0]
    if p >= 100:
        return ordered[-1]
    rank = int(round((p / 100) * (len(ordered) - 1)))
    return ordered[rank]
