import math

SCALE = 173.7178
TAU = 0.5

def _g(phi: float) -> float:
    return 1.0 / math.sqrt(1 + 3 * phi**2 / math.pi**2)

def _E(mu: float, mu_j: float, phi_j: float) -> float:
    exponent = -_g(phi_j) * (mu - mu_j)
    exponent = max(-700.0, min(700.0, exponent))  # clamp to prevent math.exp overflow
    e = 1.0 / (1 + math.exp(exponent))
    return max(1e-8, min(1 - 1e-8, e))            # keep away from 0/1 to avoid ZeroDivision

def update(
        rating: float,
        rd: float,
        volatility: float,
        opp_rating: float,
        opp_rd: float,
        score: float,
) -> tuple[float, float, float]:
    """
    Single-game Glicko-2 update.
    Returns (new_rating, new_rd, new_volatility) on the original 1500-scale.
    """
    # Convert to Glicko-2 scale
    mu = (rating - 1500) / SCALE
    phi = rd / SCALE
    mu_j = (opp_rating - 1500) / SCALE
    phi_j = opp_rd / SCALE
    sigma = volatility

    g_j = _g(phi_j)
    e_j = _E(mu, mu_j, phi_j)

    # Estimated variance v
    v = 1.0 / (g_j**2 * e_j * (1.0 - e_j))

    # Score-based improvement delta
    delta = v * g_j * (score - e_j)

    # ── New volatility (Illinois algorithm) ──────────────────────────────────
    a = math.log(sigma**2)

    def f(x: float) -> float:
        ex = math.exp(x)
        phi2 = phi**2
        d = phi2 + v + ex
        return (ex * (delta**2 - phi2 - v - ex) / (2 * d**2)
                - (x - a) / TAU**2)
    
    A = a
    if delta**2 > phi**2 + v:
        B = math.log(delta**2 - phi**2 - v)
    else:
        k = 1
        while f(a - k * TAU) < 0:
            k += 1
        B = a - k * TAU

    fA, fB = f(A), f(B)
    for _ in range(100):
        if abs(fB - fA) < 1e-10:
            break
        C = A + (A - B) * fA / (fB - fA)
        fC = f(C)
        if fC * fB < 0:
            A, fA = B, fB
        else:
            fA /= 2
            B, fB = C, fC
            if abs(B - A) < 1e-6:
                break

    sigma_prime = math.exp(A / 2)

    # ── New RD and rating ────────────────────────────────────────────────────
    phi_star = math.sqrt(phi**2 + sigma_prime**2)
    phi_prime = 1.0 / math.sqrt(1.0 / phi_star**2 + 1.0 / v)
    mu_prime = mu + phi_prime**2 * g_j * (score - e_j)

    # Convert back to original scale
    new_rating = mu_prime * SCALE + 1500
    new_rd = phi_prime * SCALE

    return round(new_rating, 2), round(new_rd, 2), round(sigma_prime, 6)