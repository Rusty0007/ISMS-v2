# Sport ruleset configs — drives referee console UI and scoring validation

# Universal error codes (common across all sports)
UNIVERSAL_ERRORS = [
    {"code": "UNFORCED_ERROR",  "label": "Unforced Error"},
    {"code": "SERVICE_FAULT",   "label": "Service Fault"},
    {"code": "OUT_BALL",        "label": "Out / Ball Out"},
    {"code": "NET_TOUCH",       "label": "Net Touch"},
    {"code": "DOUBLE_HIT",      "label": "Double Hit"},
    {"code": "MISSED_RETURN",   "label": "Missed Return"},
    {"code": "FOOT_FAULT",      "label": "Foot Fault"},
    {"code": "RULE_VIOLATION",  "label": "Rule Violation"},
]

SPORT_RULESETS: dict = {
    "badminton": {
        "label": "Badminton",
        "sets_to_win": 2,
        "points_per_set": 21,
        "win_by": 2,
        "max_points": 30,
        "max_sets": 3,
        "serve_rotation": "rally_point",
        "violation_types": [
            {"code": "service_fault",  "label": "Service Fault"},
            {"code": "foot_fault",     "label": "Foot Fault"},
            {"code": "net_touch",      "label": "Net Touch"},
            {"code": "shuttle_out",    "label": "Shuttle Out"},
            {"code": "double_hit",     "label": "Double Hit"},
            {"code": "carry",          "label": "Carry / Sling"},
            {"code": "obstruction",    "label": "Obstruction"},
        ],
        "error_types": [
            {"code": "SERVICE_FAULT",  "label": "Service Fault"},
            {"code": "OUT_BALL",       "label": "Shuttle Out"},
            {"code": "NET_TOUCH",      "label": "Net Touch"},
            {"code": "DOUBLE_HIT",     "label": "Double Hit"},
            {"code": "FOOT_FAULT",     "label": "Foot Fault"},
            {"code": "SHUTTLE_CARRY",  "label": "Shuttle Carry"},
            {"code": "UNFORCED_ERROR", "label": "Unforced Error"},
            {"code": "RULE_VIOLATION", "label": "Rule Violation"},
        ],
        "scoring_causes": [
            "Smash winner", "Drop shot winner", "Net shot winner",
            "Clear winner", "Drive winner", "Deceptive shot",
        ],
    },
    "pickleball": {
        "label": "Pickleball",
        "sets_to_win": 2,
        "points_per_set": 11,
        "win_by": 2,
        "max_points": 15,
        "max_sets": 3,
        "serve_rotation": "side_out",
        "violation_types": [
            {"code": "kitchen_fault",  "label": "Kitchen Fault (NVZ)"},
            {"code": "service_fault",  "label": "Service Fault"},
            {"code": "out_of_bounds",  "label": "Out of Bounds"},
            {"code": "net_fault",      "label": "Net Fault"},
            {"code": "double_bounce",  "label": "Double Bounce"},
            {"code": "carry",          "label": "Carry"},
        ],
        "error_types": [
            {"code": "SERVICE_FAULT",        "label": "Service Fault"},
            {"code": "OUT_BALL",             "label": "Out of Bounds"},
            {"code": "NON_VOLLEY_ZONE_FAULT","label": "Kitchen / NVZ Fault"},
            {"code": "DOUBLE_BOUNCE",        "label": "Double Bounce"},
            {"code": "NET_TOUCH",            "label": "Net Fault"},
            {"code": "MISSED_RETURN",        "label": "Missed Return"},
            {"code": "UNFORCED_ERROR",       "label": "Unforced Error"},
            {"code": "RULE_VIOLATION",       "label": "Rule Violation"},
        ],
        "scoring_causes": [
            "Dink winner", "Drive winner", "Drop shot winner",
            "Lob winner", "Smash winner",
        ],
    },
    "lawn_tennis": {
        "label": "Lawn Tennis",
        "sets_to_win": 2,
        "games_per_set": 6,
        "win_by": 2,
        "tiebreak_at": 6,
        "max_sets": 3,
        "serve_rotation": "alternating",
        "violation_types": [
            {"code": "double_fault",   "label": "Double Fault"},
            {"code": "foot_fault",     "label": "Foot Fault"},
            {"code": "out",            "label": "Ball Out"},
            {"code": "net_fault",      "label": "Net Fault"},
            {"code": "hindrance",      "label": "Hindrance"},
            {"code": "time_violation", "label": "Time Violation"},
        ],
        "error_types": [
            {"code": "DOUBLE_FAULT",   "label": "Double Fault"},
            {"code": "SERVICE_FAULT",  "label": "Service Fault"},
            {"code": "OUT_BALL",       "label": "Ball Out"},
            {"code": "NET_TOUCH",      "label": "Net Fault"},
            {"code": "FOOT_FAULT",     "label": "Foot Fault"},
            {"code": "MISSED_RETURN",  "label": "Missed Return"},
            {"code": "UNFORCED_ERROR", "label": "Unforced Error"},
            {"code": "RULE_VIOLATION", "label": "Rule Violation"},
        ],
        "scoring_causes": [
            "Ace", "Service winner", "Volley winner",
            "Groundstroke winner", "Passing shot", "Drop shot winner",
        ],
    },
    "table_tennis": {
        "label": "Table Tennis",
        "sets_to_win": 3,
        "points_per_set": 11,
        "win_by": 2,
        "max_points": None,
        "max_sets": 5,
        "serve_rotation": "every_2_points",
        "violation_types": [
            {"code": "service_fault",  "label": "Service Fault"},
            {"code": "edge_disputed",  "label": "Edge Ball (disputed)"},
            {"code": "net_let",        "label": "Net Let"},
            {"code": "obstruction",    "label": "Obstruction"},
            {"code": "illegal_serve",  "label": "Illegal Serve (hidden)"},
        ],
        "error_types": [
            {"code": "SERVICE_FAULT",  "label": "Service Fault"},
            {"code": "WRONG_ORDER",    "label": "Wrong Serving Order"},
            {"code": "MISSED_RETURN",  "label": "Missed Return"},
            {"code": "OUT_BALL",       "label": "Ball Out"},
            {"code": "NET_TOUCH",      "label": "Net Touch (edge)"},
            {"code": "ILLEGAL_SERVE",  "label": "Illegal Serve"},
            {"code": "UNFORCED_ERROR", "label": "Unforced Error"},
            {"code": "RULE_VIOLATION", "label": "Rule Violation"},
        ],
        "scoring_causes": [
            "Smash winner", "Topspin winner", "Chop winner",
            "Serve winner", "Edge ball", "Net ball (let replay)",
        ],
    },
}


def get_ruleset(sport: str) -> dict | None:
    return SPORT_RULESETS.get(sport)
