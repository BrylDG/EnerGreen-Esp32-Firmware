import functions_framework
import uuid
from google.cloud import firestore

db = firestore.Client()

# Default feature weights
BASE_POWER_WEIGHT = 0.5
BASE_PF_WEIGHT = 0.3
RISE_WEIGHT = 0.1
OVERSHOOT_WEIGHT = 0.1
MAX_ACCEPTABLE_SCORE = 0.55   # ↑ increased tolerance for minor variations


@functions_framework.http
def store_signature(request):
    try:
        data = request.get_json()
        if not data:
            return {"error": "Empty request"}, 400

        # Extract or derive feature set
        features = data.get("features")
        if not features and "signature_data" in data and data["signature_data"]:
            last_point = data["signature_data"][-1]
            features = {
                "steady_avg_power": last_point.get("powerWatt"),
                "powerFactor": last_point.get("powerFactor"),
                "rise_time": -1,
                "overshoot": 0.0
            }

        if not features:
            return {"error": "Could not extract features"}, 400

        steady_avg_power = features.get("steady_avg_power")
        pf = features.get("powerFactor")
        rise_time = features.get("rise_time", -1)
        overshoot = features.get("overshoot", 0.0)

        col_ref = db.collection("ApplianceSignatures")
        matches = col_ref.stream()

        best_match = None
        best_score = 999

        for doc in matches:
            doc_data = doc.to_dict()
            doc_features = doc_data.get("features", {})
            if not doc_features:
                continue

            existing_power = doc_features.get("steady_avg_power", 0)
            existing_pf = doc_features.get("powerFactor", 0)
            existing_rise = doc_features.get("rise_time", -1)
            existing_overshoot = doc_features.get("overshoot", 0.0)

            # --- Adaptive weight tuning for low-power devices ---
            power_weight = BASE_POWER_WEIGHT
            pf_weight = BASE_PF_WEIGHT
            max_score = MAX_ACCEPTABLE_SCORE

            if existing_power < 20 and steady_avg_power < 20:
                # For small appliances (e.g. fans, chargers)
                power_weight = 0.3
                pf_weight = 0.2
                max_score = 0.6  # ↑ more forgiving

            # --- Normalized differences ---
            power_diff = abs(existing_power - steady_avg_power) / max(existing_power, 1)
            pf_diff = abs(existing_pf - pf)

            if rise_time == -1 and existing_rise == -1:
                rise_diff = 0.0
            elif rise_time == -1 or existing_rise == -1:
                rise_diff = 1.0  # penalize mismatch
            else:
                rise_diff = abs(existing_rise - rise_time) / max(existing_rise, 1)

            overshoot_diff = abs(existing_overshoot - overshoot) / max(existing_overshoot, overshoot, 1)

            score = (power_weight * power_diff +
                     pf_weight * pf_diff +
                     RISE_WEIGHT * rise_diff +
                     OVERSHOOT_WEIGHT * overshoot_diff)

            print(f"[DEBUG] Comparing to {doc_data.get('applianceName','?')} "
                  f"(Pdiff={power_diff:.2f}, PFdiff={pf_diff:.2f}) => Score={score:.3f}")

            if score < best_score:
                best_score = score
                best_match = doc
                best_match_threshold = max_score  # store its adaptive threshold

        # --- Match decision ---
        if best_match and best_score < best_match_threshold:
            doc_ref = col_ref.document(best_match.id)
            doc_ref.update({
                "signatures": firestore.ArrayUnion([data]),
                "lastUpdated": firestore.SERVER_TIMESTAMP
            })

            match_data = best_match.to_dict()
            return {
                "applianceName": match_data.get("applianceName", "Unknown"),
                "applianceID": match_data.get("applianceID"),
                "matchScore": round(best_score, 3)
            }, 200

        # --- No good match → create new record ---
        matched_id = str(uuid.uuid4())[:8]
        new_doc = {
            "applianceID": matched_id,
            "applianceName": "Unknown",
            "features": features,
            "signatures": [data],
            "createdAt": firestore.SERVER_TIMESTAMP,
            "lastUpdated": firestore.SERVER_TIMESTAMP
        }
        col_ref.add(new_doc)
        print(f"[INFO] New appliance registered → ID {matched_id} | Power={steady_avg_power}W PF={pf}")
        return {"applianceID": matched_id, "applianceName": "Unknown", "matchScore": None}, 200

    except Exception as e:
        print("[ERROR]", e)
        return {"error": str(e)}, 500
