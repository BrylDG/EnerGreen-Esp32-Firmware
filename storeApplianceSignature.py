import functions_framework
import uuid
from google.cloud import firestore

db = firestore.Client()

# Feature weights
POWER_WEIGHT = 0.5
PF_WEIGHT = 0.3
RISE_WEIGHT = 0.1
OVERSHOOT_WEIGHT = 0.1
MAX_ACCEPTABLE_SCORE = 0.35   # tune this threshold

@functions_framework.http
def store_signature(request):
    try:
        data = request.get_json()
        if not data:
            return {"error": "Empty request"}, 400

        # Extract features
        features = data.get("features")
        if not features:
            if "signature_data" in data and data["signature_data"]:
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

            # Normalized differences
            power_diff = abs(existing_power - steady_avg_power) / max(existing_power, 1)
            pf_diff = abs(existing_pf - pf)

            if rise_time == -1 and existing_rise == -1:
                rise_diff = 0.0
            elif rise_time == -1 or existing_rise == -1:
                rise_diff = 1.0  # penalize missing mismatch
            else:
                rise_diff = abs(existing_rise - rise_time) / max(existing_rise, 1)

            overshoot_diff = abs(existing_overshoot - overshoot) / max(existing_overshoot, overshoot, 1)

            score = (POWER_WEIGHT * power_diff +
                     PF_WEIGHT * pf_diff +
                     RISE_WEIGHT * rise_diff +
                     OVERSHOOT_WEIGHT * overshoot_diff)

            if score < best_score:
                best_score = score
                best_match = doc

        # --- If a good match ---
        if best_match and best_score < MAX_ACCEPTABLE_SCORE:
            doc_ref = col_ref.document(best_match.id)
            # Append new signature to history
            doc_ref.update({
                "signatures": firestore.ArrayUnion([data]),
                "lastUpdated": firestore.SERVER_TIMESTAMP
            })
            return {
                "applianceName": best_match.to_dict().get("applianceName", "Unknown"),
                "applianceID": best_match.to_dict().get("applianceID"),
                "matchScore": round(best_score, 3)
            }, 200

        # --- No good match → create new appliance ---
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
        return {"applianceID": matched_id, "applianceName": "Unknown", "matchScore": None}, 200

    except Exception as e:
        return {"error": str(e)}, 500
