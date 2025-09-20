from flask import Flask, jsonify, request
from flask_cors import CORS
from fotmob_scraper import scrape_matches
from datetime import datetime
import pytz

app = Flask(__name__)
CORS(app)

@app.route("/api/matches", methods=["GET"])
def get_matches():
    try:
        team = request.args.get("team", "castilla")
        season = request.args.get("season", "2025")

        matches = scrape_matches()

        metadata = {
            "fuente": "Transfermarkt (scraper simplificado)",
            "ultima_actualizacion": datetime.now(pytz.timezone("America/Guatemala")).isoformat(),
            "version": "3.1.0-transfermarkt",
            "zona_horaria": "America/Guatemala"
        }

        return jsonify({
            "metadata": metadata,
            "partidos_completos": matches,
            "resumen": {
                "total": len(matches),
                "finalizados": sum(1 for m in matches if m["status"] == "FINISHED"),
                "proximos": sum(1 for m in matches if m["status"] == "SCHEDULED"),
                "en_vivo": sum(1 for m in matches if m["status"] == "LIVE") if any("LIVE" in m["status"] for m in matches) else 0
            }
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/status", methods=["GET"])
def status():
    return jsonify({
        "estado": "OK",
        "mensaje": "API funcionando correctamente",
        "hora": datetime.now(pytz.timezone("America/Guatemala")).isoformat()
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
