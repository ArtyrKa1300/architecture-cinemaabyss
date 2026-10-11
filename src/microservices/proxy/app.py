import os
import random

import requests
from flask import Flask, Response, jsonify, request


app = Flask(__name__)

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")

GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))


def choose_service(path):
    if path.startswith("api/events"):
        return "events-service", EVENTS_SERVICE_URL

    if path.startswith("api/movies"):
        if not GRADUAL_MIGRATION:
            return "movies-service", MOVIES_SERVICE_URL

        if random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
            return "movies-service", MOVIES_SERVICE_URL

    return "monolith", MONOLITH_URL


@app.get("/health")
def health():
    return jsonify(status=True)


@app.route(
    "/<path:path>",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
)
def proxy(path):
    service_name, service_url = choose_service(path)
    url = f"{service_url.rstrip('/')}/{path}"

    try:
        result = requests.request(
            method=request.method,
            url=url,
            params=request.args,
            data=request.get_data(),
            headers={"Content-Type": request.content_type}
            if request.content_type
            else {},
            timeout=10,
        )
    except requests.RequestException:
        return jsonify(error=f"{service_name} is unavailable"), 502

    response = Response(
        result.content,
        status=result.status_code,
        content_type=result.headers.get("Content-Type"),
    )
    response.headers["X-CinemaAbyss-Upstream"] = service_name
    return response


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        threaded=True,
    )
