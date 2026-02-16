import csv
import io
import uuid
from typing import Dict, Any, List

from flask import (
    Flask,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
    make_response,
)

from crawler_service import crawl_site


app = Flask(__name__)

# Very simple in-memory storage of crawl results keyed by ID.
# In production you'd persist this in a database or cache.
CRAWL_RESULTS: Dict[str, List[Dict[str, Any]]] = {}


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/crawl", methods=["POST"])
def start_crawl():
    """
    Accepts a homepage URL, crawls up to 30 pages, extracts products, and
    stores the result in memory. Returns a redirect to a results page.
    """
    homepage = request.form.get("homepage_url", "").strip()
    if not homepage:
        return redirect(url_for("index"))

    max_pages = int(request.form.get("max_pages", "30") or "30")

    products = crawl_site(homepage, max_pages=max_pages)

    crawl_id = str(uuid.uuid4())
    CRAWL_RESULTS[crawl_id] = products

    resp = make_response(redirect(url_for("view_results", crawl_id=crawl_id)))
    # Store crawl id in a cookie so browser "remembers" the last run
    resp.set_cookie("last_crawl_id", crawl_id, max_age=60 * 60 * 24)
    return resp


@app.route("/results/<crawl_id>", methods=["GET"])
def view_results(crawl_id: str):
    products = CRAWL_RESULTS.get(crawl_id, [])
    return render_template(
        "results.html",
        crawl_id=crawl_id,
        products=products,
        total=len(products),
    )


@app.route("/api/results/<crawl_id>", methods=["GET"])
def api_results(crawl_id: str):
    """
    JSON API: returns all extracted product data for a given crawl.
    """
    products = CRAWL_RESULTS.get(crawl_id, [])
    return jsonify(
        {
            "crawl_id": crawl_id,
            "total": len(products),
            "products": products,
        }
    )


@app.route("/download/<crawl_id>", methods=["GET"])
def download_csv(crawl_id: str):
    """
    Streams a CSV file of extracted product data.
    Browsers will typically cache the download per usual HTTP semantics.
    """
    products = CRAWL_RESULTS.get(crawl_id, [])
    if not products:
        return jsonify({"error": "No data for this crawl id"}), 404

    # Determine CSV headers from union of keys
    fieldnames = set()
    for p in products:
        fieldnames.update(p.keys())
    field_list = sorted(fieldnames)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=field_list)
    writer.writeheader()
    for p in products:
        writer.writerow(p)

    csv_bytes = output.getvalue().encode("utf-8-sig")

    resp = make_response(csv_bytes)
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers[
        "Content-Disposition"
    ] = f'attachment; filename="products_{crawl_id}.csv"'
    return resp


if __name__ == "__main__":
    # Run the Flask dev server
    app.run(debug=True, host="0.0.0.0", port=5000)

