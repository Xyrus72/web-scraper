import csv
import io
import uuid
import threading
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
CRAWL_PROGRESS: Dict[str, Dict[str, Any]] = {}  # crawl_id -> {status, progress, total, current}


def _normalize_name(name: str) -> str:
    """
    Normalize product name for deduplication:
    - Remove "Picture of" prefix if present
    - Strip whitespace
    - Convert to lowercase for comparison
    """
    if not name:
        return ""
    normalized = name.strip()
    # Remove "Picture of" prefix if present
    if normalized.lower().startswith("picture of"):
        normalized = normalized[10:].strip()
    return normalized.lower()


def _score_product_completeness(product: Dict[str, Any]) -> int:
    """
    Score a product by how complete its information is.
    Higher score = more complete information.
    """
    score = 0
    name = (product.get("name") or "").strip()
    price = (product.get("price") or "").strip()
    image = (product.get("image_url") or product.get("image") or "").strip()
    href = (product.get("product_href") or "").strip()
    description = (product.get("description") or "").strip()

    if name:
        score += 10
    if price:
        score += 5
    if image:
        score += 10  # Image is very important
    if href:
        score += 3
    if description:
        score += 2

    return score


def _clean_products(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove empty / useless rows and deduplicate products before exposing as CSV/JSON.
    When duplicates exist, keeps the one with the most complete information.

    Rules:
    - Drop rows that have no name, no price and no image at all.
    - Group products by normalized name (or product_href if name is missing).
    - For each group, keep only the product with the highest completeness score.
    """
    # First pass: filter out completely empty products
    valid_products: List[Dict[str, Any]] = []
    for raw in products:
        if not isinstance(raw, dict):
            continue

        p = dict(raw)
        name = (p.get("name") or "").strip()
        price = (p.get("price") or "").strip()
        image = (p.get("image_url") or p.get("image") or "").strip()

        # Keep only rows that have at least one of these fields
        if not (name or price or image):
            continue

        valid_products.append(p)

    # Second pass: group by normalized name or product_href, keep best one
    product_groups: Dict[str, Dict[str, Any]] = {}

    for product in valid_products:
        name = (product.get("name") or "").strip()
        href = (product.get("product_href") or "").strip()

        # Use normalized name as key, fallback to product_href if name is empty
        if name:
            key = _normalize_name(name)
        elif href:
            key = href.lower().strip()
        else:
            # If no name or href, use a combination of available fields as key
            price = (product.get("price") or "").strip()
            image = (product.get("image_url") or product.get("image") or "").strip()
            key = f"{price}_{image}".lower()[:50]  # Truncate to avoid huge keys

        if not key:
            continue

        # If we haven't seen this key, or if current product has higher score, keep it
        if key not in product_groups:
            product_groups[key] = product
        else:
            current_score = _score_product_completeness(product_groups[key])
            new_score = _score_product_completeness(product)
            if new_score > current_score:
                product_groups[key] = product

    return list(product_groups.values())


def _crawl_in_background(crawl_id: str, homepage: str, max_pages: int):
    """
    Run crawl in background thread and update progress.
    """
    try:
        CRAWL_PROGRESS[crawl_id] = {"status": "discovering", "progress": 0, "total": max_pages, "current": 0}
        
        # Import here to avoid circular import
        from crawler_service import crawl_site_with_progress
        
        def progress_cb(current: int, total: int, status: str):
            _update_progress(crawl_id, current, total, status)
        
        products = crawl_site_with_progress(
            homepage, 
            max_pages=max_pages,
            progress_callback=progress_cb
        )
        
        products = _clean_products(products)
        CRAWL_RESULTS[crawl_id] = products
        CRAWL_PROGRESS[crawl_id] = {"status": "completed", "progress": 100, "total": len(products), "current": len(products)}
    except Exception as e:
        CRAWL_PROGRESS[crawl_id] = {"status": "error", "progress": 0, "total": 0, "current": 0, "error": str(e)}


def _update_progress(crawl_id: str, current: int, total: int, status: str):
    """Update progress for a crawl."""
    if crawl_id in CRAWL_PROGRESS:
        progress_pct = int((current / total * 100)) if total > 0 else 0
        CRAWL_PROGRESS[crawl_id] = {
            "status": status,
            "progress": progress_pct,
            "total": total,
            "current": current
        }


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/crawl", methods=["POST"])
def start_crawl():
    """
    Accepts a homepage URL, starts crawl in background, and redirects to progress page.
    """
    homepage = request.form.get("homepage_url", "").strip()
    if not homepage:
        return redirect(url_for("index"))

    max_pages = int(request.form.get("max_pages", "30") or "30")

    crawl_id = str(uuid.uuid4())
    
    # Start crawl in background thread
    thread = threading.Thread(target=_crawl_in_background, args=(crawl_id, homepage, max_pages))
    thread.daemon = True
    thread.start()

    resp = make_response(redirect(url_for("crawl_progress", crawl_id=crawl_id)))
    resp.set_cookie("last_crawl_id", crawl_id, max_age=60 * 60 * 24)
    return resp


@app.route("/progress/<crawl_id>", methods=["GET"])
def crawl_progress(crawl_id: str):
    """Show crawl progress page."""
    progress = CRAWL_PROGRESS.get(crawl_id, {"status": "unknown", "progress": 0, "total": 0, "current": 0})
    return render_template("progress.html", crawl_id=crawl_id, progress=progress)


@app.route("/api/progress/<crawl_id>", methods=["GET"])
def api_progress(crawl_id: str):
    """API endpoint to check crawl progress."""
    progress = CRAWL_PROGRESS.get(crawl_id, {"status": "unknown", "progress": 0, "total": 0, "current": 0})
    
    # If completed, redirect to results
    if progress.get("status") == "completed":
        return jsonify({
            **progress,
            "redirect": url_for("view_results", crawl_id=crawl_id)
        })
    
    return jsonify(progress)


@app.route("/results/<crawl_id>", methods=["GET"])
def view_results(crawl_id: str):
    products = _clean_products(CRAWL_RESULTS.get(crawl_id, []))
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
    products = _clean_products(CRAWL_RESULTS.get(crawl_id, []))
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
    products = _clean_products(CRAWL_RESULTS.get(crawl_id, []))
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


@app.route("/download_json/<crawl_id>", methods=["GET"])
def download_json(crawl_id: str):
    """
    Download cleaned product data as a JSON file.
    """
    products = _clean_products(CRAWL_RESULTS.get(crawl_id, []))
    if not products:
        return jsonify({"error": "No data for this crawl id"}), 404

    import json

    json_bytes = json.dumps(products, ensure_ascii=False, indent=2).encode("utf-8")

    resp = make_response(json_bytes)
    resp.headers["Content-Type"] = "application/json; charset=utf-8"
    resp.headers[
        "Content-Disposition"
    ] = f'attachment; filename="products_{crawl_id}.json"'
    return resp


if __name__ == "__main__":
    # Run the Flask dev server
    app.run(debug=True, host="0.0.0.0", port=5000)

