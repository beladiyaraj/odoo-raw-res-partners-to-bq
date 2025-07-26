from flask import Flask, jsonify
import main

app = Flask(__name__)


@app.route("/", methods=["POST", "GET"])
def run_main():
    try:
        main.main(None, None)  # call your existing function
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
