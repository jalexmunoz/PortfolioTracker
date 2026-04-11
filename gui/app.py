import os
from flask import Flask

from .db_context import load_active_db
from .routes import bp


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY=os.environ.get("PORTFOLIO_GUI_SECRET_KEY", "portfolio-gui-h1-local"),
        GUI_APP_NAME="PortfolioTracker GUI v0.1",
    )

    if test_config:
        app.config.update(test_config)

    app.register_blueprint(bp)

    @app.context_processor
    def inject_gui_context():
        active = load_active_db()
        return {
            "gui_active_db": active,
        }

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
