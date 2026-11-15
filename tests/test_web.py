from pathlib import Path

from cellcloak.web import create_app

ROOT = Path(__file__).resolve().parents[1]


def _app(tmp_path):
    app = create_app(
        db_path=tmp_path / "ledger.sqlite3",
        demo_out=tmp_path / "web-out",
        testing=True,
    )
    return app, app.test_client()


def test_home_and_static_css_are_local(tmp_path):
    _, client = _app(tmp_path)
    response = client.get("/")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "小域遮审台" in html
    assert "运行演示 fixture" in html
    assert "http://" not in html
    assert "https://" not in html
    assert "referrer-policy" in {key.lower() for key in response.headers.keys()}
    css = client.get("/static/app.css")
    assert css.status_code == 200
    assert "font-family" in css.get_data(as_text=True)


def test_demo_button_runs_only_bundled_fixture(tmp_path):
    app, client = _app(tmp_path)
    response = client.post("/demo", follow_redirects=True)
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "region_by_age" in html
    assert "核验结果" in html
    assert "raw_count" not in html
    ledger = app.extensions["ledger"]
    runs = ledger.list_runs()
    assert len(runs) == 1
    assert runs[0]["status"] == "ok"
    assert runs[0]["id"] in html
    detail = ledger.get_run_detail(runs[0]["id"])
    assert detail["tables"][1]["verify_passed"] is True


def test_home_shows_current_release_after_demo_run(tmp_path):
    app, client = _app(tmp_path)
    html = client.get("/").get_data(as_text=True)
    assert "当前发布" not in html

    client.post("/demo", follow_redirects=True)
    run = app.extensions["ledger"].list_runs()[0]
    html = client.get("/").get_data(as_text=True)
    assert "当前发布" in html
    assert run["id"] in html
    assert "generations" in html


def test_diff_page_compares_two_runs(tmp_path):
    app, client = _app(tmp_path)
    for _ in range(2):
        assert client.post("/demo", follow_redirects=False).status_code == 302
    runs = app.extensions["ledger"].list_runs()
    assert len(runs) == 2
    response = client.get(f"/diff?run_a={runs[1]['id']}&run_b={runs[0]['id']}")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "RUN A" in html
    assert "RUN B" in html
    assert "公开输出哈希变化" in html
    assert "unchanged" in html


def test_unknown_run_and_error_page_do_not_leak_paths(tmp_path):
    _, client = _app(tmp_path)
    response = client.get("/runs/run-does-not-exist")
    assert response.status_code == 404
    html = response.get_data(as_text=True)
    assert "页面不存在" in html
    assert str(tmp_path) not in html
    assert "Traceback" not in html


def test_run_page_never_returns_raw_cell_values(tmp_path):
    app, client = _app(tmp_path)
    client.post("/demo", follow_redirects=True)
    run = app.extensions["ledger"].list_runs()[0]
    response = client.get(f"/runs/{run['id']}")
    html = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "raw_count" not in html
    assert "原始计数" not in html or "不展示" in html
    assert "http://" not in html
    assert "https://" not in html
