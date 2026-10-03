import argparse
import os
from http.server import ThreadingHTTPServer

from src.repository import Repository
from src.service import Service
from src.http_api import build_handler


def main():
    parser = argparse.ArgumentParser(description="燃气管线泄漏检测与隔离服务")
    parser.add_argument("--db", default=os.path.join(os.path.dirname(__file__), "data.db"))
    parser.add_argument("--port", type=int, default=8333)
    parser.add_argument("--init", action="store_true", help="initialize the database and exit")
    parser.add_argument("--seed", action="store_true", help="seed the demo region ledger with --init")
    args = parser.parse_args()

    repo = Repository(args.db)
    repo.initialize()
    if args.seed:
        repo.seed_demo_ledger()
    if args.init:
        print("initialized: %s" % args.db)
        return

    service = Service(repo)
    resume = service.resume_on_startup()
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    server = ThreadingHTTPServer(("127.0.0.1", args.port), build_handler(service, static_dir))
    server.service = service
    print("gas pipeline leak service listening on http://127.0.0.1:%d" % args.port)
    print("startup resume: %d promoted, %d unfinished commands, %d open conflicts" % (
        len(resume["promoted"]), len(resume["resumed"]), len(resume["open_conflicts"])))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
