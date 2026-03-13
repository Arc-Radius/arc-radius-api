import os
from pathlib import Path

from dotenv import load_dotenv

# graph environment in order to run experiments using different environments
def load_graph_env() -> None:
    graph_root = Path(__file__).resolve().parents[1]
    backend_env = graph_root.parent / ".env"
    load_dotenv(backend_env, override=False)

    graph_env_file = os.getenv("GRAPH_ENV_FILE")
    if not graph_env_file:
        return

    env_path = Path(graph_env_file)
    if not env_path.is_absolute():
        env_path = graph_root / env_path
    load_dotenv(env_path, override=True)
