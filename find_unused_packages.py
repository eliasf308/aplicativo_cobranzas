# find_unused_packages.py
# Detecta paquetes instalados que no parecen ser usados por el código del proyecto.
# Uso:
#   python find_unused_packages.py --project-dir . --format table
#
import argparse
import os
import sys
import ast
from pathlib import Path
from importlib import metadata

# --- Config ---
DEFAULT_EXTS = {".py"}
DEFAULT_IGNORE_DIRS = {
    ".git", ".hg", ".svn", ".venv", "venv", "env",
    ".idea", ".vscode", "__pycache__", "node_modules",
    "dist", "build", "staticfiles", "migrations"  # ignorar migraciones
}
# Paquetes que NO vamos a marcar como "no usados" aunque no aparezcan importados explícitamente
SAFE_ALLOWLIST = {
    "pip", "setuptools", "wheel",
    "python-dotenv", "flower",
    "asgiref", "sqlparse", "tzdata",
}

def norm_name(name: str | None) -> str | None:
    if not name:
        return None
    return name.strip().lower().replace("_", "-")

def collect_py_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        # filtrar directorios
        dirnames[:] = [d for d in dirnames if d not in DEFAULT_IGNORE_DIRS]
        for fn in filenames:
            if Path(fn).suffix in DEFAULT_EXTS:
                yield Path(dirpath) / fn

def top_level_imports_from_file(path: Path):
    out = set()
    try:
        code = path.read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(code, filename=str(path))
    except Exception:
        return out
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                top = n.name.split(".")[0]
                if top:
                    out.add(top)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                top = node.module.split(".")[0]
                if top:
                    out.add(top)
    return out

def map_imports_to_distributions(imports: set[str]) -> set[str]:
    # packages_distributions: {top_level_package: [dist_name, ...]}
    try:
        pkg2dists = metadata.packages_distributions() or {}
    except Exception:
        pkg2dists = {}
    used_dists = set()
    for top in imports:
        if not top:
            continue
        for dist_name in pkg2dists.get(top, []):
            n = norm_name(dist_name)
            if n:
                used_dists.add(n)
    return used_dists

def get_installed_distributions() -> set[str]:
    names = set()
    for dist in metadata.distributions():
        n = norm_name(dist.metadata.get("Name"))
        if n:
            names.add(n)
    return names

def main():
    ap = argparse.ArgumentParser(description="Detecta paquetes instalados pero no usados en el código.")
    ap.add_argument("--project-dir", default=".", help="Raíz del proyecto a escanear (default: .)")
    ap.add_argument("--format", choices=["table", "text"], default="table", help="Formato de salida")
    args = ap.parse_args()

    root = Path(args.project_dir).resolve()
    if not root.exists():
        print(f"[ERROR] No existe la carpeta: {root}", file=sys.stderr)
        sys.exit(2)

    # 1) Escanear imports
    all_imports = set()
    for py in collect_py_files(root):
        all_imports |= top_level_imports_from_file(py)

    # 2) Mapear a distribuciones
    used_dists = map_imports_to_distributions(all_imports)

    # 3) Instalados
    installed = get_installed_distributions()

    # 4) Allowlist normalizada
    allow = {norm_name(x) for x in SAFE_ALLOWLIST if norm_name(x)}

    # 5) Extras (instalados pero no mapeados por imports) menos allowlist
    unused = sorted((installed - used_dists) - allow)
    used_sorted = sorted(used_dists & installed)

    if args.format == "text":
        print("=== Paquetes instalados NO detectados en imports (posibles no usados) ===")
        for name in unused or ["(ninguno)"]:
            print(name)
        print("\n=== Paquetes detectados como usados ===")
        for name in used_sorted or ["(ninguno)"]:
            print(name)
        return

    # formato tabla simple
    print("=== Posibles NO usados (instalados pero no detectados en imports) ===")
    if not unused:
        print("(ninguno)")
    else:
        for name in unused:
            print(f"- {name}")
    print("\n=== Detectados como usados (por imports) ===")
    if not used_sorted:
        print("(ninguno)")
    else:
        for name in used_sorted:
            print(f"- {name}")

    print("\nNota: esto es una heurística. Dependencias cargadas dinámicamente o por plugins pueden aparecer como 'no usadas'. Verificar antes de desinstalar.")

if __name__ == "__main__":
    main()
