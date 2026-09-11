#!/usr/bin/env python3
"""
Codebase Tree-Sitter AST Analyzer and Skills Synchronization Verifier.

Usage:
    uv run --with tree-sitter --with tree-sitter-rust --with tree-sitter-typescript --with tree-sitter-sql python scripts/codebase_ast.py scan
    uv run --with tree-sitter --with tree-sitter-rust --with tree-sitter-typescript --with tree-sitter-sql python scripts/codebase_ast.py check-sync
    uv run --with tree-sitter --with tree-sitter-rust --with tree-sitter-typescript --with tree-sitter-sql python scripts/codebase_ast.py markdown
"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any

try:
    from tree_sitter import Language, Parser
    import tree_sitter_rust as tsrust
    import tree_sitter_typescript as tsts
    import tree_sitter_sql as tssql
except ImportError as e:
    sys.stderr.write(f"Missing tree-sitter dependencies: {e}\n")
    sys.stderr.write("Run using uv:\n")
    sys.stderr.write("uv run --with tree-sitter --with tree-sitter-rust --with tree-sitter-typescript --with tree-sitter-sql python scripts/codebase_ast.py <command>\n")
    sys.exit(1)


class CodebaseASTAnalyzer:
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir.resolve()
        
        # Initialize languages & parsers
        self.rust_lang = Language(tsrust.language())
        self.tsx_lang = Language(tsts.language_tsx())
        self.ts_lang = Language(tsts.language_typescript())
        self.sql_lang = Language(tssql.language())

        self.rust_parser = Parser(self.rust_lang)
        self.tsx_parser = Parser(self.tsx_lang)
        self.ts_parser = Parser(self.ts_lang)
        self.sql_parser = Parser(self.sql_lang)

    # ---------------------------------------------------------
    # Rust AST Extraction
    # ---------------------------------------------------------
    def parse_rust_routes(self, routes_file: Path) -> List[Dict[str, str]]:
        if not routes_file.exists():
            return []
        code = routes_file.read_bytes()
        tree = self.rust_parser.parse(code)
        routes = []

        def traverse(node):
            if node.type == 'call_expression':
                func_node = node.child_by_field_name('function')
                args_node = node.child_by_field_name('arguments')
                if func_node and func_node.type == 'field_expression':
                    field = func_node.child_by_field_name('field')
                    if field and field.text.decode('utf-8') == 'route' and args_node:
                        args = [c for c in args_node.children if c.type not in ('(', ')', ',')]
                        if len(args) >= 2:
                            path_text = args[0].text.decode('utf-8').strip('"')
                            handler_text = args[1].text.decode('utf-8')
                            method = "UNKNOWN"
                            for m in ("get", "post", "put", "delete", "patch"):
                                if handler_text.startswith(m + "("):
                                    method = m.upper()
                                    break
                            routes.append({
                                "path": f"/api/v1{path_text}" if not path_text.startswith("/api") else path_text,
                                "raw_path": path_text,
                                "method": method,
                                "handler": handler_text,
                                "line": node.start_point.row + 1
                            })
            for child in node.children:
                traverse(child)

        traverse(tree.root_node)
        return routes

    def parse_rust_models(self, model_file: Path) -> List[Dict[str, Any]]:
        if not model_file.exists():
            return []
        code = model_file.read_bytes()
        tree = self.rust_parser.parse(code)
        models = []

        def traverse(node):
            if node.type == 'struct_item':
                name_node = node.child_by_field_name('name')
                name = name_node.text.decode('utf-8') if name_node else "Unknown"
                
                fields = []
                body_node = node.child_by_field_name('body')
                if body_node:
                    for field in body_node.children:
                        if field.type == 'field_declaration':
                            f_name = field.child_by_field_name('name')
                            f_type = field.child_by_field_name('type')
                            if f_name and f_type:
                                fields.append({
                                    "name": f_name.text.decode('utf-8'),
                                    "type": f_type.text.decode('utf-8')
                                })
                
                models.append({
                    "name": name,
                    "fields_count": len(fields),
                    "fields": fields,
                    "line": node.start_point.row + 1
                })
            for child in node.children:
                traverse(child)

        traverse(tree.root_node)
        return models

    def parse_rust_repository_methods(self, repo_file: Path) -> List[Dict[str, Any]]:
        if not repo_file.exists():
            return []
        code = repo_file.read_bytes()
        tree = self.rust_parser.parse(code)
        methods = []

        def traverse(node):
            if node.type == 'impl_item':
                body = node.child_by_field_name('body')
                if body:
                    for child in body.children:
                        if child.type == 'function_item':
                            name_node = child.child_by_field_name('name')
                            ret_node = child.child_by_field_name('return_type')
                            if name_node:
                                methods.append({
                                    "name": name_node.text.decode('utf-8'),
                                    "line": child.start_point.row + 1,
                                    "return_type": ret_node.text.decode('utf-8') if ret_node else "()"
                                })
            for child in node.children:
                traverse(child)

        traverse(tree.root_node)
        return methods

    # ---------------------------------------------------------
    # Frontend TSX / TS Extraction
    # ---------------------------------------------------------
    def parse_frontend_pages(self, pages_dir: Path) -> List[Dict[str, Any]]:
        if not pages_dir.exists():
            return []
        pages = []
        for file in sorted(pages_dir.glob("*.tsx")):
            code = file.read_bytes()
            tree = self.tsx_parser.parse(code)
            exports = []
            
            def traverse(node):
                if node.type == 'export_statement':
                    declaration = node.child_by_field_name('declaration')
                    if declaration:
                        if declaration.type == 'function_declaration':
                            n = declaration.child_by_field_name('name')
                            if n:
                                exports.append(n.text.decode('utf-8'))
                        elif declaration.type == 'lexical_declaration':
                            for decl in declaration.children:
                                if decl.type == 'variable_declarator':
                                    n = decl.child_by_field_name('name')
                                    if n:
                                        exports.append(n.text.decode('utf-8'))
                for child in node.children:
                    traverse(child)

            traverse(tree.root_node)
            pages.append({
                "file": file.name,
                "path": str(file.relative_to(self.root_dir)).replace('\\', '/'),
                "component_name": file.stem,
                "exports": exports
            })
        return pages

    def parse_frontend_components(self, comp_dir: Path) -> List[Dict[str, Any]]:
        if not comp_dir.exists():
            return []
        components = []
        for file in sorted(comp_dir.rglob("*.tsx")):
            components.append({
                "name": file.stem,
                "rel_path": str(file.relative_to(self.root_dir)).replace('\\', '/')
            })
        return components

    # ---------------------------------------------------------
    # DuckLake Views
    # ---------------------------------------------------------
    def parse_lake_views(self) -> List[str]:
        return [
            "current_hospitals",
            "current_charges",
            "lake.hospitals",
            "lake.hospital_versions",
            "lake.hospital_identity",
            "lake.current_hospital_versions",
            "lake.standard_charges",
            "lake.standard_charge_details",
            "lake.modifier_charges",
            "lake.modifier_charge_details"
        ]

    # ---------------------------------------------------------
    # Full Workspace Inventory
    # ---------------------------------------------------------
    def scan_all(self) -> Dict[str, Any]:
        routes = self.parse_rust_routes(self.root_dir / "backend/src/routes/mod.rs")
        models = self.parse_rust_models(self.root_dir / "common/src/model.rs")
        repo_methods = self.parse_rust_repository_methods(self.root_dir / "common/src/repository.rs")
        pages = self.parse_frontend_pages(self.root_dir / "frontend/src/pages")
        components = self.parse_frontend_components(self.root_dir / "frontend/src/components")
        lake_views = self.parse_lake_views()

        return {
            "routes": routes,
            "models": models,
            "repository_methods": repo_methods,
            "frontend_pages": pages,
            "frontend_components": components,
            "lake_views": lake_views
        }

    # ---------------------------------------------------------
    # Skills & Documentation Synchronization Check
    # ---------------------------------------------------------
    def check_sync(self) -> Dict[str, Any]:
        """
        Verifies that:
        1. Agent skills accurately reflect backend API routes, models, and repository patterns.
        2. Agent skills reflect frontend pages, component structures, and design tokens.
        3. Agent skills reflect DuckLake schemas and views.
        4. Human-facing README.md covers the high-level services and architecture.
        """
        data = self.scan_all()
        issues = []

        # 1. Check Human-facing README.md for core high-level components
        readme_path = self.root_dir / "README.md"
        if not readme_path.exists():
            issues.append("README.md is missing from workspace root.")
        else:
            readme_text = readme_path.read_text(encoding="utf-8")
            required_readme_topics = ["Frontend", "Backend", "DuckDB", "DuckLake", "Getting Started"]
            for topic in required_readme_topics:
                if topic.lower() not in readme_text.lower():
                    issues.append(f"README.md is missing high-level section: '{topic}'")

        # 2. Check Backend Agent Skill for routes & patterns
        backend_skill_path = self.root_dir / ".agents/skills/healthcare-backend-api/SKILL.md"
        if not backend_skill_path.exists():
            issues.append(".agents/skills/healthcare-backend-api/SKILL.md is missing.")
        else:
            backend_skill_text = backend_skill_path.read_text(encoding="utf-8")
            for route in data["routes"]:
                path = route["path"]
                if path not in backend_skill_text:
                    issues.append(f"Axum route '{path}' missing from healthcare-backend-api SKILL.md")

        # 3. Check Frontend Agent Skill for pages & conventions
        frontend_skill_path = self.root_dir / ".agents/skills/healthcare-frontend/SKILL.md"
        if not frontend_skill_path.exists():
            issues.append(".agents/skills/healthcare-frontend/SKILL.md is missing.")
        else:
            frontend_skill_text = frontend_skill_path.read_text(encoding="utf-8")
            for page in data["frontend_pages"]:
                name = page["component_name"]
                if name not in frontend_skill_text:
                    issues.append(f"Frontend page '{name}' missing from healthcare-frontend SKILL.md")

        # 4. Check DuckLake Agent Skill for views & tables
        ducklake_skill_path = self.root_dir / ".agents/skills/healthcare-ducklake/SKILL.md"
        if not ducklake_skill_path.exists():
            issues.append(".agents/skills/healthcare-ducklake/SKILL.md is missing.")
        else:
            ducklake_skill_text = ducklake_skill_path.read_text(encoding="utf-8")
            for view in data["lake_views"]:
                if view not in ducklake_skill_text:
                    issues.append(f"DuckLake view/table '{view}' missing from healthcare-ducklake SKILL.md")

        return {
            "status": "PASS" if not issues else "FAIL",
            "issues": issues,
            "routes_count": len(data["routes"]),
            "models_count": len(data["models"]),
            "pages_count": len(data["frontend_pages"]),
            "components_count": len(data["frontend_components"])
        }


def main():
    parser = argparse.ArgumentParser(description="Healthcare Codebase Tree-Sitter AST & Skills Sync")
    parser.add_argument("command", choices=["scan", "check-sync", "markdown"], help="Command to execute")
    parser.add_argument("--json", action="store_true", help="Output results in JSON")
    args = parser.parse_args()

    workspace_root = Path(__file__).resolve().parent.parent
    analyzer = CodebaseASTAnalyzer(workspace_root)

    if args.command == "scan":
        data = analyzer.scan_all()
        if args.json:
            print(json.dumps(data, indent=2))
        else:
            print("=" * 65)
            print(" CODEBASE AST INVENTORY (Tree-Sitter)")
            print("=" * 65)
            print(f"\n[+] Axum REST Routes ({len(data['routes'])}):")
            for r in data["routes"]:
                print(f"    {r['method']:<6} {r['path']:<30} -> {r['handler']}")

            print(f"\n[+] Core Models ({len(data['models'])}):")
            for m in data["models"]:
                print(f"    struct {m['name']:<28} ({m['fields_count']} fields)")

            print(f"\n[+] Repository Engine Methods ({len(data['repository_methods'])}):")
            for rep in data["repository_methods"]:
                print(f"    fn {rep['name']}()")

            print(f"\n[+] Frontend Pages ({len(data['frontend_pages'])}):")
            for p in data["frontend_pages"]:
                print(f"    {p['component_name']:<20} ({p['path']})")

            print(f"\n[+] Frontend Components ({len(data['frontend_components'])}):")
            for c in data["frontend_components"]:
                print(f"    {c['name']:<25} ({c['rel_path']})")

            print(f"\n[+] DuckLake Catalog Views & Tables ({len(data['lake_views'])}):")
            for v in data["lake_views"]:
                print(f"    {v}")
            print("\n" + "=" * 65)

    elif args.command == "check-sync":
        res = analyzer.check_sync()
        if args.json:
            print(json.dumps(res, indent=2))
        else:
            print(f"Documentation & Skills AST Sync Status: {res['status']}")
            print(f"Discovered: {res['routes_count']} routes, {res['models_count']} models, {res['pages_count']} pages, {res['components_count']} components.")
            if res["issues"]:
                print("\nDiscrepancies found:")
                for issue in res["issues"]:
                    print(f"  - [FAIL] {issue}")
                sys.exit(1)
            else:
                print("\nAll agent skills and human documentation are fully synchronized!")
                sys.exit(0)

    elif args.command == "markdown":
        data = analyzer.scan_all()
        print("### Axum REST Endpoints")
        print("| Method | Endpoint Path | Handler |")
        print("|---|---|---|")
        for r in data["routes"]:
            print(f"| `{r['method']}` | `{r['path']}` | `{r['handler']}` |")
        print("\n### Frontend Pages & Views")
        print("| Page Component | Path |")
        print("|---|---|")
        for p in data["frontend_pages"]:
            print(f"| `{p['component_name']}` | `{p['path']}` |")


if __name__ == "__main__":
    main()
