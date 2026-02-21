"""
Notebook de exploracion: Marcas y modelos de equipos mineros.
Ejecutar como script o en Jupyter con # %% para celdas.

Objetivo: Visualizar el inventario de marcas/modelos configurados
y validar la cobertura antes de ejecutar el pipeline completo.
"""

# %% Imports
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.utils.config_loader import load_brands_config, get_all_brands_flat, get_all_models_for_brand

# %% Cargar configuracion
brands_config = load_brands_config()
brands = get_all_brands_flat(brands_config)

print(f"Total marcas configuradas: {len(brands)}")
print()

# %% Resumen por tier
for tier in ["tier_1", "tier_2", "chinese_brands"]:
    tier_brands = [b for b in brands if b["tier"] == tier]
    print(f"\n{'='*50}")
    print(f"  {tier.upper()} ({len(tier_brands)} marcas)")
    print(f"{'='*50}")
    for b in tier_brands:
        models = get_all_models_for_brand(b)
        n_carguio = len([m for m in models if m["category"] == "carguio"])
        n_transporte = len([m for m in models if m["category"] == "transporte"])
        print(f"  {b['nombre']:<45} | {b['pais']:<15} | Carguio: {n_carguio:>2} | Transporte: {n_transporte:>2}")

# %% Tabla detallada de todos los modelos
rows = []
for brand in brands:
    models = get_all_models_for_brand(brand)
    for m in models:
        rows.append({
            "Marca": brand["nombre"],
            "Pais": brand["pais"],
            "Tier": brand["tier"],
            "Categoria": m["category"],
            "Tipo Equipo": m["equipment_type"],
            "Modelo": m["model"],
        })

df = pd.DataFrame(rows)
print(f"\n\nTotal modelos a buscar: {len(df)}")
print(f"\nPor categoria:")
print(df["Categoria"].value_counts().to_string())
print(f"\nPor tipo de equipo:")
print(df["Tipo Equipo"].value_counts().to_string())
print(f"\nPor marca:")
print(df.groupby("Marca")["Modelo"].count().sort_values(ascending=False).to_string())

# %% Estimacion de queries
TEMPLATES = 7  # Numero de templates de busqueda
total_queries = len(df) * TEMPLATES
print(f"\n\nEstimacion de esfuerzo de busqueda:")
print(f"  Modelos unicos:     {len(df)}")
print(f"  Templates x modelo: {TEMPLATES}")
print(f"  Total queries:      {total_queries}")
print(f"  Tiempo estimado:    ~{total_queries * 2 / 60:.0f} minutos (2s delay entre queries)")

# %% Guardar inventario
output_path = Path(__file__).parent.parent / "data" / "processed" / "inventario_modelos.csv"
output_path.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"\nInventario guardado en: {output_path}")
