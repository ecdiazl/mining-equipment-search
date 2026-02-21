#!/usr/bin/env bash
# =============================================================================
# run.sh — Control de ejecucion del proyecto Mining Equipment Search
#
# Uso:
#   ./run.sh pipeline [--brand BRAND] [--fresh]   Pipeline completo
#   ./run.sh search   [--brand BRAND] [--fresh]   Solo fase de busqueda
#   ./run.sh report                                Regenerar reportes
#   ./run.sh status   [--brand BRAND]              Ver estado de datos
#   ./run.sh query    "texto de busqueda"          Busqueda semantica
#   ./run.sh test                                  Ejecutar tests
#   ./run.sh test-cov                              Tests con cobertura
#   ./run.sh validate                              Validar entorno
#   ./run.sh db-fix                                Reparar DB bloqueada (NFS)
#   ./run.sh backup                                Backup manual de la DB
#   ./run.sh clean    [--brand BRAND]              Limpiar datos de marca
#   ./run.sh help                                  Mostrar esta ayuda
#
# Marcas disponibles:
#   komatsu, hitachi, liebherr, volvo_ce, belaz, doosan_infracore,
#   xcmg, sany, zoomlion, liugong, shantui
#
# Ejemplos:
#   ./run.sh pipeline --brand xcmg --fresh
#   ./run.sh status --brand komatsu
#   ./run.sh query "Komatsu 930E payload capacity"
#   ./run.sh test
# =============================================================================

set -euo pipefail

# --- Rutas ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
DATA_DIR="$PROJECT_DIR/data"
DB_PATH="$DATA_DIR/mining_equipment.db"
DB_LOCAL="/tmp/mining-equipment-data/mining_equipment.db"
REPORTS_DIR="$DATA_DIR/reports"
LOGS_DIR="$PROJECT_DIR/logs"
BACKUP_DIR="$DATA_DIR/processed"

# --- Colores ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# --- Funciones auxiliares ---

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }
log_step()  { echo -e "\n${CYAN}${BOLD}=== $* ===${NC}"; }

VALID_BRANDS="komatsu hitachi liebherr volvo_ce belaz doosan_infracore xcmg sany zoomlion liugong shantui"

validate_brand() {
    local brand="$1"
    for b in $VALID_BRANDS; do
        [[ "$b" == "$brand" ]] && return 0
    done
    log_error "Marca '$brand' no valida."
    echo "  Marcas disponibles: $VALID_BRANDS"
    exit 1
}

ensure_dirs() {
    mkdir -p "$DATA_DIR" "$DATA_DIR/raw" "$DATA_DIR/processed" "$DATA_DIR/embeddings" \
             "$REPORTS_DIR" "$LOGS_DIR" "/tmp/mining-equipment-data"
}

timestamp() {
    date "+%Y-%m-%d %H:%M:%S"
}

# Detecta NFS y prepara DB local si es necesario.
# Exporta MINING_DB_PATH para que el pipeline use la ruta correcta.
# Retorna 0 si NFS (y DB fue copiada a /tmp), 1 si local.
setup_nfs_db() {
    local fs_type
    fs_type=$(df --output=fstype "$DATA_DIR" 2>/dev/null | tail -1 || echo "unknown")

    if [[ "$fs_type" == *"cifs"* ]] || [[ "$fs_type" == *"nfs"* ]] || [[ "$fs_type" == *"fuse"* ]]; then
        mkdir -p "$(dirname "$DB_LOCAL")"
        # Si existe DB en NFS con datos, copiar a /tmp
        if [[ -f "$DB_PATH" ]]; then
            local size
            size=$(stat -c%s "$DB_PATH" 2>/dev/null || echo "0")
            if [[ "$size" -gt 0 ]]; then
                cp "$DB_PATH" "$DB_LOCAL" 2>/dev/null || true
            fi
        fi
        export MINING_DB_PATH="$DB_LOCAL"
        return 0
    fi
    export MINING_DB_PATH="$DB_PATH"
    return 1
}

# Copia DB de vuelta de /tmp a NFS despues de una operacion de escritura
sync_db_back() {
    if [[ "$MINING_DB_PATH" == "$DB_LOCAL" ]] && [[ -f "$DB_LOCAL" ]]; then
        local size
        size=$(stat -c%s "$DB_LOCAL" 2>/dev/null || echo "0")
        if [[ "$size" -gt 0 ]]; then
            cp "$DB_LOCAL" "$DB_PATH" 2>/dev/null \
                && log_info "DB sincronizada a NFS: $DB_PATH" \
                || log_warn "No se pudo sincronizar DB a NFS. Funcional en: $DB_LOCAL"
        fi
    fi
}

# Ejecuta python con el proyecto en el path
run_python() {
    cd "$PROJECT_DIR"
    python "$@"
}

# --- Comandos ---

cmd_validate() {
    log_step "Validando entorno"

    # Python
    if command -v python &>/dev/null; then
        local pyver
        pyver=$(python --version 2>&1)
        log_ok "Python: $pyver"
    else
        log_error "Python no encontrado"
        exit 1
    fi

    # Dependencias criticas
    local deps=("sqlalchemy" "pandas" "tqdm" "yaml" "bs4" "requests")
    local dep_names=("SQLAlchemy" "pandas" "tqdm" "PyYAML" "BeautifulSoup4" "requests")
    local all_ok=true

    for i in "${!deps[@]}"; do
        if python -c "import ${deps[$i]}" 2>/dev/null; then
            log_ok "${dep_names[$i]}"
        else
            log_error "${dep_names[$i]} no instalado"
            all_ok=false
        fi
    done

    # Dependencias opcionales
    local opt_deps=("plotly" "chromadb" "sentence_transformers")
    local opt_names=("Plotly" "ChromaDB" "sentence-transformers")
    for i in "${!opt_deps[@]}"; do
        if python -c "import ${opt_deps[$i]}" 2>/dev/null; then
            log_ok "${opt_names[$i]} (opcional)"
        else
            log_warn "${opt_names[$i]} no instalado (opcional, algunas funciones limitadas)"
        fi
    done

    # pytest
    if python -m pytest --version &>/dev/null; then
        log_ok "pytest: $(python -m pytest --version 2>&1 | head -1)"
    else
        log_warn "pytest no instalado (no se podran ejecutar tests)"
    fi

    # Archivos de configuracion
    if [[ -f "$PROJECT_DIR/config/settings.yaml" ]]; then
        log_ok "config/settings.yaml"
    else
        log_error "config/settings.yaml no encontrado"
        all_ok=false
    fi

    if [[ -f "$PROJECT_DIR/config/brands.yaml" ]]; then
        log_ok "config/brands.yaml"
    else
        log_error "config/brands.yaml no encontrado"
        all_ok=false
    fi

    # .env
    if [[ -f "$PROJECT_DIR/.env" ]]; then
        local has_keys=false
        if grep -qE "^SERPER_API_KEY=.+" "$PROJECT_DIR/.env" 2>/dev/null; then
            log_ok "SERPER_API_KEY configurada"
            has_keys=true
        else
            log_warn "SERPER_API_KEY no configurada en .env"
        fi
        if grep -qE "^GOOGLE_API_KEY=.+" "$PROJECT_DIR/.env" 2>/dev/null; then
            log_ok "GOOGLE_API_KEY configurada"
            has_keys=true
        else
            log_warn "GOOGLE_API_KEY no configurada en .env"
        fi
        if [[ "$has_keys" == false ]]; then
            log_warn "Sin API keys: la fase de busqueda web no funcionara"
        fi
    else
        log_warn ".env no encontrado (copiar de .env.example)"
    fi

    # Filesystem / NFS check
    setup_nfs_db || true
    log_info "DB path: $MINING_DB_PATH"

    # Directorios
    ensure_dirs
    log_ok "Directorios de datos creados"

    if [[ "$all_ok" == true ]]; then
        echo ""
        log_ok "Entorno listo para ejecucion"
    else
        echo ""
        log_error "Hay problemas pendientes. Revisar errores arriba."
        exit 1
    fi
}

cmd_test() {
    log_step "Ejecutando tests"
    cd "$PROJECT_DIR"
    python -m pytest tests/ -v "$@"
}

cmd_test_cov() {
    log_step "Ejecutando tests con cobertura"
    cd "$PROJECT_DIR"
    python -m pytest tests/ -v --tb=short \
        --cov=src --cov-report=term-missing --cov-report=html:data/reports/coverage "$@"
    log_ok "Reporte de cobertura en: $REPORTS_DIR/coverage/index.html"
}

cmd_pipeline() {
    local brand=""
    local fresh=""
    local extra_args=()

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --brand)  brand="$2"; shift 2 ;;
            --fresh)  fresh="--fresh"; shift ;;
            *)        extra_args+=("$1"); shift ;;
        esac
    done

    [[ -n "$brand" ]] && validate_brand "$brand"
    ensure_dirs
    setup_nfs_db || true

    log_step "Pipeline completo"
    [[ -n "$brand" ]] && log_info "Marca: $brand"
    [[ -n "$fresh" ]] && log_warn "Modo FRESH: se borraran datos previos"
    log_info "Inicio: $(timestamp)"
    log_info "DB: $MINING_DB_PATH"

    local args=()
    [[ -n "$brand" ]] && args+=(--brand "$brand")
    [[ -n "$fresh" ]] && args+=($fresh)

    run_python main.py "${args[@]}" "${extra_args[@]}" 2>&1 | tee -a "$LOGS_DIR/pipeline_$(date +%Y%m%d_%H%M%S).log"
    local rc=${PIPESTATUS[0]}

    sync_db_back

    if [[ $rc -eq 0 ]]; then
        log_ok "Pipeline completado exitosamente"
        log_info "Reportes en: $REPORTS_DIR/"
        log_info "DB en: $MINING_DB_PATH"
    else
        log_error "Pipeline fallo (exit code: $rc)"
        log_info "Revisar logs en: $LOGS_DIR/"
        log_info "Si el error es 'database is locked', ejecutar: ./run.sh db-fix"
        exit $rc
    fi
}

cmd_search() {
    local brand=""
    local fresh=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --brand)  brand="$2"; shift 2 ;;
            --fresh)  fresh="--fresh"; shift ;;
            *)        shift ;;
        esac
    done

    [[ -n "$brand" ]] && validate_brand "$brand"
    ensure_dirs
    setup_nfs_db || true

    log_step "Fase de busqueda web"
    [[ -n "$brand" ]] && log_info "Marca: $brand"

    local args=(--search-only)
    [[ -n "$brand" ]] && args+=(--brand "$brand")
    [[ -n "$fresh" ]] && args+=($fresh)

    run_python main.py "${args[@]}" 2>&1 | tee -a "$LOGS_DIR/search_$(date +%Y%m%d_%H%M%S).log"
    sync_db_back
}

cmd_report() {
    ensure_dirs
    setup_nfs_db || true
    log_step "Regenerando reportes"
    run_python main.py --report-only 2>&1
    sync_db_back
    log_ok "Reportes generados en: $REPORTS_DIR/"

    if [[ -f "$REPORTS_DIR/equipment_report.html" ]]; then
        log_info "Reporte HTML: $REPORTS_DIR/equipment_report.html"
    fi
}

cmd_status() {
    local brand=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --brand)  brand="$2"; shift 2 ;;
            *)        shift ;;
        esac
    done

    [[ -n "$brand" ]] && validate_brand "$brand"

    log_step "Estado de datos"
    setup_nfs_db || true

    # Intento via main.py --status (usa MINING_DB_PATH para NFS)
    local db_to_check="$MINING_DB_PATH"
    if [[ -f "$db_to_check" ]]; then
        local size
        size=$(stat -c%s "$db_to_check" 2>/dev/null || echo "0")
        if [[ "$size" -gt 0 ]]; then
            if [[ -n "$brand" ]]; then
                run_python main.py --status --brand "$brand"
            else
                run_python main.py --status
            fi
            return
        fi
    fi

    # Fallback: revisar archivos CSV
    log_warn "DB vacia o no accesible. Mostrando info desde archivos:"
    echo ""
    if [[ -f "$DATA_DIR/raw/search_results.csv" ]]; then
        local lines
        lines=$(wc -l < "$DATA_DIR/raw/search_results.csv")
        log_info "search_results.csv: $((lines - 1)) resultados"
    fi
    if [[ -f "$REPORTS_DIR/summary.json" ]]; then
        log_info "summary.json:"
        python -c "
import json
with open('$REPORTS_DIR/summary.json') as f:
    s = json.load(f)
print(f'  Marcas:   {s.get(\"total_brands\", 0)}')
print(f'  Modelos:  {s.get(\"total_models\", 0)}')
print(f'  Specs:    {s.get(\"total_specs\", 0)}')
if 'specs_per_brand' in s:
    for b, c in s['specs_per_brand'].items():
        print(f'    {b}: {c} specs')
"
    fi
    if [[ -f "$REPORTS_DIR/all_specs.csv" ]]; then
        local spec_lines
        spec_lines=$(wc -l < "$REPORTS_DIR/all_specs.csv")
        log_info "all_specs.csv: $((spec_lines - 1)) registros"
    fi
}

cmd_query() {
    if [[ $# -lt 1 ]]; then
        log_error "Falta texto de busqueda."
        echo "  Uso: ./run.sh query \"Komatsu 930E payload\""
        exit 1
    fi
    setup_nfs_db || true
    log_step "Busqueda semantica"
    run_python main.py --query "$1"
}

cmd_db_fix() {
    log_step "Reparacion de DB (NFS locking)"

    local is_nfs=false
    local fs_type
    fs_type=$(df --output=fstype "$DATA_DIR" 2>/dev/null | tail -1 || echo "unknown")
    if [[ "$fs_type" == *"cifs"* ]] || [[ "$fs_type" == *"nfs"* ]] || [[ "$fs_type" == *"fuse"* ]]; then
        is_nfs=true
    fi

    local size=0
    if [[ -f "$DB_PATH" ]]; then
        size=$(stat -c%s "$DB_PATH" 2>/dev/null || echo "0")
    fi

    # Caso 1: No existe o tiene 0 bytes
    if [[ ! -f "$DB_PATH" ]] || [[ "$size" -eq 0 ]]; then
        [[ "$size" -eq 0 ]] && log_warn "DB existe pero tiene 0 bytes (corrupta por NFS lock)."
        [[ ! -f "$DB_PATH" ]] && log_info "No existe DB en $DB_PATH."

        rm -f "$DB_PATH"

        if [[ "$is_nfs" == true ]]; then
            log_info "Filesystem NFS/SMB detectado. Creando DB en /tmp y copiando..."
            run_python -c "
from src.models.database import DatabaseManager
db = DatabaseManager(db_path='$DB_LOCAL')
db.create_tables()
print('DB creada en $DB_LOCAL')
"
            cp "$DB_LOCAL" "$DB_PATH" 2>/dev/null && log_ok "DB copiada a $DB_PATH" \
                || log_warn "No se pudo copiar a NFS. DB funcional en: $DB_LOCAL"
        else
            log_info "Creando nueva DB..."
            run_python -c "
from src.models.database import DatabaseManager
db = DatabaseManager(db_path='$DB_PATH')
db.create_tables()
print('DB creada exitosamente')
"
            log_ok "DB creada en $DB_PATH"
        fi

        # Verificar tablas
        local target="$DB_PATH"
        [[ "$is_nfs" == true ]] && target="$DB_LOCAL"
        if command -v sqlite3 &>/dev/null; then
            log_info "Tablas creadas:"
            sqlite3 "$target" ".tables" 2>/dev/null || true
        fi
        return
    fi

    # Caso 2: DB existe con datos — verificar integridad
    if command -v sqlite3 &>/dev/null; then
        log_info "Verificando integridad de DB ($(du -h "$DB_PATH" | cut -f1))..."
        local result
        result=$(sqlite3 "$DB_PATH" "PRAGMA integrity_check;" 2>&1) || true
        if [[ "$result" == "ok" ]]; then
            log_ok "Integridad OK"
        else
            log_warn "Problemas de integridad: $result"
            log_info "Intentando recuperar..."

            # Backup antes de reparar
            local bak="${DB_PATH}.bak.$(date +%Y%m%d_%H%M%S)"
            cp "$DB_PATH" "$bak"
            log_info "Backup creado: $bak"

            # Dump y reload via /tmp
            local dump_file="/tmp/mining_db_dump_$$.sql"
            local tmp_db="/tmp/mining_db_repair_$$.db"
            sqlite3 "$DB_PATH" ".dump" > "$dump_file" 2>/dev/null || true
            sqlite3 "$tmp_db" < "$dump_file" 2>/dev/null || true
            rm -f "$dump_file"

            # Verificar reparada
            result=$(sqlite3 "$tmp_db" "PRAGMA integrity_check;" 2>&1) || true
            if [[ "$result" == "ok" ]]; then
                cp "$tmp_db" "$DB_PATH" 2>/dev/null || true
                rm -f "$tmp_db"
                log_ok "DB reparada exitosamente"
            else
                rm -f "$tmp_db"
                log_error "No se pudo reparar. Restaurando backup..."
                cp "$bak" "$DB_PATH"
                log_info "Considere usar: ./run.sh pipeline --brand BRAND --fresh"
            fi
        fi

        # Mostrar tablas
        log_info "Tablas en DB:"
        sqlite3 "$DB_PATH" ".tables" 2>/dev/null || log_warn "No se pudo listar tablas"
    else
        log_warn "sqlite3 no disponible. Intentando via Python..."
        local target="$DB_PATH"
        [[ "$is_nfs" == true ]] && target="$DB_LOCAL"
        run_python -c "
from src.models.database import DatabaseManager
db = DatabaseManager(db_path='$target')
db.create_tables()
print('Tablas verificadas/creadas')
"
    fi
}

cmd_backup() {
    log_step "Backup de base de datos"
    ensure_dirs

    if [[ ! -f "$DB_PATH" ]]; then
        log_warn "No hay DB para respaldar en $DB_PATH"
        return
    fi

    local size
    size=$(stat -c%s "$DB_PATH" 2>/dev/null || echo "0")
    if [[ "$size" -eq 0 ]]; then
        log_warn "DB tiene 0 bytes, no se respalda"
        return
    fi

    local backup_file="$BACKUP_DIR/mining_equipment_$(date +%Y%m%d_%H%M%S).db"
    cp "$DB_PATH" "$backup_file"
    log_ok "Backup creado: $backup_file ($(du -h "$backup_file" | cut -f1))"

    # Listar backups existentes
    log_info "Backups existentes:"
    ls -lh "$BACKUP_DIR"/mining_equipment_*.db 2>/dev/null | while read -r line; do
        echo "  $line"
    done || log_info "  (ninguno)"
}

cmd_clean() {
    local brand=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --brand)  brand="$2"; shift 2 ;;
            *)        shift ;;
        esac
    done

    if [[ -z "$brand" ]]; then
        log_error "Debe especificar --brand para limpiar datos."
        echo "  Uso: ./run.sh clean --brand xcmg"
        echo "  Marcas: $VALID_BRANDS"
        exit 1
    fi

    validate_brand "$brand"
    setup_nfs_db || true
    log_step "Limpiando datos de '$brand'"

    run_python -c "
from src.models.database import DatabaseManager
db = DatabaseManager(db_path='$MINING_DB_PATH')
db.create_tables()
counts = db.clear_brand_data('$brand')
print(f'Datos eliminados para $brand:')
for k, v in counts.items():
    print(f'  {k}: {v}')
"
    sync_db_back
    log_ok "Datos de '$brand' eliminados"
}

cmd_help() {
    echo -e "${BOLD}Mining Equipment Search — Control de Ejecucion${NC}"
    echo ""
    echo -e "${BOLD}Uso:${NC} ./run.sh <comando> [opciones]"
    echo ""
    echo -e "${BOLD}Comandos principales:${NC}"
    echo "  pipeline [--brand B] [--fresh]   Pipeline completo (search + scrape + extract + report)"
    echo "  search   [--brand B] [--fresh]   Solo fase de busqueda web"
    echo "  report                            Regenerar reportes desde DB existente"
    echo "  status   [--brand B]              Estado de recopilacion de datos"
    echo "  query    \"texto\"                  Busqueda semantica sobre documentos"
    echo ""
    echo -e "${BOLD}Desarrollo y mantenimiento:${NC}"
    echo "  test                              Ejecutar suite de tests (pytest)"
    echo "  test-cov                          Tests con reporte de cobertura"
    echo "  validate                          Validar entorno (Python, deps, configs, API keys)"
    echo "  db-fix                            Reparar DB bloqueada o corrupta (problema NFS)"
    echo "  backup                            Crear backup manual de la DB"
    echo "  clean    --brand B                Borrar datos de una marca (preserva la marca)"
    echo "  help                              Mostrar esta ayuda"
    echo ""
    echo -e "${BOLD}Marcas disponibles:${NC}"
    echo "  Tier 1:    komatsu, hitachi, liebherr, volvo_ce"
    echo "  Tier 2:    belaz, doosan_infracore"
    echo "  Chinese:   xcmg, sany, zoomlion, liugong, shantui"
    echo ""
    echo -e "${BOLD}Ejemplos:${NC}"
    echo "  ./run.sh validate                          # Verificar que todo esta listo"
    echo "  ./run.sh pipeline --brand xcmg --fresh     # Pipeline XCMG desde cero"
    echo "  ./run.sh pipeline --brand komatsu          # Pipeline Komatsu (resume)"
    echo "  ./run.sh status                            # Ver estado de todas las marcas"
    echo "  ./run.sh status --brand xcmg               # Detalle de XCMG"
    echo "  ./run.sh report                            # Regenerar reportes"
    echo "  ./run.sh query \"CAT 797F rimpull curve\"    # Busqueda semantica"
    echo "  ./run.sh test                              # Ejecutar tests"
    echo "  ./run.sh clean --brand sany                # Limpiar datos de SANY"
    echo "  ./run.sh db-fix                            # Reparar DB si hay problemas"
    echo ""
    echo -e "${BOLD}Notas:${NC}"
    echo "  - Requiere API keys en .env (SERPER_API_KEY y/o GOOGLE_API_KEY)"
    echo "  - La DB SQLite puede tener problemas de locking en NFS/Azure Files"
    echo "    Usar './run.sh db-fix' si se produce 'database is locked'"
    echo "  - Los logs se guardan en logs/"
    echo "  - Los reportes (CSV, Excel, HTML) se generan en data/reports/"
}

# --- Main dispatcher ---

if [[ $# -lt 1 ]]; then
    cmd_help
    exit 0
fi

COMMAND="$1"
shift

case "$COMMAND" in
    pipeline)   cmd_pipeline "$@" ;;
    search)     cmd_search "$@" ;;
    report)     cmd_report "$@" ;;
    status)     cmd_status "$@" ;;
    query)      cmd_query "$@" ;;
    test)       cmd_test "$@" ;;
    test-cov)   cmd_test_cov "$@" ;;
    validate)   cmd_validate ;;
    db-fix)     cmd_db_fix ;;
    backup)     cmd_backup ;;
    clean)      cmd_clean "$@" ;;
    help|--help|-h)  cmd_help ;;
    *)
        log_error "Comando desconocido: '$COMMAND'"
        echo ""
        cmd_help
        exit 1
        ;;
esac
