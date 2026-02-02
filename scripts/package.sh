#!/usr/bin/env bash

set -euo pipefail
shopt -s nullglob 2>/dev/null || true

readonly SCRIPT_VERSION="2.0.0"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly BUILD_DIR="${PROJECT_ROOT}/build"
readonly DIST_DIR="${PROJECT_ROOT}/dist"
readonly DEFAULT_VERSION="0.18.0-SNAPSHOT"
readonly DEFAULT_JAVA_RELEASE="21"
readonly MIN_JAVA_VERSION="21"
readonly BINDINGS="scylla dynamodb"
readonly MAVEN_OPTS_DEFAULT="-Xmx2g -XX:+UseG1GC"

# CI detection (not readonly as --ci flag can override)
IS_CI="${CI:-${GITHUB_ACTIONS:-${GITLAB_CI:-${JENKINS_URL:+true}}}}"
BUILD_NUMBER="${BUILD_NUMBER:-${GITHUB_RUN_NUMBER:-${CI_PIPELINE_ID:-local}}}"

# Colors for output (disabled if not a terminal or in CI)
setup_colors() {
    if [[ -t 1 ]] && [[ -z "${NO_COLOR:-}" ]] && [[ "${TERM:-}" != "dumb" ]]; then
        RED='\033[0;31m'
        GREEN='\033[0;32m'
        YELLOW='\033[1;33m'
        BLUE='\033[0;34m'
        BOLD='\033[1m'
        NC='\033[0m'
    else
        RED='' GREEN='' YELLOW='' BLUE='' BOLD='' NC=''
    fi
}
setup_colors

# Logging functions
log_info()  { printf "${GREEN}[INFO]${NC}  %s\n" "$*"; }
log_warn()  { printf "${YELLOW}[WARN]${NC}  %s\n" "$*" >&2; }
log_error() { printf "${RED}[ERROR]${NC} %s\n" "$*" >&2; }
log_debug() { [[ "${VERBOSE:-}" == "true" ]] && printf "${BLUE}[DEBUG]${NC} %s\n" "$*" || true; }
log_step()  { printf "${BOLD}[STEP]${NC}  %s\n" "$*"; }

die() {
    log_error "$@"
    exit 1
}

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Build, test, and package ScyllaDB YCSB bindings for release.

Options:
    -v, --version VERSION       Set build version (default: ${DEFAULT_VERSION})
    -j, --java RELEASE          Set Java release version (default: ${DEFAULT_JAVA_RELEASE})
    -c, --clean                 Clean build artifacts before building
    -t, --test                  Run test suite before packaging
    -T, --test-integration      Run integration tests (requires Docker)
    -s, --skip-version-set      Skip Maven version update
    -p, --parallel THREADS      Number of parallel build threads (default: auto)
    -f, --format FORMAT         Output format: tar, zip, both (default: tar)
    -o, --output-dir DIR        Output directory for artifacts (default: ${DIST_DIR})
    -q, --quiet                 Suppress non-essential output
    -V, --verbose               Enable verbose/debug output
    --dry-run                   Show what would be done without executing
    --no-checksum               Skip checksum generation
    --ci                        Enable CI mode (non-interactive, strict)
    -h, --help                  Show this help message
    --version                   Show script version

Examples:
    $(basename "$0") -v 1.0.0 -t                    # Build with tests
    $(basename "$0") -v 1.0.0 -T                    # Build with integration tests
    $(basename "$0") -v 1.0.0 -c -p 4               # Clean build with 4 threads
    $(basename "$0") -v 1.0.0 -f both               # Create tar.gz and zip
    $(basename "$0") --ci -v 1.0.0 -t               # CI build with tests

Environment Variables:
    JAVA_HOME           Path to Java installation
    MAVEN_OPTS          Additional Maven JVM options
    NO_COLOR            Disable colored output
    VERBOSE             Enable debug output (same as -V)
EOF
}

show_version() {
    echo "package.sh version ${SCRIPT_VERSION}"
    echo "YCSB default version: ${DEFAULT_VERSION}"
    echo "Default Java release: ${DEFAULT_JAVA_RELEASE}"
}

# Check if running in Docker/container
is_container() {
    [[ -f /.dockerenv ]] || grep -q 'docker\|lxc\|containerd' /proc/1/cgroup 2>/dev/null
}

# Detect available CPU cores for parallel builds
detect_parallelism() {
    local cores
    if [[ -f /proc/cpuinfo ]]; then
        cores=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 1)
    elif command -v sysctl >/dev/null 2>&1; then
        cores=$(sysctl -n hw.ncpu 2>/dev/null || echo 1)
    elif command -v nproc >/dev/null 2>&1; then
        cores=$(nproc 2>/dev/null || echo 1)
    else
        cores=1
    fi
    # Use cores-1 for builds, minimum 1
    echo $(( cores > 1 ? cores - 1 : 1 ))
}

check_prerequisites() {
    log_step "Checking prerequisites..."
    local missing=()

    for cmd in mvn java tar; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing+=("$cmd")
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        die "Missing required tools: ${missing[*]}"
    fi

    # Verify Java version
    local java_version
    java_version=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    java_version="${java_version:-0}"

    if [[ "$java_version" -lt "$MIN_JAVA_VERSION" ]]; then
        die "Java ${MIN_JAVA_VERSION}+ is required, found: ${java_version}"
    fi

    # Check Maven version
    local mvn_version
    mvn_version=$(mvn --version 2>&1 | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)

    log_info "Java version: ${java_version}"
    log_info "Maven version: ${mvn_version}"
    log_debug "JAVA_HOME: ${JAVA_HOME:-not set}"

    # Check for Docker if integration tests requested
    if [[ "${RUN_INTEGRATION_TESTS:-}" == "true" ]]; then
        if ! command -v docker >/dev/null 2>&1; then
            log_warn "Docker not found, integration tests will be skipped"
        elif ! docker info >/dev/null 2>&1; then
            log_warn "Docker daemon not running, integration tests will be skipped"
        else
            log_info "Docker available for integration tests"
        fi
    fi
}

clean_build() {
    local maven_clean="$1"

    log_step "Cleaning build directories..."
    rm -rf "${BUILD_DIR}" "${DIST_DIR}"

    if [[ "$maven_clean" == "true" ]]; then
        log_info "Running Maven clean..."
        run_maven clean -q
    fi
}

set_version() {
    local version="$1"
    log_step "Setting project version to ${version}..."
    run_maven versions:set -DnewVersion="${version}" -DgenerateBackupPoms=false -q
}

run_maven() {
    local maven_opts="${MAVEN_OPTS:-${MAVEN_OPTS_DEFAULT}}"

    if [[ "${DRY_RUN:-}" == "true" ]]; then
        log_info "[DRY-RUN] mvn $*"
        return 0
    fi

    MAVEN_OPTS="${maven_opts}" mvn -f "${PROJECT_ROOT}/pom.xml" "$@"
}

run_tests() {
    local test_type="$1"

    log_step "Running ${test_type} tests..."

    local mvn_args=("test")
    mvn_args+=("-Dmaven.test.failure.ignore=false")

    if [[ "$test_type" == "unit" ]]; then
        mvn_args+=("-DskipITs=true")
    elif [[ "$test_type" == "integration" ]]; then
        mvn_args+=("-DskipTests=false")
        mvn_args+=("-DskipITs=false")
    fi

    # Add encoding
    mvn_args+=("-Dproject.build.sourceEncoding=UTF-8")

    if ! run_maven "${mvn_args[@]}"; then
        die "Tests failed"
    fi

    log_info "${test_type^} tests passed"
}

build_project() {
    local java_release="$1"
    local parallel="${2:-}"

    log_step "Building project with Java ${java_release}..."

    local mvn_args=("clean" "package")
    mvn_args+=("-DskipTests")
    mvn_args+=("-Djava.release=${java_release}")
    mvn_args+=("-Dmaven.javadoc.skip=true")
    mvn_args+=("-Dmaven.source.skip=true")
    mvn_args+=("-Dproject.build.sourceEncoding=UTF-8")
    mvn_args+=("-ff")

    if [[ -n "$parallel" ]]; then
        mvn_args+=("-T" "${parallel}")
    fi

    if [[ "${QUIET:-}" == "true" ]]; then
        mvn_args+=("-q")
    fi

    run_maven "${mvn_args[@]}"

    log_info "Build completed successfully"
}

extract_bindings() {
    local version="$1"

    log_step "Extracting bindings..."
    mkdir -p "${BUILD_DIR}"/{conf,lib,bin,workloads}

    for binding in ${BINDINGS}; do
        local archive="${PROJECT_ROOT}/${binding}/target/ycsb-${binding}-binding-${version}.tar.gz"

        if [[ ! -f "$archive" ]]; then
            log_warn "Binding archive not found: ${archive}"
            continue
        fi

        log_info "Extracting ${binding} binding..."

        local tmpdir
        tmpdir=$(mktemp -d)
        trap "rm -rf '${tmpdir}'" RETURN

        tar -xf "${archive}" -C "${tmpdir}"

        local extracted_dir
        extracted_dir=$(find "${tmpdir}" -mindepth 1 -maxdepth 1 -type d | head -1)

        if [[ -d "${extracted_dir}" ]]; then
            # Merge contents, avoiding overwrites of existing files
            cp -rn "${extracted_dir}"/* "${BUILD_DIR}/" 2>/dev/null || \
            cp -r "${extracted_dir}"/* "${BUILD_DIR}/"
        fi

        rm -rf "${tmpdir}"
        trap - RETURN
    done

    # Copy additional configs
    for binding in ${BINDINGS}; do
        if [[ -d "${PROJECT_ROOT}/${binding}/conf" ]]; then
            cp -r "${PROJECT_ROOT}/${binding}/conf"/* "${BUILD_DIR}/conf/" 2>/dev/null || true
        fi
    done

    # Copy workloads
    if [[ -d "${PROJECT_ROOT}/workloads" ]]; then
        cp -r "${PROJECT_ROOT}/workloads"/* "${BUILD_DIR}/workloads/" 2>/dev/null || true
    fi
}

generate_build_info() {
    local version="$1"
    local java_release="$2"

    local git_commit="unknown"
    local git_branch="unknown"
    local git_dirty="false"

    if command -v git >/dev/null 2>&1 && [[ -d "${PROJECT_ROOT}/.git" ]]; then
        git_commit=$(git -C "${PROJECT_ROOT}" rev-parse --short HEAD 2>/dev/null || echo "unknown")
        git_branch=$(git -C "${PROJECT_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
        git_dirty=$(git -C "${PROJECT_ROOT}" diff --quiet 2>/dev/null && echo "false" || echo "true")
    fi

    cat > "${BUILD_DIR}/BUILD_INFO" <<EOF
# YCSB Build Information
# Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")

version=${version}
java.release=${java_release}
build.date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
build.timestamp=$(date +%s)
build.host=$(hostname -s 2>/dev/null || hostname)
build.user=${USER:-unknown}
build.number=${BUILD_NUMBER}
build.ci=${IS_CI:-false}

git.commit=${git_commit}
git.branch=${git_branch}
git.dirty=${git_dirty}

bindings=${BINDINGS}
script.version=${SCRIPT_VERSION}
EOF

    # Also create a VERSION file for easy access
    echo "${version}" > "${BUILD_DIR}/VERSION"
}

create_archive() {
    local format="$1"
    local name="$2"
    local source_dir="$3"
    local output_dir="$4"

    local output_path

    case "$format" in
        tar|tar.gz|tgz)
            output_path="${output_dir}/${name}.tar.gz"
            log_debug "Creating tar.gz: ${output_path}"
            tar -czf "${output_path}" -C "$(dirname "${source_dir}")" "$(basename "${source_dir}")"
            ;;
        zip)
            output_path="${output_dir}/${name}.zip"
            log_debug "Creating zip: ${output_path}"
            if command -v zip >/dev/null 2>&1; then
                (cd "$(dirname "${source_dir}")" && zip -qr "${output_path}" "$(basename "${source_dir}")")
            else
                log_warn "zip command not found, skipping zip format"
                return 1
            fi
            ;;
        *)
            die "Unknown archive format: $format"
            ;;
    esac

    echo "${output_path}"
}

generate_checksums() {
    local file="$1"

    if [[ "${SKIP_CHECKSUM:-}" == "true" ]]; then
        return 0
    fi

    log_debug "Generating checksums for ${file}"

    # SHA256
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "${file}" > "${file}.sha256"
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "${file}" > "${file}.sha256"
    fi

    # SHA512
    if command -v sha512sum >/dev/null 2>&1; then
        sha512sum "${file}" > "${file}.sha512"
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 512 "${file}" > "${file}.sha512"
    fi

    # MD5 (for legacy systems)
    if command -v md5sum >/dev/null 2>&1; then
        md5sum "${file}" > "${file}.md5"
    elif command -v md5 >/dev/null 2>&1; then
        md5 -r "${file}" > "${file}.md5"
    fi
}

create_distribution() {
    local version="$1"
    local java_release="$2"
    local format="$3"
    local output_dir="$4"

    log_step "Creating distribution archives..."

    mkdir -p "${output_dir}"

    generate_build_info "${version}" "${java_release}"

    local base_name="scylla-ycsb-${version}-java${java_release}"
    local created_files=()

    case "$format" in
        both)
            for fmt in tar zip; do
                local file
                file=$(create_archive "$fmt" "$base_name" "${BUILD_DIR}" "${output_dir}") && \
                    created_files+=("$file")
            done
            ;;
        *)
            local file
            file=$(create_archive "$format" "$base_name" "${BUILD_DIR}" "${output_dir}") && \
                created_files+=("$file")
            ;;
    esac

    # Generate checksums
    for file in "${created_files[@]}"; do
        generate_checksums "$file"
    done

    # Return the list of created files
    printf '%s\n' "${created_files[@]}"
}

print_summary() {
    local version="$1"
    local java_release="$2"
    local output_dir="$3"
    shift 3
    local files=("$@")

    echo ""
    log_step "============================================"
    log_step "        Build Summary"
    log_step "============================================"
    echo ""
    log_info "Version:        ${version}"
    log_info "Java Release:   ${java_release}"
    log_info "Bindings:       ${BINDINGS}"
    log_info "Build Number:   ${BUILD_NUMBER}"
    log_info "Output Dir:     ${output_dir}"
    echo ""

    if [[ ${#files[@]} -gt 0 ]]; then
        log_info "Generated artifacts:"
        for file in "${files[@]}"; do
            if [[ -f "$file" ]]; then
                local size
                size=$(du -h "$file" | cut -f1)
                echo "    ${file} (${size})"
            fi
        done
    fi

    echo ""
    log_info "To use the distribution:"
    echo "    tar -xzf ${output_dir}/scylla-ycsb-${version}-java${java_release}.tar.gz"
    echo "    cd build"
    echo "    ./bin/ycsb load scylla -P workloads/workloada -p hosts=<host>"
    echo ""
}

main() {
    local version="${DEFAULT_VERSION}"
    local java_release="${DEFAULT_JAVA_RELEASE}"
    local do_clean="false"
    local run_tests="false"
    local run_integration_tests="false"
    local skip_version_set="false"
    local parallel=""
    local format="tar"
    local output_dir="${DIST_DIR}"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -v|--version)
                version="$2"
                shift 2
                ;;
            -j|--java)
                java_release="$2"
                shift 2
                ;;
            -c|--clean)
                do_clean="true"
                shift
                ;;
            -t|--test)
                run_tests="true"
                shift
                ;;
            -T|--test-integration)
                run_tests="true"
                run_integration_tests="true"
                RUN_INTEGRATION_TESTS="true"
                shift
                ;;
            -s|--skip-version-set)
                skip_version_set="true"
                shift
                ;;
            -p|--parallel)
                parallel="$2"
                shift 2
                ;;
            -f|--format)
                format="$2"
                shift 2
                ;;
            -o|--output-dir)
                output_dir="$2"
                shift 2
                ;;
            -q|--quiet)
                QUIET="true"
                shift
                ;;
            -V|--verbose)
                VERBOSE="true"
                shift
                ;;
            --dry-run)
                DRY_RUN="true"
                shift
                ;;
            --no-checksum)
                SKIP_CHECKSUM="true"
                shift
                ;;
            --ci)
                IS_CI="true"
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            --version)
                show_version
                exit 0
                ;;
            -*)
                die "Unknown option: $1. Use -h for help."
                ;;
            *)
                die "Unexpected argument: $1. Use -h for help."
                ;;
        esac
    done

    # Set default parallelism if not specified
    if [[ -z "$parallel" ]]; then
        parallel=$(detect_parallelism)
        log_debug "Auto-detected parallelism: ${parallel}"
    fi

    log_info "Starting build: version=${version}, java=${java_release}, parallel=${parallel}"
    [[ "${DRY_RUN:-}" == "true" ]] && log_warn "DRY-RUN mode enabled"

    cd "${PROJECT_ROOT}"

    check_prerequisites

    if [[ "$do_clean" == "true" ]]; then
        clean_build "true"
    else
        clean_build "false"
    fi

    if [[ "$skip_version_set" != "true" ]]; then
        set_version "$version"
    fi

    # Run tests if requested
    if [[ "$run_tests" == "true" ]]; then
        run_tests "unit"
    fi

    if [[ "$run_integration_tests" == "true" ]]; then
        run_tests "integration"
    fi

    build_project "$java_release" "$parallel"
    extract_bindings "$version"

    local -a created_files
    while IFS= read -r file; do
        [[ -n "$file" ]] && created_files+=("$file")
    done < <(create_distribution "$version" "$java_release" "$format" "$output_dir")

    print_summary "$version" "$java_release" "$output_dir" "${created_files[@]}"

    log_info "Build completed successfully!"

    # Exit with success
    exit 0
}

# Trap for cleanup on error/interrupt
cleanup_on_exit() {
    local exit_code=$?
    if [[ $exit_code -ne 0 ]]; then
        log_error "Build failed with exit code: ${exit_code}"
    fi
}
trap cleanup_on_exit EXIT
trap 'log_error "Build interrupted"; exit 130' INT TERM

main "$@"
