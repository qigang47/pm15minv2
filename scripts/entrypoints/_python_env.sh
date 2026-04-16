#!/bin/bash

_pm15min_trim_ws() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

_pm15min_is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON|y|Y)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

_pm15min_detect_login_home() {
  local login_user="${PM15MIN_REAL_USER:-${SUDO_USER:-${LOGNAME:-${USER:-}}}}"
  local candidate=""

  if [[ -z "$login_user" ]]; then
    return 1
  fi
  if [[ ! "$login_user" =~ ^[A-Za-z0-9._-]+$ ]]; then
    return 1
  fi

  candidate="$(eval "printf '%s' ~$login_user" 2>/dev/null || true)"
  if [[ -z "$candidate" || "$candidate" == "~$login_user" ]]; then
    return 1
  fi
  printf '%s' "$candidate"
}

_pm15min_source_conda_from_home_candidates() {
  local login_home=""
  local candidate=""
  local conda_sh=""

  login_home="$(_pm15min_detect_login_home || true)"

  for candidate in "${PM15MIN_REAL_HOME:-}" "${REAL_HOME:-}" "$login_home" "${HOME:-}"; do
    [[ -z "$candidate" ]] && continue
    conda_sh="$candidate/miniconda3/etc/profile.d/conda.sh"
    if [[ -f "$conda_sh" ]]; then
      # shellcheck source=/dev/null
      source "$conda_sh"
      return 0
    fi
  done

  return 1
}

pm15min_load_project_env() {
  local project_dir="${PM15MIN_PROJECT_DIR:-$PWD}"
  local parent_dir
  parent_dir="$(cd "$project_dir/.." && pwd)"
  local env_path="${PM15MIN_ENV_FILE:-}"
  local candidate

  if [[ -n "$env_path" && ! -f "$env_path" ]]; then
    echo "WARN: 指定的环境文件不存在，忽略: ${env_path}"
    env_path=""
  fi
  if [[ -z "$env_path" ]]; then
    for candidate in "$project_dir/.env" "$parent_dir/.env"; do
      if [[ -f "$candidate" ]]; then
        env_path="$candidate"
        break
      fi
    done
  fi
  if [[ -z "$env_path" ]]; then
    echo "WARN: 未找到环境文件，跳过导入: ${project_dir}/.env"
    return 0
  fi

  while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
    local line key value
    line="${raw_line%$'\r'}"
    line="$(_pm15min_trim_ws "$line")"
    if [[ -z "$line" || "${line:0:1}" == "#" ]]; then
      continue
    fi
    if [[ "$line" != *=* ]]; then
      continue
    fi

    key="$(_pm15min_trim_ws "${line%%=*}")"
    value="${line#*=}"
    value="$(_pm15min_trim_ws "$value")"

    if [[ -z "$key" ]]; then
      continue
    fi
    if [[ "$key" == export\ * ]]; then
      key="$(_pm15min_trim_ws "${key#export }")"
    fi

    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    else
      value="${value%%[[:space:]]#*}"
      value="$(_pm15min_trim_ws "$value")"
    fi

    export "$key=$value"
  done < "$env_path"

  export PM15MIN_ENV_FILE_LOADED="$env_path"
  echo "✓ 已导入环境变量: ${env_path}"
}

pm15min_load_managed_proxy_env() {
  local enabled="${PM15MIN_MANAGED_PROXY_ENABLE:-0}"
  local env_path="${PM15MIN_MANAGED_PROXY_ENV_FILE:-${HOME:-}/.local/state/pm15min-managed-proxy/active_proxy.env}"
  local required="${PM15MIN_MANAGED_PROXY_REQUIRED:-0}"

  if ! _pm15min_is_truthy "$enabled"; then
    return 0
  fi

  if [[ -n "${HTTP_PROXY:-}" || -n "${HTTPS_PROXY:-}" || -n "${ALL_PROXY:-}" ]]; then
    echo "✓ 检测到显式代理环境，跳过受管代理加载"
    return 0
  fi

  if [[ ! -f "$env_path" ]]; then
    if _pm15min_is_truthy "$required"; then
      echo "WARN: 受管代理环境文件不存在，跳过: ${env_path}"
    fi
    return 0
  fi

  # shellcheck source=/dev/null
  source "$env_path"
  if [[ -n "${HTTP_PROXY:-}" ]]; then
    export http_proxy="${HTTP_PROXY}"
  fi
  if [[ -n "${HTTPS_PROXY:-}" ]]; then
    export https_proxy="${HTTPS_PROXY}"
  fi
  if [[ -n "${ALL_PROXY:-}" ]]; then
    export all_proxy="${ALL_PROXY}"
  fi
  if [[ -n "${NO_PROXY:-}" ]]; then
    export no_proxy="${NO_PROXY}"
  fi
  export PM15MIN_MANAGED_PROXY_ENV_LOADED="$env_path"
  echo "✓ 已加载受管代理: ${PM15MIN_MANAGED_PROXY_ACTIVE_URL:-${HTTP_PROXY:-}}"
}

pm15min_activate_python() {
  local requested_env="${CONDA_ENV:-pm15min}"
  local resolved_env=""

  if ! command -v conda >/dev/null 2>&1; then
    _pm15min_source_conda_from_home_candidates || true
  fi

  if ! command -v conda >/dev/null 2>&1; then
    echo "❌ 未找到 conda，拒绝回退到系统/Miniconda base Python。"
    return 1
  fi

  local conda_base
  conda_base="$(conda info --base 2>/dev/null || true)"
  if [[ -z "$conda_base" ]]; then
    echo "❌ 无法定位 conda base。"
    return 1
  fi

  # shellcheck source=/dev/null
  source "$conda_base/etc/profile.d/conda.sh"

  if conda env list | awk '{print $1}' | grep -qx "$requested_env"; then
    resolved_env="$requested_env"
  elif conda env list | awk '{print $1}' | grep -qx "pm15min_arm64"; then
    resolved_env="pm15min_arm64"
  elif conda env list | awk '{print $1}' | grep -qx "pm15min-exp-310"; then
    resolved_env="pm15min-exp-310"
  else
    echo "❌ 未找到 conda 环境: ${requested_env}"
    conda env list
    return 1
  fi

  conda activate "$resolved_env"

  export PM15MIN_CONDA_ENV="$resolved_env"
  export PM15MIN_CONDA_BASE="$conda_base"
  export PYTHON_BIN="${CONDA_PREFIX:-}/bin/python"

  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "❌ 激活 ${resolved_env} 后未找到环境 Python: ${PYTHON_BIN}"
    return 1
  fi

  if [[ "$PYTHON_BIN" == "$conda_base/bin/python" ]]; then
    echo "❌ 当前 Python 仍指向 Miniconda base: ${PYTHON_BIN}"
    echo "   为避免再次用错解释器，脚本中止。"
    return 1
  fi

  local actual_python
  actual_python="$("$PYTHON_BIN" -c 'import sys; print(sys.executable)')"
  if [[ "$actual_python" != "$PYTHON_BIN" ]]; then
    echo "❌ Python 路径校验失败: expected=${PYTHON_BIN} actual=${actual_python}"
    return 1
  fi

  echo "✓ Conda 环境已激活: ${PM15MIN_CONDA_ENV}"
  echo "✓ 使用 Python: ${PYTHON_BIN}"
}
