#!/bin/bash

_pm15min_trim_ws() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
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

pm15min_activate_python() {
  local requested_env="${CONDA_ENV:-pm15min}"
  local resolved_env=""

  if ! command -v conda >/dev/null 2>&1; then
    if [[ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]]; then
      # shellcheck source=/dev/null
      source "$HOME/miniconda3/etc/profile.d/conda.sh"
    fi
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
  export PYTHON_BIN="${CONDA_PREFIX:-}/bin/python"

  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "❌ 激活 ${resolved_env} 后未找到环境 Python: ${PYTHON_BIN}"
    return 1
  fi

  if [[ "$PYTHON_BIN" == "$HOME/miniconda3/bin/python" ]]; then
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
